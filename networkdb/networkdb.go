package networkdb

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/armon/go-radix"
	"github.com/docker/go-events"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
)

const (
	byTable int = 1 + iota
	byNetwork
)

type NetworkDB struct {
	sync.RWMutex
	config            *Config
	mConfig           *memberlist.Config
	indexes           map[int]*radix.Tree
	memberlist        *memberlist.Memberlist
	nodes             map[string]*memberlist.Node
	networks          map[string]map[string]*network
	networkNodes      map[string][]string
	bulkSyncAckTbl    map[string]chan struct{}
	networkClock      serf.LamportClock
	tableClock        serf.LamportClock
	networkBroadcasts *memberlist.TransmitLimitedQueue
	stopCh            chan struct{}
	broadcaster       *events.Broadcaster
	tickers           []*time.Ticker
}

type network struct {
	id              string
	ltime           serf.LamportTime
	leaving         bool
	leaveTime       time.Time
	tableBroadcasts *memberlist.TransmitLimitedQueue
}

type Config struct {
	NodeName string
	BindPort int
}

type entry struct {
	node       string
	ltime      serf.LamportTime
	value      []byte
	deleting   bool
	deleteTime time.Time
}

func New(c *Config) (*NetworkDB, error) {
	nDB := &NetworkDB{
		config:         c,
		indexes:        make(map[int]*radix.Tree),
		networks:       make(map[string]map[string]*network),
		nodes:          make(map[string]*memberlist.Node),
		networkNodes:   make(map[string][]string),
		bulkSyncAckTbl: make(map[string]chan struct{}),
		broadcaster:    events.NewBroadcaster(),
	}

	nDB.indexes[byTable] = radix.New()
	nDB.indexes[byNetwork] = radix.New()

	if err := nDB.clusterInit(); err != nil {
		return nil, err
	}

	return nDB, nil
}

func (nDB *NetworkDB) Join(members []string) error {
	return nDB.clusterJoin(members)
}

func (nDB *NetworkDB) Close() {
	if err := nDB.clusterLeave(); err != nil {
		logrus.Errorf("Could not close DB %s: %v", nDB.config.NodeName)
	}
}

func (nDB *NetworkDB) GetEntry(tname, nid, key string) ([]byte, error) {
	entry, err := nDB.getEntry(tname, nid, key)
	if err != nil {
		return nil, err
	}

	return entry.value, nil
}

func (nDB *NetworkDB) getEntry(tname, nid, key string) (*entry, error) {
	nDB.RLock()
	defer nDB.RUnlock()

	e, ok := nDB.indexes[byTable].Get(fmt.Sprintf("/%s/%s/%s", tname, nid, key))
	if !ok {
		return nil, fmt.Errorf("could not get entry in table %s with network id %s and key %s", tname, nid, key)
	}

	return e.(*entry), nil
}

func (nDB *NetworkDB) CreateEntry(tname, nid, key string, value []byte) error {
	if _, err := nDB.GetEntry(tname, nid, key); err == nil {
		return fmt.Errorf("cannot create entry as the entry in table %s with network id %s and key %s already exists", tname, nid, key)
	}

	entry := &entry{
		ltime: nDB.tableClock.Increment(),
		node:  nDB.config.NodeName,
		value: value,
	}

	if err := nDB.sendTableEvent(tableEntryCreate, nid, tname, key, entry); err != nil {
		return fmt.Errorf("cannot send table create event: %v", err)
	}

	nDB.Lock()
	nDB.indexes[byTable].Insert(fmt.Sprintf("/%s/%s/%s", tname, nid, key), entry)
	nDB.indexes[byNetwork].Insert(fmt.Sprintf("/%s/%s/%s", nid, tname, key), entry)
	nDB.Unlock()

	nDB.broadcaster.Write(makeEvent(opCreate, tname, nid, key, value))
	return nil
}

func (nDB *NetworkDB) UpdateEntry(tname, nid, key string, value []byte) error {
	if _, err := nDB.GetEntry(tname, nid, key); err != nil {
		return fmt.Errorf("cannot update entry as the entry in table %s with network id %s and key %s does not exist", tname, nid, key)
	}

	entry := &entry{
		ltime: nDB.tableClock.Increment(),
		node:  nDB.config.NodeName,
		value: value,
	}

	if err := nDB.sendTableEvent(tableEntryUpdate, nid, tname, key, entry); err != nil {
		return fmt.Errorf("cannot send table update event: %v", err)
	}

	nDB.Lock()
	nDB.indexes[byTable].Insert(fmt.Sprintf("/%s/%s/%s", tname, nid, key), entry)
	nDB.indexes[byNetwork].Insert(fmt.Sprintf("/%s/%s/%s", nid, tname, key), entry)
	nDB.Unlock()

	nDB.broadcaster.Write(makeEvent(opUpdate, tname, nid, key, value))
	return nil
}

func (nDB *NetworkDB) DeleteEntry(tname, nid, key string) error {
	value, err := nDB.GetEntry(tname, nid, key)
	if err != nil {
		return fmt.Errorf("cannot delete entry as the entry in table %s with network id %s and key %s does not exist", tname, nid, key)
	}

	entry := &entry{
		ltime:      nDB.tableClock.Increment(),
		node:       nDB.config.NodeName,
		value:      value,
		deleting:   true,
		deleteTime: time.Now(),
	}

	if err := nDB.sendTableEvent(tableEntryDelete, nid, tname, key, entry); err != nil {
		return fmt.Errorf("cannot send table delete event: %v", err)
	}

	nDB.Lock()
	nDB.indexes[byTable].Insert(fmt.Sprintf("/%s/%s/%s", tname, nid, key), entry)
	nDB.indexes[byNetwork].Insert(fmt.Sprintf("/%s/%s/%s", nid, tname, key), entry)
	nDB.Unlock()

	nDB.broadcaster.Write(makeEvent(opDelete, tname, nid, key, value))
	return nil
}

func (nDB *NetworkDB) deleteNodeTableEntries(node string) {
	nDB.Lock()
	nDB.indexes[byTable].Walk(func(path string, v interface{}) bool {
		oldEntry := v.(*entry)
		if oldEntry.node != node {
			return false
		}

		params := strings.Split(path[1:], "/")
		tname := params[0]
		nid := params[1]
		key := params[2]

		entry := &entry{
			ltime:      oldEntry.ltime,
			node:       node,
			value:      oldEntry.value,
			deleting:   true,
			deleteTime: time.Now(),
		}

		nDB.indexes[byTable].Insert(fmt.Sprintf("/%s/%s/%s", tname, nid, key), entry)
		nDB.indexes[byNetwork].Insert(fmt.Sprintf("/%s/%s/%s", nid, tname, key), entry)
		return false
	})
	nDB.Unlock()
}

func (nDB *NetworkDB) WalkTable(tname string, fn func(string, string, []byte) bool) error {
	nDB.RLock()
	values := make(map[string]interface{})
	nDB.indexes[byTable].WalkPrefix(fmt.Sprintf("/%s", tname), func(path string, v interface{}) bool {
		values[path] = v
		return false
	})
	nDB.RUnlock()

	for k, v := range values {
		params := strings.Split(k[1:], "/")
		nid := params[1]
		key := params[2]
		if fn(nid, key, v.(*entry).value) {
			return nil
		}
	}

	return nil
}

func (nDB *NetworkDB) JoinNetwork(nid string) error {
	ltime := nDB.networkClock.Increment()

	if err := nDB.sendNetworkEvent(nid, networkJoin, ltime); err != nil {
		return fmt.Errorf("failed to send leave network event for %s: %v", nid, err)
	}

	nDB.Lock()
	nodeNetworks, ok := nDB.networks[nDB.config.NodeName]
	if !ok {
		nodeNetworks = make(map[string]*network)
		nDB.networks[nDB.config.NodeName] = nodeNetworks
	}
	nodeNetworks[nid] = &network{id: nid, ltime: ltime}
	nodeNetworks[nid].tableBroadcasts = &memberlist.TransmitLimitedQueue{
		NumNodes: func() int {
			return len(nDB.networkNodes[nid])
		},
		RetransmitMult: 4,
	}
	nDB.networkNodes[nid] = append(nDB.networkNodes[nid], nDB.config.NodeName)
	nDB.Unlock()

	if _, err := nDB.bulkSync(nid, true); err != nil {
		logrus.Errorf("Error bulk syncing while joining network %s: %v", nid, err)
	}

	return nil
}

func (nDB *NetworkDB) LeaveNetwork(nid string) error {
	ltime := nDB.networkClock.Increment()
	if err := nDB.sendNetworkEvent(nid, networkLeave, ltime); err != nil {
		return fmt.Errorf("failed to send leave network event for %s: %v", nid, err)
	}

	nDB.Lock()
	defer nDB.Unlock()
	nodeNetworks, ok := nDB.networks[nDB.config.NodeName]
	if !ok {
		return fmt.Errorf("could not find self node for network %s while trying to leave", nid)
	}

	n, ok := nodeNetworks[nid]
	if !ok {
		return fmt.Errorf("could not find network %s while trying to leave", nid)
	}

	n.ltime = ltime
	n.leaving = true
	return nil
}

// Deletes the node from the list of nodes which participate in the
// passed network. Caller should hold the NetworkDB lock while calling
// this
func (nDB *NetworkDB) deleteNetworkNode(nid string, nodeName string) {
	nodes := nDB.networkNodes[nid]
	for i, name := range nodes {
		if name == nodeName {
			nodes[i] = nodes[len(nodes)-1]
			nodes = nodes[:len(nodes)-1]
			break
		}
	}
	nDB.networkNodes[nid] = nodes
}

func (nDB *NetworkDB) findCommonNetworks(nodeName string) []string {
	nDB.RLock()
	defer nDB.RUnlock()

	var networks []string
	for nid, _ := range nDB.networks[nDB.config.NodeName] {
		if _, ok := nDB.networks[nodeName][nid]; ok {
			networks = append(networks, nid)
		}
	}

	return networks
}
