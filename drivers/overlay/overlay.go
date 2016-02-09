package overlay

import (
	"fmt"
	"net"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/docker/libkv/store"
	"github.com/docker/libnetwork/datastore"
	"github.com/docker/libnetwork/driverapi"
	"github.com/docker/libnetwork/idm"
	"github.com/docker/libnetwork/netlabel"
	"github.com/hashicorp/serf/serf"
)

const (
	networkType  = "overlay"
	vethPrefix   = "veth"
	vethLen      = 7
	vxlanIDStart = 256
	vxlanIDEnd   = 1000
	vxlanPort    = 4789
	vxlanVethMTU = 1450
)

type driver struct {
	eventCh      chan serf.Event
	notifyCh     chan ovNotify
	exitCh       chan chan struct{}
	bindAddress  string
	neighIP      string
	config       map[string]interface{}
	peerDb       peerNetworkMap
	serfInstance *serf.Serf
	networks     networkTable
	store        datastore.DataStore
	ipAllocator  *idm.Idm
	vxlanIdm     *idm.Idm
	once         sync.Once
	joinOnce     sync.Once
	scope        string
	sync.Mutex
}

// Init registers a new instance of overlay driver
func Init(dc driverapi.DriverCallback, config map[string]interface{}) error {
	d := &driver{
		networks: networkTable{},
		peerDb: peerNetworkMap{
			mp: map[string]*peerMap{},
		},
		config: config,
	}

	if err := d.procBindInterface(); err != nil {
		return fmt.Errorf("failed using bind interface option: %v", err)
	}

	if err := d.procNeighIP(); err != nil {
		return fmt.Errorf("failed using neighbor IP option: %v", err)
	}

	c := driverapi.Capability{
		DataScope: d.chooseScope(),
	}

	d.scope = c.DataScope
	return dc.RegisterDriver(networkType, d, c)
}

// Fini cleans up the driver resources
func Fini(drv driverapi.Driver) {
	d := drv.(*driver)

	if d.exitCh != nil {
		waitCh := make(chan struct{})

		d.exitCh <- waitCh

		<-waitCh
	}
}

func (d *driver) chooseScope() string {
	_, provOk := d.config[netlabel.GlobalKVProvider]
	_, urlOk := d.config[netlabel.GlobalKVProviderURL]

	if provOk && urlOk {
		return datastore.GlobalScope
	}

	return datastore.LocalScope
}

func (d *driver) getStoreConfig() *datastore.ScopeCfg {
	var providerLabel, urlLabel, cfgLabel string
	switch d.scope {
	case datastore.GlobalScope:
		providerLabel = netlabel.GlobalKVProvider
		urlLabel = netlabel.GlobalKVProviderURL
		cfgLabel = netlabel.GlobalKVProviderConfig
	case datastore.LocalScope:
		providerLabel = netlabel.LocalKVProvider
		urlLabel = netlabel.LocalKVProviderURL
		cfgLabel = netlabel.LocalKVProviderConfig
	}

	provider, provOk := d.config[providerLabel]
	provURL, urlOk := d.config[urlLabel]
	provConfig, confOk := d.config[cfgLabel]

	if !provOk || !urlOk {
		return nil
	}

	cfg := &datastore.ScopeCfg{
		Client: datastore.ScopeClientCfg{
			Provider: provider.(string),
			Address:  provURL.(string),
		},
	}

	if confOk {
		cfg.Client.Config = provConfig.(*store.Config)
	}

	return cfg
}

func (d *driver) configure() error {
	var err error

	if len(d.config) == 0 {
		return nil
	}

	d.once.Do(func() {
		cfg := d.getStoreConfig()
		if cfg == nil {
			return
		}

		d.store, err = datastore.NewDataStore(d.scope, cfg)
		if err != nil {
			err = fmt.Errorf("failed to initialize data store: %v", err)
			return
		}

		d.vxlanIdm, err = idm.New(d.store, "vxlan-id", vxlanIDStart, vxlanIDEnd)
		if err != nil {
			err = fmt.Errorf("failed to initialize vxlan id manager: %v", err)
			return
		}
	})

	return err
}

func (d *driver) Type() string {
	return networkType
}

func validateSelf(node string) error {
	advIP := net.ParseIP(node)
	if advIP == nil {
		return fmt.Errorf("invalid self address (%s)", node)
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return fmt.Errorf("Unable to get interface addresses %v", err)
	}
	for _, addr := range addrs {
		ip, _, err := net.ParseCIDR(addr.String())
		if err == nil && ip.Equal(advIP) {
			return nil
		}
	}
	return fmt.Errorf("Multi-Host overlay networking requires cluster-advertise(%s) to be configured with a local ip-address that is reachable within the cluster", advIP.String())
}

func (d *driver) nodeJoin(node string, self bool) {
	if self && !d.isSerfAlive() {
		if err := validateSelf(node); err != nil {
			logrus.Errorf("%s", err.Error())
		}
		d.Lock()
		d.bindAddress = node
		d.Unlock()
		err := d.serfInit()
		if err != nil {
			logrus.Errorf("initializing serf instance failed: %v", err)
			return
		}
	}

	d.Lock()
	if !self {
		d.neighIP = node
	}
	neighIP := d.neighIP
	d.Unlock()

	if d.serfInstance != nil && neighIP != "" {
		var err error
		d.joinOnce.Do(func() {
			err = d.serfJoin(neighIP)
			if err == nil {
				d.pushLocalDb()
			}
		})
		if err != nil {
			logrus.Errorf("joining serf neighbor %s failed: %v", node, err)
			d.Lock()
			d.joinOnce = sync.Once{}
			d.Unlock()
			return
		}
	}
}

func getBindAddr(ifaceName string) (string, error) {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return "", fmt.Errorf("failed to find interface %s: %v", ifaceName, err)
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %v", err)
	}

	for _, a := range addrs {
		addr, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		addrIP := addr.IP

		if addrIP.IsLinkLocalUnicast() {
			continue
		}

		return addrIP.String(), nil
	}

	return "", fmt.Errorf("failed to get bind address")
}

func (d *driver) procBindInterface() error {
	if bindIP, ok := d.config[netlabel.OverlayBindIP]; ok {
		d.nodeJoin(bindIP.(string), true)
		return nil
	}

	bif, ok := d.config[netlabel.OverlayBindInterface]
	if !ok {
		return nil
	}

	bindIf, ok := bif.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for bind interface option. expected string", bif)
	}

	bindIP, err := getBindAddr(bindIf)
	if err != nil {
		return fmt.Errorf("could not find a valid IP address for interface %s: %v", bif, err)
	}

	d.nodeJoin(bindIP, true)
	return nil
}

func (d *driver) procNeighIP() error {
	nip, ok := d.config[netlabel.OverlayNeighborIP]
	if !ok {
		return nil
	}

	neighIP, ok := nip.(string)
	if !ok {
		return fmt.Errorf("unexpected type %T for neighbor IP option. expected string", nip)
	}

	d.nodeJoin(neighIP, false)
	return nil
}

func (d *driver) pushLocalEndpointEvent(action, nid, eid string) {
	if !d.isSerfAlive() {
		return
	}
	d.notifyCh <- ovNotify{
		action: "join",
		nid:    nid,
		eid:    eid,
	}
}

// DiscoverNew is a notification for a new discovery event, such as a new node joining a cluster
func (d *driver) DiscoverNew(dType driverapi.DiscoveryType, data interface{}) error {
	if dType == driverapi.NodeDiscovery {
		nodeData, ok := data.(driverapi.NodeDiscoveryData)
		if !ok || nodeData.Address == "" {
			return fmt.Errorf("invalid discovery data")
		}
		d.nodeJoin(nodeData.Address, nodeData.Self)
	}
	return nil
}

// DiscoverDelete is a notification for a discovery delete event, such as a node leaving a cluster
func (d *driver) DiscoverDelete(dType driverapi.DiscoveryType, data interface{}) error {
	return nil
}
