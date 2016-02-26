package ovmanager

import (
	"fmt"
	"sync"

	"github.com/docker/libnetwork/datastore"
	"github.com/docker/libnetwork/discoverapi"
	"github.com/docker/libnetwork/driverapi"
	"github.com/docker/libnetwork/idm"
	"github.com/docker/libnetwork/netlabel"
	"github.com/docker/libnetwork/types"
)

const (
	networkType  = "overlay"
	vxlanIDStart = 256
	vxlanIDEnd   = 1000
)

type networkTable map[string]*network

type driver struct {
	config   map[string]interface{}
	networks networkTable
	store    datastore.DataStore
	vxlanIdm *idm.Idm
	sync.Mutex
}

type network struct{}

// type network struct {
//	id        string
//	dbIndex   uint64
//	dbExists  bool
//	sbox      osl.Sandbox
//	endpoints endpointTable
//	driver    *driver
//	joinCnt   int
//	once      *sync.Once
//	initEpoch int
//	initErr   error
//	subnets   []*subnet
//	sync.Mutex
// }

// Init registers a new instance of overlay driver
func Init(dc driverapi.DriverCallback, config map[string]interface{}) error {
	var err error
	c := driverapi.Capability{
		DataScope: datastore.GlobalScope,
	}

	d := &driver{
		networks: networkTable{},
		config:   config,
	}

	if data, ok := config[netlabel.GlobalKVClient]; ok {
		var err error
		dsc, ok := data.(discoverapi.DatastoreConfigData)
		if !ok {
			return types.InternalErrorf("incorrect data in datastore configuration: %v", data)
		}
		d.store, err = datastore.NewDataStoreFromConfig(dsc)
		if err != nil {
			return types.InternalErrorf("failed to initialize data store: %v", err)
		}
	}

	d.vxlanIdm, err = idm.New(d.store, "vxlan-id", vxlanIDStart, vxlanIDEnd)
	if err != nil {
		return fmt.Errorf("failed to initialize vxlan id manager: %v", err)
	}

	return dc.RegisterDriver(networkType, d, c)
}

func (d *driver) NetworkAllocate(id string, option map[string]interface{}, ipV4Data, ipV6Data []driverapi.IPAMData) (map[string]string, error) {
	return nil, nil
}

func (d *driver) NetworkFree(id string) error {
	return nil
}

func (d *driver) CreateNetwork(id string, option map[string]interface{}, ipV4Data, ipV6Data []driverapi.IPAMData) error {
	return fmt.Errorf("not implemented")
}

func (d *driver) DeleteNetwork(nid string) error {
	return fmt.Errorf("not implemented")
}

func (d *driver) CreateEndpoint(nid, eid string, ifInfo driverapi.InterfaceInfo, epOptions map[string]interface{}) error {
	return fmt.Errorf("not implemented")
}

func (d *driver) DeleteEndpoint(nid, eid string) error {
	return fmt.Errorf("not implemented")
}

func (d *driver) EndpointOperInfo(nid, eid string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}

// Join method is invoked when a Sandbox is attached to an endpoint.
func (d *driver) Join(nid, eid string, sboxKey string, jinfo driverapi.JoinInfo, options map[string]interface{}) error {
	return fmt.Errorf("not implemented")
}

// Leave method is invoked when a Sandbox detaches from an endpoint.
func (d *driver) Leave(nid, eid string) error {
	return fmt.Errorf("not implemented")
}

func (d *driver) Type() string {
	return networkType
}

// DiscoverNew is a notification for a new discovery event, such as a new node joining a cluster
func (d *driver) DiscoverNew(dType discoverapi.DiscoveryType, data interface{}) error {
	return fmt.Errorf("not implemented")
}

// DiscoverDelete is a notification for a discovery delete event, such as a node leaving a cluster
func (d *driver) DiscoverDelete(dType discoverapi.DiscoveryType, data interface{}) error {
	return fmt.Errorf("not implemented")
}
