package drvregistry

import (
	"fmt"
	"sync"

	"github.com/docker/docker/pkg/plugins"
	"github.com/docker/libnetwork/driverapi"
	"github.com/docker/libnetwork/types"
)

type DriverRegistry interface {
	// Find a driver given a name.
	Find(string) (driverapi.Driver, error)

	// Scope returns the scope of the driver with a given name.
	Scope(string) (string, error)
}

type driverInitFn func(driverapi.DriverCallback, map[string]interface{}) error

type initializer struct {
	fn    driverInitFn
	ntype string
}

type driver struct {
	d     interface{}
	scope string
}

type driverMap map[string]driver

type driverRegistry struct {
	mp driverMap
	sync.Mutex
}

func (dr *driverRegistry) RegisterDriver(name string, d driverapi.Driver, c driverapi.Capability) error {
	dr.Lock()
	defer dr.Unlock()

	dr.mp[name] = driver{d: d, scope: c.DataScope}
	return nil
}

func (dr *driverRegistry) lookup(name string) (*driver, error) {
	dr.Lock()
	defer dr.Unlock()

	if mp, ok := dr.mp[name]; ok {
		return &mp, nil
	}

	// Plugins pkg performs lazy loading of plugins that acts as remote drivers.
	// As per the design, this Get call will result in remote driver discovery if there is a corresponding plugin available.
	_, err := plugins.Get(name, driverapi.NetworkPluginEndpointType)
	if err != nil {
		if err == plugins.ErrNotFound {
			return nil, types.NotFoundErrorf(err.Error())
		}
		return nil, err
	}

	if mp, ok := dr.mp[name]; ok {
		return &mp, nil
	}

	return nil, fmt.Errorf("could not find driver for type %s", name)
}

func (dr *driverRegistry) Find(name string) (driverapi.Driver, error) {
	drv, err := dr.lookup(name)
	if err != nil {
		return nil, err
	}

	return drv.d.(driverapi.Driver), nil
}

func (dr *driverRegistry) Scope(name string) (string, error) {
	drv, err := dr.lookup(name)
	if err != nil {
		return "", err
	}

	return drv.scope, nil
}

// NewNetDriverRegistry initalizes the network drivers registry and runs the driver initialization code.
// It will pass along any failures in driver initialization code.
func NewDriverRegistry(types []string) (DriverRegistry, error) {
	dr := &driverRegistry{mp: driverMap{}}
	for _, i := range getInitializers(types) {
		if err := i.fn(dr, nil); err != nil {
			return nil, err
		}
	}

	return dr, nil
}
