package drvregistry

import (
	"github.com/docker/libnetwork/drivers/bridge"
	"github.com/docker/libnetwork/drivers/host"
	"github.com/docker/libnetwork/drivers/null"
	"github.com/docker/libnetwork/drivers/overlay"
	"github.com/docker/libnetwork/drivers/remote"
)

var initializerTbl = map[string]driverInitFn{
	"bridge":  bridge.Init,
	"host":    host.Init,
	"null":    null.Init,
	"remote":  remote.Init,
	"overlay": overlay.Init,
}

func getInitializers(types []string) []initializer {
	var i []initializer

	for _, name := range types {
		i = append(i, initializer{ntype: name, fn: initializerTbl[name]})
	}

	return i
}
