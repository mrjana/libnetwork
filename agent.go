package libnetwork

import (
	"fmt"
	"net"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/docker/go-events"
	"github.com/docker/libnetwork/driverapi"
	"github.com/docker/libnetwork/networkdb"
)

type agent struct {
	networkDB         *networkdb.NetworkDB
	epTblCancel       func()
	driverCancelFuncs map[string][]func()
}

func (c *controller) agentInit(bindAddr string) error {
	if !c.cfg.Daemon.IsAgent {
		return nil
	}

	nDB, err := networkdb.New(&networkdb.Config{BindAddr: bindAddr})
	if err != nil {
		return err
	}

	ch, cancel := nDB.Watch("endpoint_table", "", "")

	c.agent = &agent{
		networkDB:         nDB,
		epTblCancel:       cancel,
		driverCancelFuncs: make(map[string][]func()),
	}

	go c.handleTableEvents(ch, c.handleEpTableEvent)
	return nil
}

func (c *controller) agentJoin(remotes []string) error {
	if c.agent == nil {
		return nil
	}

	return c.agent.networkDB.Join(remotes)
}

func (c *controller) agentClose() {
	if c.agent == nil {
		return
	}

	for _, cancelFuncs := range c.agent.driverCancelFuncs {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}
	c.agent.epTblCancel()

	c.agent.networkDB.Close()
}

func (n *network) joinCluster() error {
	c := n.getController()
	if c.agent == nil {
		return nil
	}

	return c.agent.networkDB.JoinNetwork(n.ID())
}

func (n *network) leaveCluster() error {
	c := n.getController()
	if c.agent == nil {
		return nil
	}

	return c.agent.networkDB.LeaveNetwork(n.ID())
}

func (ep *endpoint) addToCluster() error {
	n := ep.getNetwork()
	c := n.getController()
	if c.agent == nil {
		return nil
	}

	if !ep.isAnonymous() && ep.Iface().Address() != nil {
		if err := c.agent.networkDB.CreateEntry("endpoint_table", n.ID(), ep.ID(), []byte(fmt.Sprintf("%s=%s", ep.Name(), ep.Iface().Address().IP))); err != nil {
			return err
		}
	}

	for _, te := range ep.joinInfo.driverTableEntries {
		if err := c.agent.networkDB.CreateEntry(te.tableName, n.ID(), te.key, te.value); err != nil {
			return err
		}
	}

	return nil
}

func (ep *endpoint) deleteFromCluster() error {
	n := ep.getNetwork()
	c := n.getController()
	if c.agent == nil {
		return nil
	}

	if !ep.isAnonymous() {
		if err := c.agent.networkDB.DeleteEntry("endpoint_table", n.ID(), ep.ID()); err != nil {
			return err
		}
	}

	for _, te := range ep.joinInfo.driverTableEntries {
		if err := c.agent.networkDB.DeleteEntry(te.tableName, n.ID(), te.key); err != nil {
			return err
		}
	}

	return nil
}

func (n *network) addDriverWatch(tableName string) {
	c := n.getController()
	if c.agent == nil {
		return
	}

	ch, cancel := c.agent.networkDB.Watch(tableName, n.ID(), "")
	c.Lock()
	c.agent.driverCancelFuncs[n.ID()] = append(c.agent.driverCancelFuncs[n.ID()], cancel)
	c.Unlock()

	go c.handleTableEvents(ch, n.handleDriverTableEvent)
}

func (n *network) cancelDriverWatches() {
	c := n.getController()
	if c.agent == nil {
		return
	}

	c.Lock()
	cancelFuncs := c.agent.driverCancelFuncs[n.ID()]
	delete(c.agent.driverCancelFuncs, n.ID())
	c.Unlock()

	for _, cancel := range cancelFuncs {
		cancel()
	}
}

func (c *controller) handleTableEvents(ch chan events.Event, fn func(events.Event)) {
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}

			fn(ev)
		}
	}
}

func (n *network) handleDriverTableEvent(ev events.Event) {
	d, err := n.driver(false)
	if err != nil {
		logrus.Errorf("Could not resolve driver %s while handling driver table event: %v", n.networkType, err)
		return
	}

	var (
		etype driverapi.EventType
		tname string
		key   string
		value []byte
	)

	switch event := ev.(type) {
	case networkdb.CreateEvent:
		tname = event.Table
		key = event.Key
		value = event.Value
		etype = driverapi.Create
	case networkdb.DeleteEvent:
		tname = event.Table
		key = event.Key
		value = event.Value
		etype = driverapi.Delete
	case networkdb.UpdateEvent:
		tname = event.Table
		key = event.Key
		value = event.Value
		etype = driverapi.Delete
	}

	d.EventNotify(etype, tname, n.ID(), key, value)
}

func (c *controller) handleEpTableEvent(ev events.Event) {
	var (
		id    string
		value string
		isAdd bool
	)

	switch event := ev.(type) {
	case networkdb.CreateEvent:
		id = event.NetworkID
		value = string(event.Value)
		isAdd = true
	case networkdb.DeleteEvent:
		id = event.NetworkID
		value = string(event.Value)
	case networkdb.UpdateEvent:
		logrus.Errorf("Unexpected update service table event = %#v", event)
	}

	nw, err := c.NetworkByID(id)
	if err != nil {
		logrus.Errorf("Could not find network %s while handling service table event: %v", id, err)
		return
	}
	n := nw.(*network)

	pair := strings.Split(value, "=")
	if len(pair) < 2 {
		logrus.Errorf("Incorrect service table value = %s", value)
		return
	}

	name := pair[0]
	ip := net.ParseIP(pair[1])

	if name == "" || ip == nil {
		logrus.Errorf("Invalid endpoint name/ip received while handling service table event %s", value)
		return
	}

	if isAdd {
		n.addSvcRecords(name, ip, true)
	} else {
		n.deleteSvcRecords(name, ip, true)
	}
}
