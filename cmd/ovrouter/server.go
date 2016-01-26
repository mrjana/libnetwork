package main

import (
	"fmt"
	"log"
	"net"

	"github.com/docker/go-plugins-helpers/network"
	"github.com/docker/libnetwork/driverapi"
	"github.com/docker/libnetwork/types"
)

type ovRouterServer struct {
	ovr *ovRouter
}

type ovEndpointInterface struct {
	req   *network.CreateEndpointRequest
	iface *network.EndpointInterface
}

func (ovi *ovEndpointInterface) SetMacAddress(mac net.HardwareAddr) error {
	ovi.iface.MacAddress = mac.String()
	return nil
}

func (ovi *ovEndpointInterface) SetIPAddress(ip *net.IPNet) error {
	ovi.iface.Address = ip.String()
	return nil
}

func (ovi *ovEndpointInterface) MacAddress() net.HardwareAddr {
	mac, err := net.ParseMAC(ovi.req.Interface.MacAddress)
	if err != nil {
		return nil
	}

	return mac
}

func (ovi *ovEndpointInterface) Address() *net.IPNet {
	ip, err := types.ParseCIDR(ovi.req.Interface.Address)
	if err != nil {
		return nil
	}

	return ip
}

func (ovi *ovEndpointInterface) AddressIPv6() *net.IPNet {
	ip, err := types.ParseCIDR(ovi.req.Interface.AddressIPv6)
	if err != nil {
		return nil
	}

	return ip
}

func (ors *ovRouterServer) GetCapabilities() (*network.CapabilitiesResponse, error) {
	return &network.CapabilitiesResponse{Scope: network.GlobalScope}, nil
}

func (ors *ovRouterServer) CreateNetwork(req *network.CreateNetworkRequest) error {
	log.Printf("Entering create network of ovrouter")
	ilv4 := make([]driverapi.IPAMData, 0, len(req.IPv4Data))
	for _, ipd := range req.IPv4Data {
		pool, err := types.ParseCIDR(ipd.Pool)
		if err != nil {
			return fmt.Errorf("could not parse ip pool %s while creating network: %v", ipd.Pool, err)
		}

		gw, err := types.ParseCIDR(ipd.Gateway)
		if err != nil {
			return fmt.Errorf("could not parse gateway %s while creating network: %v", ipd.Gateway, err)
		}

		ipdv4 := driverapi.IPAMData{
			AddressSpace: ipd.AddressSpace,
			Pool:         pool,
			Gateway:      gw,
		}

		ilv4 = append(ilv4, ipdv4)
	}

	// ilv6 := make([]driverapi.IPAMData, 0, len(req.IPv6Data))
	// for _, ipd := range req.IPv6Data {
	//	ilv6 = append(ilv6, ipd)
	// }

	return ors.ovr.d.CreateNetwork(req.NetworkID, req.Options, ilv4, nil)
}

func (ors *ovRouterServer) DeleteNetwork(req *network.DeleteNetworkRequest) error {
	return ors.ovr.d.DeleteNetwork(req.NetworkID)
}

func (ors *ovRouterServer) CreateEndpoint(req *network.CreateEndpointRequest) (*network.CreateEndpointResponse, error) {
	ovi := &ovEndpointInterface{req: req, iface: &network.EndpointInterface{}}
	if err := ors.ovr.d.CreateEndpoint(req.NetworkID, req.EndpointID, ovi, req.Options); err != nil {
		return nil, err
	}

	return &network.CreateEndpointResponse{Interface: ovi.iface}, nil
}

func (ors *ovRouterServer) DeleteEndpoint(req *network.DeleteEndpointRequest) error {
	return ors.ovr.d.DeleteEndpoint(req.NetworkID, req.EndpointID)
}

func (ors *ovRouterServer) EndpointInfo(req *network.InfoRequest) (*network.InfoResponse, error) {
	return nil, nil
}

type ovJoinResponse struct {
	resp network.JoinResponse
}

func (ojr *ovJoinResponse) SetNames(srcName, dstPrefix string) error {
	ojr.resp.InterfaceName.SrcName = srcName
	ojr.resp.InterfaceName.DstPrefix = dstPrefix
	return nil
}

func (ojr *ovJoinResponse) InterfaceName() driverapi.InterfaceNameInfo {
	return ojr
}

func (ojr *ovJoinResponse) SetGateway(ip net.IP) error {
	ojr.resp.Gateway = ip.String()
	return nil
}

func (ojr *ovJoinResponse) SetGatewayIPv6(net.IP) error {
	return nil
}

func (ojr *ovJoinResponse) AddStaticRoute(destination *net.IPNet, routeType int, nextHop net.IP) error {
	ojr.resp.StaticRoutes = append(ojr.resp.StaticRoutes, &network.StaticRoute{
		Destination: destination.String(),
		RouteType:   routeType,
		NextHop:     nextHop.String(),
	})
	return nil
}

func (ojr *ovJoinResponse) DisableGatewayService() {
}

func (ors *ovRouterServer) Join(req *network.JoinRequest) (*network.JoinResponse, error) {
	ojr := &ovJoinResponse{}
	if err := ors.ovr.d.Join(req.NetworkID, req.EndpointID, req.SandboxKey, ojr, req.Options); err != nil {
		return nil, err
	}
	return &ojr.resp, nil
}

func (ors *ovRouterServer) Leave(req *network.LeaveRequest) error {
	return ors.ovr.d.Leave(req.NetworkID, req.EndpointID)
}

type DiscoveryNotification struct {
	DiscoveryType int
	DiscoveryData interface{}
}

func (ors *ovRouterServer) DiscoverNew(notif *network.DiscoveryNotification) error {
	return ors.ovr.d.DiscoverNew(driverapi.DiscoveryType(notif.DiscoveryType), notif.DiscoveryData)
}

func (ors *ovRouterServer) DiscoverDelete(notif *network.DiscoveryNotification) error {
	return ors.ovr.d.DiscoverDelete(driverapi.DiscoveryType(notif.DiscoveryType), notif.DiscoveryData)
}

func (ovr *ovRouter) serverStart() error {
	d := &ovRouterServer{ovr: ovr}
	h := network.NewHandler(d)
	go h.ServeTCP("ovrouter", ":8080")
	return nil
}
