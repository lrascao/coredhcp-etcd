package etcdplugin

import (
	"context"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/coredhcp/coredhcp/logger"
	"github.com/coredhcp/coredhcp/plugins"
	"github.com/coredhcp/coredhcp/plugins/allocators"
	"github.com/insomniacslk/dhcp/dhcpv4"
)

// Plugin wraps plugin registration information
var Plugin = plugins.Plugin{
	Name:   "etcd",
	Setup4: setup,
}

const (
	constDefaultSeparator = "::"
	constDefaultLeaseTime = 10 * time.Minute
)

// PluginState is the data held by an instance of the range plugin
type PluginState struct {
	// Rough lock for the whole plugin, we'll get better performance once we use leasestorage
	sync.Mutex
	config    Config
	client    *etcd.Client
	allocator allocators.Allocator
	dns       *DNS
	grp       *errgroup.Group
}

// various global variables
var (
	log = logger.GetLogger("plugins/etcd")
)

// Handler4 handles DHCPv4 packets for the etcd plugin
func (p *PluginState) Handler4(req, resp *dhcpv4.DHCPv4) (*dhcpv4.DHCPv4, bool) {
	p.Lock()
	defer p.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	log.Debugf("got DHCPv4 packet %v", req.MessageType())
	log.Debugf("%v", req.Summary())

	defer func() {
		log.Debugf("replying with DHCPv4 packet: %v", resp.MessageType())
		log.Debugf("%v", resp.Summary())
	}()

	switch req.MessageType() {
	case dhcpv4.MessageTypeDiscover:
		ip, err := p.nicLeasedIP(ctx, req.ClientHWAddr)
		if err != nil {
			log.Errorf("unable to allocate IP for MAC %s: %w", req.ClientHWAddr.String(), err)
			return nil, true
		}
		if ip != nil {
			resp.YourIPAddr = ip
			log.Infof("found previous lease for %s: %s", req.ClientHWAddr, ip)
			return resp, false
		}

		// fetch a free ip
		ip, err = p.freeIP(ctx)
		if err != nil {
			log.Errorf("unable to fetch free IP: %w", err)
			return nil, true
		}

		// return the free to our client
		resp.YourIPAddr = ip

		log.Infof("returning IP %s for MAC %s", resp.YourIPAddr, req.ClientHWAddr.String())

	case dhcpv4.MessageTypeRequest:
		reqServerIP := req.ServerIdentifier()

		// deny REQUESTs without a server identifier
		if reqServerIP == nil {
			log.Errorf("no server identifier in DHCP request, returning negative reply")
			resp.UpdateOption(dhcpv4.OptMessageType(dhcpv4.MessageTypeNak))
			return resp, false
		}

		// is the message meant for this server?
		if !reqServerIP.Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP request meant for %s", reqServerIP)
			return nil, true
		}

		// prefer renewing leases
		ip := req.ClientIPAddr
		if req.RequestedIPAddress() != nil {
			ip = req.RequestedIPAddress()
		}

		leaseTime := resp.IPAddressLeaseTime(constDefaultLeaseTime)
		// did the client request a different lease time than what
		// we're configured with?
		if req.IPAddressLeaseTime(leaseTime) != leaseTime {
			log.Debugf("client requested lease time of %v, using that",
				req.IPAddressLeaseTime(leaseTime))
			leaseTime = req.IPAddressLeaseTime(leaseTime)

			resp.UpdateOption(dhcpv4.OptIPAddressLeaseTime(leaseTime))
		}

		// lease the IP in etcd
		if err := p.leaseIP(ctx, req.ClientHWAddr, ip, leaseTime); err != nil {
			log.Errorf("unable to lease nic %s, ip %s: %w", req.ClientHWAddr, ip, err)
			if IsAlreadyLeased(err) {
				log.Debugf("ip %s already leased, returning negative reply to DHCP request", ip)
				// return a negative reply
				resp.UpdateOption(dhcpv4.OptMessageType(dhcpv4.MessageTypeNak))
				return resp, false
			}
			return nil, true
		}

		// set ip reply
		resp.YourIPAddr = ip

		// register DNS if available
		if hostname := req.HostName(); hostname != "" {
			if err := p.dns.Register(ctx, p.client, hostname, ip, req.ClientHWAddr,
				leaseTime); err != nil {
				return nil, true
			}
		}

		log.Infof("return requested IP %s for MAC %s", ip, req.ClientHWAddr)

	case dhcpv4.MessageTypeRelease, dhcpv4.MessageTypeDecline:
		// is the message meant for this server?
		if !req.ServerIdentifier().Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP release meant for %s", req.ServerIdentifier())
			return nil, true
		}

		if err := p.revokeLease(ctx, req.ClientHWAddr); err != nil {
			log.Errorf("error revoking lease for nic %s: %v", req.ClientHWAddr, err)
			return nil, true
		}

	default:
		log.Errorf("unhandled DHCPv4 packet %v (%s): ", req.MessageType(), req.Summary())
	}

	return resp, false
}
