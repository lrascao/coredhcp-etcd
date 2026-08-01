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
	constDefaultSeparator    = "::"
	constDefaultLeaseTime    = 10 * time.Minute
	constDefaultMinLeaseTime = 1 * time.Minute
	constDefaultMaxLeaseTime = 24 * time.Hour
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
			log.Errorf("unable to allocate IP for MAC %s: %v", req.ClientHWAddr.String(), err)
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
			log.Errorf("unable to fetch free IP: %v", err)
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
		if requested := req.IPAddressLeaseTime(leaseTime); requested != leaseTime {
			log.Debugf("client requested lease time of %v, using that", requested)
			leaseTime = requested
		}

		// a client can ask for anything it likes, including a lease that
		// outlives the pool, so bound it before we act on it
		if clamped := p.config.clampLeaseTime(leaseTime); clamped != leaseTime {
			log.Debugf("clamping requested lease time %v to %v", leaseTime, clamped)
			leaseTime = clamped
		}

		// whatever we ended up with is what we're going to hold in etcd, so
		// it's what the reply has to advertise
		if resp.IPAddressLeaseTime(constDefaultLeaseTime) != leaseTime {
			resp.UpdateOption(dhcpv4.OptIPAddressLeaseTime(leaseTime))
		}

		// lease the IP in etcd
		leaseID, err := p.leaseIP(ctx, req.ClientHWAddr, ip, leaseTime)
		if err != nil {
			log.Errorf("unable to lease nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
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

		// register DNS if available, on the same etcd lease as the address so
		// that the records live and die with it
		if hostname := req.HostName(); hostname != "" {
			if err := p.dns.Register(ctx, p.client, hostname, ip, req.ClientHWAddr,
				leaseID); err != nil {
				log.Errorf("unable to register DNS name %s for ip %s: %v", hostname, ip, err)
				return nil, true
			}
		}

		log.Infof("return requested IP %s for MAC %s", ip, req.ClientHWAddr)

	case dhcpv4.MessageTypeRelease:
		// is the message meant for this server?
		if !req.ServerIdentifier().Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP release meant for %s", req.ServerIdentifier())
			return nil, true
		}

		// RFC 2131 has the released address in ciaddr, only give up a lease the
		// client can name rather than whatever this nic happens to hold
		ip := req.ClientIPAddr
		if ip == nil || ip.IsUnspecified() {
			log.Errorf("ignoring DHCP release from %s without a client IP address",
				req.ClientHWAddr)
			return nil, true
		}

		if err := p.revokeLease(ctx, req.ClientHWAddr, ip); err != nil {
			log.Errorf("error revoking lease for nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
			return nil, true
		}

	case dhcpv4.MessageTypeDecline:
		// is the message meant for this server?
		if !req.ServerIdentifier().Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP decline meant for %s", req.ServerIdentifier())
			return nil, true
		}

		// RFC 2131 has the declined address in the requested IP option, ciaddr
		// is zero for a DECLINE
		ip := req.RequestedIPAddress()
		if ip == nil || ip.IsUnspecified() {
			log.Errorf("ignoring DHCP decline from %s without a requested IP address",
				req.ClientHWAddr)
			return nil, true
		}

		if err := p.revokeLease(ctx, req.ClientHWAddr, ip); err != nil {
			log.Errorf("error revoking lease for nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
			return nil, true
		}

	default:
		log.Errorf("unhandled DHCPv4 packet %v (%s): ", req.MessageType(), req.Summary())
	}

	return resp, false
}
