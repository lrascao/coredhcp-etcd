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

	// Kea's default probation for a declined address is 24h. The pool here is
	// 100 addresses (192.168.3.100-199), and declining clients have been seen
	// at about four a minute. A client that declines whatever it is offered --
	// broken ARP probing, or something on the segment answering ARP for every
	// address -- takes one more address out with every DECLINE, so it keeps
	// four per minute of probation out at once: 24h would drain the free pool
	// in well under an hour and keep it drained for a day, ten minutes caps it
	// at about 40 and leaves the rest of the pool serving.
	//
	// What a short probation costs is that a conflict which outlasts it is
	// found again: once it expires the address is offered, probed and declined
	// once more. That is one retry for one client every ten minutes, where
	// before this the declining client was offered the same address on every
	// DISCOVER.
	constDefaultDeclineProbation = 10 * time.Minute

	// Every leftover RELEASE seen in production (18 of them from one Shelly
	// over 4.5 days) arrived within a second of the grant it destroyed. A
	// genuine release this soon after a grant costs nothing to ignore -- the
	// address stays with the same client until it expires -- so the margin is
	// deliberately wide.
	constDefaultReleaseGrace = 30 * time.Second

	// how soon after another server grants a binding a REQUEST without a server
	// identifier is taken to be the same broadcast that server answered, see
	// renewLease. The copies of one broadcast reach every server within
	// milliseconds of each other, and the margin is for the time a grant spends
	// between being stamped and being committed when etcd is slow. A client
	// that gets no answer retransmits after about 4s and again after about 12s
	// (RFC 2131 §4.1), so if the server that won crashes before answering, the
	// client is answered by the second retransmission at the latest.
	constAnsweredWindow = 5 * time.Second
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

		// is the message meant for this server? One without a server
		// identifier is a client holding on to the address it has rather than
		// choosing between offers, see confirm
		if reqServerIP != nil && !reqServerIP.Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP request meant for %s", reqServerIP)
			return nil, true
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

		if reqServerIP == nil {
			return p.confirm(ctx, req, resp, leaseTime)
		}

		// prefer renewing leases
		ip := req.ClientIPAddr
		if req.RequestedIPAddress() != nil {
			ip = req.RequestedIPAddress()
		}

		// lease the IP in etcd, as granted by this server
		leaseID, err := p.leaseIP(ctx, req.ClientHWAddr, ip, leaseTime, resp.ServerIPAddr)
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

		if err := p.releaseLease(ctx, req.ClientHWAddr, ip, resp.ServerIPAddr); err != nil {
			if IsStaleRelease(err) {
				log.Infof("ignoring stale DHCP release from %s, it predates the lease it names: %v",
					req.ClientHWAddr, err)
				return nil, true
			}
			log.Errorf("error revoking lease for nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
			return nil, true
		}

		log.Infof("released %s from %s", ip, req.ClientHWAddr)

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

		// no grant checks here: a DECLINE is the client's verdict on the lease
		// it was just granted, so arriving right after the grant is the norm
		if err := p.revokeLease(ctx, req.ClientHWAddr, ip); err != nil {
			log.Errorf("error revoking lease for nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
			return nil, true
		}

		log.Infof("%s declined %s as already in use, quarantined it for %v",
			req.ClientHWAddr, ip, p.config.DeclineProbation)

	default:
		log.Errorf("unhandled DHCPv4 packet %v (%s): ", req.MessageType(), req.Summary())
	}

	return resp, false
}

// confirm answers a REQUEST that carries no server identifier. RFC 2131
// §4.3.2 has a client leave option 54 out whenever it is holding on to an
// address it was already given, rather than taking one from an offer:
//
//   - INIT-REBOOT, after a reboot or a reconnect: the address in option 50,
//     ciaddr zero, broadcast
//   - RENEWING, from T1: the address in ciaddr, unicast to the server that
//     granted it
//   - REBINDING, from T2 when renewing got no answer: the address in ciaddr,
//     broadcast
//
// All three get an ACK that renews the binding when the address is the one the
// client holds, and a NAK when it names one the client does not hold, so that
// it stops using it and starts over. The exception is an INIT-REBOOT from a
// client there is no binding for at all, which gets no answer: the RFC says the
// server MUST remain silent, and the client falls back to DISCOVER on its own.
// RENEWING and REBINDING look the same from here, they only differ in where the
// packet was sent, and renewLease makes sure that only one server answers the
// broadcast ones.
func (p *PluginState) confirm(ctx context.Context, req, resp *dhcpv4.DHCPv4, leaseTime time.Duration) (*dhcpv4.DHCPv4, bool) {
	// RENEWING and REBINDING must leave option 50 out, and INIT-REBOOT must
	// leave ciaddr zero. A client that fills both in anyway is held to ciaddr,
	// the address it is actually using and the one the reply goes to
	ip, initReboot := req.ClientIPAddr, false
	if ip == nil || ip.IsUnspecified() {
		ip, initReboot = req.RequestedIPAddress(), true
	}
	if ip == nil || ip.IsUnspecified() {
		log.Errorf("DHCP request from %s names neither a server nor an address, returning negative reply",
			req.ClientHWAddr)
		resp.UpdateOption(dhcpv4.OptMessageType(dhcpv4.MessageTypeNak))
		return resp, false
	}

	leaseID, previous, err := p.renewLease(ctx, req.ClientHWAddr, ip, leaseTime, resp.ServerIPAddr)
	if err != nil {
		switch {
		case initReboot && IsNoBinding(err):
			log.Infof("staying silent on DHCP request from %s for %s, it has no binding: %v",
				req.ClientHWAddr, ip, err)
			return nil, true
		case IsNoBinding(err), IsNotLeased(err):
			log.Infof("DHCP request from %s for %s refused, returning negative reply: %v",
				req.ClientHWAddr, ip, err)
			resp.UpdateOption(dhcpv4.OptMessageType(dhcpv4.MessageTypeNak))
			return resp, false
		case IsAnswered(err):
			log.Debugf("leaving DHCP request from %s for %s to the server that answered it: %v",
				req.ClientHWAddr, ip, err)
			return nil, true
		default:
			log.Errorf("unable to renew nic %s, ip %s: %v", req.ClientHWAddr, ip, err)
			return nil, true
		}
	}

	resp.YourIPAddr = ip

	// the records go wherever the binding went, either written afresh under the
	// name the client gives now or carried over from the lease it was renewed
	// from when it gives none
	if hostname := req.HostName(); hostname != "" {
		if err := p.dns.Register(ctx, p.client, hostname, ip, req.ClientHWAddr,
			leaseID); err != nil {
			log.Errorf("unable to register DNS name %s for ip %s: %v", hostname, ip, err)
			return nil, true
		}
	} else if err := p.carryOver(ctx, previous, leaseID); err != nil {
		// the binding is renewed regardless, the records only lapse early
		log.Errorf("unable to carry the DNS records of %s over to its renewed lease: %v", ip, err)
	}

	log.Infof("renewed %s for %s", ip, req.ClientHWAddr)

	return resp, false
}
