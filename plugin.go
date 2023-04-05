package etcdplugin

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	etcd "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/coredhcp/coredhcp/handler"
	"github.com/coredhcp/coredhcp/logger"
	"github.com/coredhcp/coredhcp/plugins"
	"github.com/coredhcp/coredhcp/plugins/allocators"
	"github.com/coredhcp/coredhcp/plugins/allocators/bitmap"
	"github.com/insomniacslk/dhcp/dhcpv4"
)

// Plugin wraps plugin registration information
var Plugin = plugins.Plugin{
	Name:   "etcd",
	Setup4: setup,
}

const constDefaultSeparator = "::"

type Config struct {
	CA        string
	Cert      string
	Key       string
	Endpoints []string
	Start     string
	End       string
	Prefix    string
	Separator string
}

// PluginState is the data held by an instance of the range plugin
type PluginState struct {
	// Rough lock for the whole plugin, we'll get better performance once we use leasestorage
	sync.Mutex
	config    Config
	client    *etcd.Client
	allocator allocators.Allocator
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
		// is the message meant for this server?
		reqServerIP := req.ServerIdentifier()
		if reqServerIP != nil && !reqServerIP.Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP request meant for %s", reqServerIP)
			return nil, true
		}

		// prefer renewing leases
		ip := req.ClientIPAddr
		if req.RequestedIPAddress() != nil {
			ip = req.RequestedIPAddress()
		}

		// lease the IP in etcd
		if err := p.leaseIP(ctx, req.ClientHWAddr, ip,
			resp.IPAddressLeaseTime(30*time.Second)); err != nil {
			log.Errorf("unable to lease nic %s, ip %s: %w", req.ClientHWAddr, ip, err)
			return nil, true
		}

		// set ip reply
		resp.YourIPAddr = ip

		log.Infof("return requested IP %s for MAC %s", ip, req.ClientHWAddr)

	case dhcpv4.MessageTypeRelease, dhcpv4.MessageTypeDecline:
		// is the message meant for this server?
		if !req.ServerIdentifier().Equal(resp.ServerIPAddr) {
			// ignore
			log.Debugf("ignoring DHCP release meant for %s", req.ServerIdentifier())
			return nil, true
		}

		if err := p.revokeLease(ctx, req.ClientHWAddr); err != nil {
			log.Errorf("unable to revoke lease nic %s: %w", req.ClientHWAddr, err)
			return nil, true
		}

	default:
		log.Errorf("unhandled DHCPv4 packet %v (%s): ", req.MessageType(), req.Summary())
	}

	return resp, false
}

func setup(args0 ...string) (handler.Handler4, error) {
	args := strings.Join(args0, "\n")

	viper.SetConfigType("properties")
	viper.ReadConfig(bytes.NewBuffer([]byte(args)))

	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal config: %w", err)
	}

	if config.Separator == "" {
		config.Separator = constDefaultSeparator
	}

	ctx := context.Background()

	client, err := NewClient(ctx, config)
	if err != nil {
		return nil, err
	}

	ipStart := net.ParseIP(config.Start)
	if ipStart.To4() == nil {
		return nil, fmt.Errorf("invalid IPv4 address: %v", config.Start)
	}
	ipEnd := net.ParseIP(config.End)
	if ipEnd.To4() == nil {
		return nil, fmt.Errorf("invalid IPv4 address: %v", config.End)
	}
	if binary.BigEndian.Uint32(ipStart.To4()) >= binary.BigEndian.Uint32(ipEnd.To4()) {
		return nil, errors.New("start of IP range has to be lower than the end of an IP range")
	}

	allocator, err := bitmap.NewIPv4Allocator(ipStart, ipEnd)
	if err != nil {
		return nil, fmt.Errorf("could not create an allocator: %w", err)
	}

	grp, ctx := errgroup.WithContext(ctx)

	p := PluginState{
		config:    config,
		client:    client,
		allocator: allocator,
		grp:       grp,
	}

	if err := p.bootstrapLeasableRange(ctx); err != nil {
		return nil, fmt.Errorf("unable to bootstrap leasable range: ", err)
	}

	grp.Go(func() error {
		log.Info("starting lease monitor")
		err := p.monitorLeases(ctx, 10*time.Second)
		return errors.Wrap(err, "could not monitor leases")
	})

	return p.Handler4, nil
}
