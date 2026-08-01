package etcdplugin

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/insomniacslk/dhcp/dhcpv4"
	etcd "go.etcd.io/etcd/client/v3"
)

var testServerIP = net.IP{10, 0, 0, 254}

// reply builds the response skeleton coredhcp hands to the plugin chain.
func reply(t *testing.T, req *dhcpv4.DHCPv4, mt dhcpv4.MessageType) *dhcpv4.DHCPv4 {
	t.Helper()

	resp, err := dhcpv4.NewReplyFromRequest(req)
	if err != nil {
		t.Fatalf("could not build reply: %v", err)
	}
	if mt != dhcpv4.MessageTypeNone {
		resp.UpdateOption(dhcpv4.OptMessageType(mt))
	}
	resp.ServerIPAddr = testServerIP
	return resp
}

func discover(t *testing.T, mac net.HardwareAddr) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.NewDiscovery(mac)
	if err != nil {
		t.Fatalf("could not build discover: %v", err)
	}
	return req
}

func request(t *testing.T, mac net.HardwareAddr, opts ...dhcpv4.Modifier) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.New(append([]dhcpv4.Modifier{
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRequest),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(testServerIP)),
	}, opts...)...)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}
	return req
}

func TestHandlerDiscoverOffersFromThePool(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req := discover(t, mac)
	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeOffer))
	if stop || resp == nil {
		t.Fatal("discover was dropped")
	}
	if resp.YourIPAddr == nil || resp.YourIPAddr.IsUnspecified() {
		t.Fatal("discover did not offer an address")
	}
	if !p.allocatorHas(resp.YourIPAddr) {
		t.Errorf("offered %v, which is outside the configured range", resp.YourIPAddr)
	}
}

func TestHandlerDiscoverReturnsTheExistingLease(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.3")

	if _, err := p.leaseIP(ctx, mac, ip, time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req := discover(t, mac)
	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeOffer))
	if stop || resp == nil {
		t.Fatal("discover was dropped")
	}
	if !resp.YourIPAddr.Equal(ip) {
		t.Errorf("offered %v to a client that already holds %v", resp.YourIPAddr, ip)
	}
}

func TestHandlerDiscoverOnAnExhaustedPool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.1")
	holder := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	if _, err := p.leaseIP(ctx, holder, net.ParseIP("10.0.0.1"), time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req := discover(t, mustParseMAC(t, "aa:bb:cc:dd:ee:02"))
	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeOffer))
	if resp != nil || !stop {
		t.Error("an exhausted pool should drop the discover rather than offer something")
	}
}

func TestHandlerRequestWithoutServerIdentifierIsNaked(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRequest),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))),
	)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}

	resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if resp == nil || resp.MessageType() != dhcpv4.MessageTypeNak {
		t.Errorf("got %v, want a NAK", resp.MessageType())
	}
}

func TestHandlerRequestForAnotherServerIsIgnored(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRequest),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(net.IP{10, 0, 0, 253})),
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))),
	)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}

	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if resp != nil || !stop {
		t.Error("a request aimed at another server should be ignored")
	}
	if _, ok := getKey(t, p.leasedIPKey("10.0.0.2")); ok {
		t.Error("a request aimed at another server took an address out of the pool")
	}
}

func TestHandlerRequestLeasesTheAddress(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req := request(t, mac, dhcpv4.WithOption(
		dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))))
	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if stop || resp == nil {
		t.Fatal("request was dropped")
	}
	if resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Errorf("got %v, want an ACK", resp.MessageType())
	}
	if !resp.YourIPAddr.Equal(net.ParseIP("10.0.0.2")) {
		t.Errorf("acked %v, want 10.0.0.2", resp.YourIPAddr)
	}
	if got, _ := getKey(t, p.leasedIPKey("10.0.0.2")); got != mac.String() {
		t.Errorf("etcd holds %q for 10.0.0.2, want %q", got, mac)
	}
}

func TestHandlerRequestForSomebodyElsesAddressIsNaked(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	holder := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	other := mustParseMAC(t, "aa:bb:cc:dd:ee:02")

	if _, err := p.leaseIP(ctx, holder, net.ParseIP("10.0.0.2"), time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req := request(t, other, dhcpv4.WithOption(
		dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))))
	resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if resp == nil || resp.MessageType() != dhcpv4.MessageTypeNak {
		t.Fatalf("got %v, want a NAK", resp.MessageType())
	}
	if got, _ := getKey(t, p.leasedIPKey("10.0.0.2")); got != holder.String() {
		t.Error("the NAKed client took the address anyway")
	}
}

// A client can ask for any lease time it likes, including one that outlives the
// pool. Both the reply and what we hold in etcd have to be the clamped value.
func TestHandlerRequestClampsTheLeaseTime(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req := request(t, mac,
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))),
		// the largest option 51 that fits on the wire, about 136 years
		dhcpv4.WithOption(dhcpv4.OptIPAddressLeaseTime(time.Duration(^uint32(0))*time.Second)))

	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if stop || resp == nil {
		t.Fatal("request was dropped")
	}

	if got := resp.IPAddressLeaseTime(0); got != p.config.MaxLeaseTime {
		t.Errorf("reply advertises a lease time of %v, want the clamped %v", got, p.config.MaxLeaseTime)
	}

	leaseID := keyLease(t, p.leasedNicKey(mac.String()))
	if leaseID == 0 {
		t.Fatal("no lease attached to the association")
	}
	ttl, err := etcd.NewLease(testClient).TimeToLive(ctx, leaseID)
	if err != nil {
		t.Fatalf("could not read the lease ttl: %v", err)
	}
	if want := int64(p.config.MaxLeaseTime.Seconds()); ttl.GrantedTTL != want {
		t.Errorf("etcd is holding the address for %ds, want the clamped %ds", ttl.GrantedTTL, want)
	}
}

func TestHandlerRequestRaisesATooShortLeaseTime(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	req := request(t, mac,
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(net.ParseIP("10.0.0.2"))),
		dhcpv4.WithOption(dhcpv4.OptIPAddressLeaseTime(time.Second)))

	resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
	if stop || resp == nil {
		t.Fatal("request was dropped")
	}
	if got := resp.IPAddressLeaseTime(0); got != p.config.MinLeaseTime {
		t.Errorf("reply advertises %v, want the clamped %v", got, p.config.MinLeaseTime)
	}
}

func TestHandlerReleaseReturnsTheAddress(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, mac, ip, time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRelease),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(testServerIP)),
	)
	if err != nil {
		t.Fatalf("could not build release: %v", err)
	}
	req.ClientIPAddr = ip

	p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))

	if _, ok := getKey(t, p.freeKey("10.0.0.2")); !ok {
		t.Error("the released address did not go back into the pool")
	}
}

// Anyone on the segment can forge a MAC. A release that does not name the
// address it is giving up must not drop somebody else's lease. The property is
// defended twice, by the ciaddr check here and by the ownership comparison in
// revokeLease, so this pins the behaviour rather than either guard.
func TestHandlerReleaseWithoutClientAddressIsIgnored(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	if _, err := p.leaseIP(ctx, mac, net.ParseIP("10.0.0.2"), time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRelease),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(testServerIP)),
	)
	if err != nil {
		t.Fatalf("could not build release: %v", err)
	}

	p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))

	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("a release with no client address dropped the lease")
	}
}

func TestHandlerReleaseNamingTheWrongAddressIsRefused(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	victim := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	if _, err := p.leaseIP(ctx, victim, net.ParseIP("10.0.0.2"), time.Minute); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRelease),
		dhcpv4.WithHwAddr(victim),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(testServerIP)),
	)
	if err != nil {
		t.Fatalf("could not build release: %v", err)
	}
	req.ClientIPAddr = net.ParseIP("10.0.0.3")

	p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))

	if got, _ := getKey(t, p.leasedNicKey(victim.String())); got != "10.0.0.2" {
		t.Error("a release naming the wrong address dropped the real lease")
	}
}
