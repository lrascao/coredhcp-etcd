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

// replyAs is reply from a server other than testServerIP, for tests that need
// several servers sharing one set of bindings the way node1-4 do.
func replyAs(t *testing.T, req *dhcpv4.DHCPv4, mt dhcpv4.MessageType, server net.IP) *dhcpv4.DHCPv4 {
	t.Helper()

	resp := reply(t, req, mt)
	resp.ServerIPAddr = server
	return resp
}

// requestTo is a SELECTING state REQUEST for ip, addressed to server.
func requestTo(t *testing.T, mac net.HardwareAddr, server, ip net.IP) *dhcpv4.DHCPv4 {
	t.Helper()

	return request(t, mac,
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(server)),
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(ip)))
}

// releaseTo is a RELEASE of ip, addressed to server.
func releaseTo(t *testing.T, mac net.HardwareAddr, server, ip net.IP) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRelease),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(server)),
	)
	if err != nil {
		t.Fatalf("could not build release: %v", err)
	}
	req.ClientIPAddr = ip
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

	if _, err := p.leaseIP(ctx, mac, ip, time.Minute, testServerIP); err != nil {
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

	if _, err := p.leaseIP(ctx, holder, net.ParseIP("10.0.0.1"), time.Minute, testServerIP); err != nil {
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

	if _, err := p.leaseIP(ctx, holder, net.ParseIP("10.0.0.2"), time.Minute, testServerIP); err != nil {
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

	if _, err := p.leaseIP(ctx, mac, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	// a client giving its address up, rather than a leftover from a session
	// that has since been replaced, does it well after the grant
	p.backdateGrant(t, mac.String(), time.Hour)

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

	if _, err := p.leaseIP(ctx, mac, net.ParseIP("10.0.0.2"), time.Minute, testServerIP); err != nil {
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

	if _, err := p.leaseIP(ctx, victim, net.ParseIP("10.0.0.2"), time.Minute, testServerIP); err != nil {
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

// declineTo is a DECLINE of ip, addressed to server. RFC 2131 has the declined
// address in the requested IP option, ciaddr is zero.
func declineTo(t *testing.T, mac net.HardwareAddr, server, ip net.IP) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.New(
		dhcpv4.WithMessageType(dhcpv4.MessageTypeDecline),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptServerIdentifier(server)),
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(ip)),
	)
	if err != nil {
		t.Fatalf("could not build decline: %v", err)
	}
	return req
}

// The 2026-10-10 loop through the handler: 5c:9b:a6:65:15:26 was granted
// 192.168.3.106, found a Shelly still answering ARP for it and declined it, and
// its next DISCOVER was offered .106 again, 47 times in two hours. However many
// addresses a client turns down, it must never be offered one of them back.
func TestHandlerDeclinedAddressIsNotOfferedAgain(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	client := mustParseMAC(t, "5c:9b:a6:65:15:26")

	declined := map[string]bool{}
	for i := 0; i < 3; i++ {
		d := discover(t, client)
		offer, _ := p.Handler4(d, reply(t, d, dhcpv4.MessageTypeOffer))
		if offer == nil {
			t.Fatalf("discover %d was dropped", i)
		}
		ip := offer.YourIPAddr
		if declined[ip.String()] {
			t.Fatalf("discover %d was offered %v, which the client has already declined", i, ip)
		}

		req := requestTo(t, client, testServerIP, ip)
		if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
			resp.MessageType() != dhcpv4.MessageTypeAck {
			t.Fatalf("request %d for %v was not acked", i, ip)
		}

		req = declineTo(t, client, testServerIP, ip)
		p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))
		declined[ip.String()] = true

		if got, ok := getKey(t, p.declinedKey(ip.String())); !ok || got != client.String() {
			t.Errorf("%v was not quarantined by the decline: got %q, %v", ip, got, ok)
		}
		if _, ok := getKey(t, p.leasedNicKey(client.String())); ok {
			t.Errorf("the binding on %v survived the decline", ip)
		}
	}

	// nor is anybody else
	d := discover(t, mustParseMAC(t, "aa:bb:cc:dd:ee:02"))
	resp, _ := p.Handler4(d, reply(t, d, dhcpv4.MessageTypeOffer))
	if resp == nil || declined[resp.YourIPAddr.String()] {
		t.Errorf("another client was offered %v, want the one address nobody declined", resp.YourIPAddr)
	}
}

// Asking for a quarantined address outright, as the declining client might in
// INIT-REBOOT or the host squatting on it might when it renews, does not get
// around the quarantine.
func TestHandlerRequestForAQuarantinedAddressIsNaked(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	client := mustParseMAC(t, "5c:9b:a6:65:15:26")
	squatter := mustParseMAC(t, "dc:b4:d9:c8:90:dc")
	ip := net.ParseIP("10.0.0.1")

	req := requestTo(t, client, testServerIP, ip)
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("the request was not acked")
	}
	req = declineTo(t, client, testServerIP, ip)
	p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))

	for _, mac := range []net.HardwareAddr{client, squatter} {
		req := requestTo(t, mac, testServerIP, ip)
		resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))
		if resp == nil || resp.MessageType() != dhcpv4.MessageTypeNak {
			t.Errorf("%v asking for the quarantined %v got %v, want a NAK", mac, ip, resp.MessageType())
		}
		if _, ok := getKey(t, p.leasedNicKey(mac.String())); ok {
			t.Errorf("%v was bound to the quarantined %v", mac, ip)
		}
	}
}

// The whole 2026-10-13 sequence through the handler, with two servers sharing
// one set of bindings: node3 grants the Shelly an address, the Shelly
// reconnects and node2 grants it the same address, and the old session's
// RELEASE reaches node3. The Shelly keeps using the address either way, so
// what matters is that nobody else is offered it.
func TestHandlerReleaseFromAReplacedSessionKeepsTheAddress(t *testing.T) {
	node3, node3IP := bootstrapped(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 253}
	node2, node2IP := newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 252}
	shelly := mustParseMAC(t, "dc:b4:d9:c8:90:dc")
	phone := mustParseMAC(t, "8e:f8:fe:1a:43:de")
	ip := net.ParseIP("10.0.0.1")

	req := requestTo(t, shelly, node3IP, ip)
	if resp, _ := node3.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node3IP)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("node3 did not ack the first lease")
	}
	node3.backdateGrant(t, shelly.String(), 30*time.Minute)

	req = requestTo(t, shelly, node2IP, ip)
	if resp, _ := node2.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node2IP)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("node2 did not ack the second lease")
	}

	req = releaseTo(t, shelly, node3IP, ip)
	node3.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeNone, node3IP))

	if got, _ := getKey(t, node2.leasedNicKey(shelly.String())); got != "10.0.0.1" {
		t.Errorf("the release destroyed node2's binding: nic maps to %q", got)
	}

	for _, n := range []struct {
		p  *PluginState
		ip net.IP
	}{{node2, node2IP}, {node3, node3IP}} {
		d := discover(t, phone)
		resp, _ := n.p.Handler4(d, replyAs(t, d, dhcpv4.MessageTypeOffer, n.ip))
		if resp != nil && resp.YourIPAddr.Equal(ip) {
			t.Errorf("%v offered the phone %v, which the Shelly is still using", n.ip, ip)
		}
	}
}

// The same server variant, 2026-10-13 05:18:21: ACK, then 364ms later a
// RELEASE of the previous lease, which that same server had also granted.
func TestHandlerReleaseRightAfterTheAckKeepsTheAddress(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	shelly := mustParseMAC(t, "dc:b4:d9:c8:90:dc")
	ip := net.ParseIP("10.0.0.1")

	req := requestTo(t, shelly, testServerIP, ip)
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("the request was not acked")
	}

	req = releaseTo(t, shelly, testServerIP, ip)
	p.Handler4(req, reply(t, req, dhcpv4.MessageTypeNone))

	if got, _ := getKey(t, p.leasedNicKey(shelly.String())); got != "10.0.0.1" {
		t.Errorf("the release destroyed the binding: nic maps to %q", got)
	}

	// and the Shelly's next DISCOVER finds it, rather than walking to a new
	// address the way it did .106 -> .107 -> .116 -> .129 -> .126 -> .106
	d := discover(t, shelly)
	resp, _ := p.Handler4(d, reply(t, d, dhcpv4.MessageTypeOffer))
	if resp == nil || !resp.YourIPAddr.Equal(ip) {
		t.Errorf("the Shelly's next discover was offered %v, want its own %v", resp.YourIPAddr, ip)
	}
}
