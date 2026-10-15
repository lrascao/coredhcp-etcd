package etcdplugin

import (
	"context"
	"net"
	"sync"
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

// renewing is a RENEWING or REBINDING state REQUEST for ip, which the client
// names in ciaddr. The two only differ in whether the packet is unicast to the
// server that granted ip or broadcast to all of them.
func renewing(t *testing.T, mac net.HardwareAddr, ip net.IP, opts ...dhcpv4.Modifier) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.New(append([]dhcpv4.Modifier{
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRequest),
		dhcpv4.WithHwAddr(mac),
	}, opts...)...)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}
	req.ClientIPAddr = ip
	return req
}

// initReboot is an INIT-REBOOT state REQUEST for ip, the address the client
// remembers from before and names in option 50, broadcast.
func initReboot(t *testing.T, mac net.HardwareAddr, ip net.IP, opts ...dhcpv4.Modifier) *dhcpv4.DHCPv4 {
	t.Helper()

	req, err := dhcpv4.New(append([]dhcpv4.Modifier{
		dhcpv4.WithMessageType(dhcpv4.MessageTypeRequest),
		dhcpv4.WithHwAddr(mac),
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(ip)),
	}, opts...)...)
	if err != nil {
		t.Fatalf("could not build request: %v", err)
	}
	return req
}

// answer describes what Handler4 sent back, for test failures.
func answer(resp *dhcpv4.DHCPv4) string {
	if resp == nil {
		return "no answer"
	}
	return resp.MessageType().String()
}

// RFC 2131 §4.3.2 has a client leave the server identifier out of every
// REQUEST that is not choosing between offers: INIT-REBOOT, RENEWING and
// REBINDING. These used to be NAKed across the board. node1-4 did that 2335
// times between 2026-10-09 and 10-13, to every renewal of 35 clients, and each
// client dropped its address at T1 and started over with a DISCOVER.
func TestHandlerRequestWithoutServerIdentifier(t *testing.T) {
	const (
		client = "aa:bb:cc:dd:ee:01"
		other  = "aa:bb:cc:dd:ee:02"
		held   = "10.0.0.2"
		theirs = "10.0.0.3"
		free   = "10.0.0.4"
	)

	for _, tc := range []struct {
		name     string
		bindings map[string]string // nic -> ip, set up beforehand
		build    func(*testing.T, net.HardwareAddr, net.IP, ...dhcpv4.Modifier) *dhcpv4.DHCPv4
		ip       string
		want     dhcpv4.MessageType // MessageTypeNone for no answer at all
	}{
		{"renewing the address it holds", map[string]string{client: held},
			renewing, held, dhcpv4.MessageTypeAck},
		{"init-reboot into the address it holds", map[string]string{client: held},
			initReboot, held, dhcpv4.MessageTypeAck},

		{"renewing a free address it does not hold", map[string]string{client: held},
			renewing, free, dhcpv4.MessageTypeNak},
		{"renewing somebody else's address", map[string]string{client: held, other: theirs},
			renewing, theirs, dhcpv4.MessageTypeNak},
		{"renewing without a binding", nil,
			renewing, free, dhcpv4.MessageTypeNak},
		{"init-reboot into a free address it does not hold", map[string]string{client: held},
			initReboot, free, dhcpv4.MessageTypeNak},
		{"init-reboot into somebody else's address", map[string]string{client: held, other: theirs},
			initReboot, theirs, dhcpv4.MessageTypeNak},
		{"naming no address at all", map[string]string{client: held},
			renewing, "0.0.0.0", dhcpv4.MessageTypeNak},

		// the RFC's MUST remain silent
		{"init-reboot without a binding", nil,
			initReboot, free, dhcpv4.MessageTypeNone},
		{"init-reboot into somebody else's address without a binding", map[string]string{other: theirs},
			initReboot, theirs, dhcpv4.MessageTypeNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
			mac := mustParseMAC(t, client)

			leases := map[string]etcd.LeaseID{}
			for nic, ip := range tc.bindings {
				id, err := p.leaseIP(ctx, mustParseMAC(t, nic), net.ParseIP(ip), time.Minute, testServerIP)
				if err != nil {
					t.Fatalf("could not lease %s to %s: %v", ip, nic, err)
				}
				leases[nic] = id
			}
			_, wasFree := getKey(t, p.freeKey(tc.ip))
			before := countLeases(t)

			req := tc.build(t, mac, net.ParseIP(tc.ip))
			resp, stop := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck))

			if tc.want == dhcpv4.MessageTypeNone {
				if resp != nil || !stop {
					t.Fatalf("got %s, want no answer at all", answer(resp))
				}
			} else if resp == nil || resp.MessageType() != tc.want {
				t.Fatalf("got %s, want %v", answer(resp), tc.want)
			}

			if tc.want == dhcpv4.MessageTypeAck {
				if !resp.YourIPAddr.Equal(net.ParseIP(tc.ip)) {
					t.Errorf("acked %v, want %s", resp.YourIPAddr, tc.ip)
				}
				if got, _ := getKey(t, p.leasedIPKey(tc.ip)); got != client {
					t.Errorf("etcd holds %q for %s, want %q", got, tc.ip, client)
				}

				// renewed: on a fresh etcd lease of the lease time the reply
				// advertises, granted by the server that sent it
				leaseID := keyLease(t, p.leasedNicKey(client))
				if leaseID == leases[client] {
					t.Error("the binding is still on the lease it had before, it was not renewed")
				}
				ttl, err := etcd.NewLease(testClient).TimeToLive(ctx, leaseID)
				if err != nil {
					t.Fatalf("could not read the lease ttl: %v", err)
				}
				if want := int64(resp.IPAddressLeaseTime(constDefaultLeaseTime).Seconds()); ttl.GrantedTTL != want {
					t.Errorf("etcd is holding the address for %ds, the reply says %ds", ttl.GrantedTTL, want)
				}
				if g := p.readGrant(t, client); g.Server != testServerIP.String() {
					t.Errorf("the renewal is recorded as granted by %s, want %s", g.Server, testServerIP)
				}
				return
			}

			// anything but an ACK leaves every binding, the pool and the etcd
			// leases as they were
			for nic, ip := range tc.bindings {
				if got, _ := getKey(t, p.leasedNicKey(nic)); got != ip {
					t.Errorf("%s now holds %q, want %s", nic, got, ip)
				}
				if keyLease(t, p.leasedNicKey(nic)) != leases[nic] {
					t.Errorf("the binding of %s was renewed", nic)
				}
			}
			if _, ok := tc.bindings[client]; !ok {
				if got, ok := getKey(t, p.leasedNicKey(client)); ok {
					t.Errorf("the client was bound to %s", got)
				}
			}
			if _, isFree := getKey(t, p.freeKey(tc.ip)); isFree != wasFree {
				t.Errorf("%s went from free=%v to free=%v", tc.ip, wasFree, isFree)
			}
			if after := countLeases(t); after != before {
				t.Errorf("the request leaked %d etcd leases", after-before)
			}
		})
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

// REBINDING is broadcast, so every server gets a copy, and since they share
// the binding every one of them could answer it. Here node3 granted the
// address and has been gone since, so the client's renewals went unanswered
// and at T2 it rebinds, heard by node1 and node2. Only one of them answers,
// and the grant moves to that one, which is the server in the ACK the client
// takes and so the one its next renewal and its RELEASE are addressed to.
func TestHandlerRebindingIsAnsweredByOneServer(t *testing.T) {
	node1, node1IP := bootstrapped(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 251}
	node2, node2IP := newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 252}
	node3, node3IP := newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 253}
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.1")

	req := requestTo(t, mac, node3IP, ip)
	if resp, _ := node3.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node3IP)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("node3 did not ack the lease")
	}
	// T2 of an 8h lease
	node3.backdateGrant(t, mac.String(), 7*time.Hour)

	req = renewing(t, mac, ip)
	if resp, _ := node2.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node2IP)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatalf("node2 gave the rebinding client %s, want an ACK", answer(resp))
	}
	if resp, stop := node1.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node1IP)); resp != nil || !stop {
		t.Errorf("node1 gave the client %s for the rebinding node2 had already acked", answer(resp))
	}

	if g := node1.readGrant(t, mac.String()); g.Server != node2IP.String() {
		t.Errorf("the binding is recorded as granted by %s, want node2 %s", g.Server, node2IP)
	}

	// node2's ACK can still go missing, and the client's retransmission is
	// node2's to answer again
	req = renewing(t, mac, ip)
	if resp, _ := node2.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node2IP)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Errorf("node2 gave the retransmission %s, want an ACK", answer(resp))
	}
	if resp, stop := node1.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, node1IP)); resp != nil || !stop {
		t.Errorf("node1 gave the retransmission %s, want no answer", answer(resp))
	}
}

// One broadcast reaching four servers at once, the way an INIT-REBOOT reaches
// node1-4. However their reads and renewals interleave, exactly one of them
// answers, and the grant names that one.
func TestHandlerBroadcastRaceIsAnsweredOnce(t *testing.T) {
	type node struct {
		p  *PluginState
		ip net.IP
	}
	nodes := []node{
		{bootstrapped(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 251}},
		{newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 252}},
		{newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 253}},
		{newTestPlugin(t, "10.0.0.1", "10.0.0.4"), net.IP{10, 0, 0, 254}},
	}
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.1")

	req := requestTo(t, mac, nodes[0].ip, ip)
	if resp, _ := nodes[0].p.Handler4(req, replyAs(t, req, dhcpv4.MessageTypeAck, nodes[0].ip)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("the lease was not acked")
	}

	for round := 0; round < 20; round++ {
		// a reboot, long after whichever server granted the binding last
		nodes[0].p.backdateGrant(t, mac.String(), time.Hour)

		req := initReboot(t, mac, ip)
		replies := make([]*dhcpv4.DHCPv4, len(nodes))
		for i, n := range nodes {
			replies[i] = replyAs(t, req, dhcpv4.MessageTypeAck, n.ip)
		}

		var wg sync.WaitGroup
		for i, n := range nodes {
			wg.Add(1)
			go func() {
				defer wg.Done()
				replies[i], _ = n.p.Handler4(req, replies[i])
			}()
		}
		wg.Wait()

		var answered []net.IP
		for i, resp := range replies {
			if resp == nil {
				continue
			}
			if resp.MessageType() != dhcpv4.MessageTypeAck {
				t.Errorf("round %d: %v gave %s, want an ACK or no answer", round, nodes[i].ip, answer(resp))
			}
			answered = append(answered, nodes[i].ip)
		}
		if len(answered) != 1 {
			t.Fatalf("round %d: %d servers answered %v, want exactly one", round, len(answered), answered)
		}
		if g := nodes[0].p.readGrant(t, mac.String()); g.Server != answered[0].String() {
			t.Errorf("round %d: %v answered, but the grant names %s", round, answered[0], g.Server)
		}
	}
}

// Register puts a client's DNS records on its binding's lease, but only when
// the client sends a hostname. A renewal moves the binding onto a fresh lease,
// so without carrying the records over, a client that renews without naming
// itself would lose its name when the old lease ran out while it kept the
// address.
func TestHandlerRenewalWithoutAHostnameKeepsTheName(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")
	nameKey := p.config.DNSPrefix + constDefaultSeparator + "example.test" +
		constDefaultSeparator + "printer" + constDefaultSeparator + "A"

	req := request(t, mac,
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(ip)),
		dhcpv4.WithOption(dhcpv4.OptHostName("printer")))
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("the lease was not acked")
	}

	req = renewing(t, mac, ip)
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatalf("got %s for the renewal, want an ACK", answer(resp))
	}

	if got, _ := getKey(t, nameKey); got != "10.0.0.2" {
		t.Errorf("the name resolves to %q, want 10.0.0.2", got)
	}
	if keyLease(t, nameKey) != keyLease(t, p.leasedNicKey(mac.String())) {
		t.Error("the name was left behind on the lease the binding was renewed from")
	}
}

// A client that renews under a new name gets the new name on the renewed
// binding, and the old one is left to run out with the lease it was on rather
// than being kept alive alongside it.
func TestHandlerRenewalUnderANewNameLetsTheOldOneLapse(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	mac := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")
	nameKey := func(name string) string {
		return p.config.DNSPrefix + constDefaultSeparator + "example.test" +
			constDefaultSeparator + name + constDefaultSeparator + "A"
	}

	req := request(t, mac,
		dhcpv4.WithOption(dhcpv4.OptRequestedIPAddress(ip)),
		dhcpv4.WithOption(dhcpv4.OptHostName("printer")))
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatal("the lease was not acked")
	}
	previous := keyLease(t, p.leasedNicKey(mac.String()))

	req = renewing(t, mac, ip, dhcpv4.WithOption(dhcpv4.OptHostName("scanner")))
	if resp, _ := p.Handler4(req, reply(t, req, dhcpv4.MessageTypeAck)); resp == nil ||
		resp.MessageType() != dhcpv4.MessageTypeAck {
		t.Fatalf("got %s for the renewal, want an ACK", answer(resp))
	}

	if keyLease(t, nameKey("scanner")) != keyLease(t, p.leasedNicKey(mac.String())) {
		t.Error("the new name is not on the renewed binding's lease")
	}
	if keyLease(t, nameKey("printer")) != previous {
		t.Error("the old name was carried over to the renewed binding")
	}
}
