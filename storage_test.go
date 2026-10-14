package etcdplugin

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
)

func TestBootstrapPopulatesFreePool(t *testing.T) {
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")

	for _, ip := range []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"} {
		got, ok := getKey(t, p.freeKey(ip))
		if !ok {
			t.Errorf("%s is not in the free pool after bootstrap", ip)
			continue
		}
		if got != ip {
			t.Errorf("free key for %s holds %q, want %q", ip, got, ip)
		}
	}

	// the range is inclusive at both ends and nothing outside it is touched
	if _, ok := getKey(t, p.freeKey("10.0.0.5")); ok {
		t.Error("bootstrap created a free key outside the configured range")
	}
}

func TestBootstrapDoesNotDisturbLeasedAddresses(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	if _, err := p.leaseIP(ctx, nic, net.ParseIP("10.0.0.1"), time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease 10.0.0.1: %v", err)
	}

	// a restart re-runs bootstrap over the whole range, a leased address must
	// not be put back into the free pool underneath its holder
	if err := p.bootstrapLeasableRange(ctx); err != nil {
		t.Fatalf("second bootstrap failed: %v", err)
	}

	if _, ok := getKey(t, p.freeKey("10.0.0.1")); ok {
		t.Error("bootstrap returned a leased address to the free pool")
	}
	if got, ok := getKey(t, p.leasedIPKey("10.0.0.1")); !ok || got != nic.String() {
		t.Errorf("lease on 10.0.0.1 was disturbed by bootstrap: got %q, %v", got, ok)
	}
}

func TestLeaseIPTakesAddressOutOfFreePool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	leaseID, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	if leaseID == 0 {
		t.Fatal("leaseIP returned a zero lease id")
	}

	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("address is still in the free pool after being leased")
	}
	if got, _ := getKey(t, p.leasedIPKey("10.0.0.2")); got != nic.String() {
		t.Errorf("ip->nic mapping is %q, want %q", got, nic)
	}
	if got, _ := getKey(t, p.leasedNicKey(nic.String())); got != "10.0.0.2" {
		t.Errorf("nic->ip mapping is %q, want 10.0.0.2", got)
	}

	// both halves of the association have to share one lease, everything else
	// in the plugin depends on them expiring together
	if a, b := keyLease(t, p.leasedIPKey("10.0.0.2")), keyLease(t, p.leasedNicKey(nic.String())); a != b {
		t.Errorf("the two halves of the lease are on different etcd leases: %d and %d", a, b)
	}
}

func TestLeaseIPRefusesAddressHeldByAnotherNic(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	first := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	second := mustParseMAC(t, "aa:bb:cc:dd:ee:02")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, first, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease for the first nic: %v", err)
	}

	_, err := p.leaseIP(ctx, second, ip, time.Minute, testServerIP)
	if err == nil {
		t.Fatal("two nics were allowed to hold the same address")
	}
	if !errors.Is(err, ErrAlreadyLeased) {
		t.Errorf("got %v, want ErrAlreadyLeased", err)
	}

	if got, _ := getKey(t, p.leasedIPKey("10.0.0.2")); got != first.String() {
		t.Errorf("the losing nic overwrote the association: %q", got)
	}
}

func TestLeaseIPRenewsExistingAssociation(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	first, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	// renewing goes down the else branch: the address is no longer free, but
	// the association still matches
	second, err := p.leaseIP(ctx, nic, ip, 10*time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not renew: %v", err)
	}
	if second == first {
		t.Error("renewal did not attach the keys to a fresh lease")
	}

	if got := keyLease(t, p.leasedNicKey(nic.String())); got != second {
		t.Errorf("nic key is on lease %d, want the renewed %d", got, second)
	}
	if got := keyLease(t, p.leasedIPKey("10.0.0.2")); got != second {
		t.Errorf("ip key is on lease %d, want the renewed %d", got, second)
	}
}

func TestLeaseIPRefusesAddressOutsideThePool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	// a client is free to ask for anything at all
	_, err := p.leaseIP(ctx, nic, net.ParseIP("8.8.8.8"), time.Minute, testServerIP)
	if !errors.Is(err, ErrAlreadyLeased) {
		t.Errorf("got %v, want ErrAlreadyLeased for an address that was never in the pool", err)
	}
	if _, ok := getKey(t, p.leasedIPKey("8.8.8.8")); ok {
		t.Error("an address outside the pool was leased")
	}
}

// A client that asks for addresses it cannot have used to leak one etcd lease
// per packet, because the lease was granted before the transaction that decides
// whether it gets used.
func TestLeaseIPDoesNotLeakLeasesOnFailure(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	before := countLeases(t)

	for i := 0; i < 20; i++ {
		if _, err := p.leaseIP(ctx, nic, net.ParseIP("8.8.8.8"), 24*time.Hour, testServerIP); err == nil {
			t.Fatal("expected the request to be refused")
		}
	}

	if after := countLeases(t); after != before {
		t.Errorf("20 refused requests leaked %d etcd leases", after-before)
	}
}

func TestFreeIPReportsAnExhaustedPool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.2")

	for i, nic := range []string{"aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"} {
		ip, err := p.freeIP(ctx)
		if err != nil {
			t.Fatalf("could not get a free ip on iteration %d: %v", i, err)
		}
		if _, err := p.leaseIP(ctx, mustParseMAC(t, nic), ip, time.Minute, testServerIP); err != nil {
			t.Fatalf("could not lease %v: %v", ip, err)
		}
	}

	if _, err := p.freeIP(ctx); err == nil {
		t.Error("freeIP handed out an address from an exhausted pool")
	}
}

func TestRevokeLeaseReturnsAddressToPool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	if err := p.revokeLease(ctx, nic, ip); err != nil {
		t.Fatalf("could not revoke: %v", err)
	}

	if got, ok := getKey(t, p.freeKey("10.0.0.2")); !ok || got != "10.0.0.2" {
		t.Error("the address did not go back into the free pool")
	}
	if _, ok := getKey(t, p.leasedIPKey("10.0.0.2")); ok {
		t.Error("the ip->nic mapping survived the revoke")
	}
	if _, ok := getKey(t, p.leasedNicKey(nic.String())); ok {
		t.Error("the nic->ip mapping survived the revoke")
	}
}

// A release names the address it is giving up. Revoking whatever the nic
// happens to hold would let anyone who can forge a MAC drop somebody else's
// lease without even knowing which address it is.
func TestRevokeLeaseRefusesAnAddressTheNicDoesNotHold(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	held := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, held, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	err := p.revokeLease(ctx, nic, net.ParseIP("10.0.0.3"))
	if !errors.Is(err, ErrNotLeased) {
		t.Errorf("got %v, want ErrNotLeased", err)
	}

	if got, _ := getKey(t, p.leasedNicKey(nic.String())); got != "10.0.0.2" {
		t.Error("the real lease was disturbed by a release naming a different address")
	}
}

// The dangerous shape: both keys named in the transaction exist, they just
// belong to different clients. A guard that only checks for their existence
// lets one client destroy another's lease and leaves the two halves of its own
// association pointing at nothing.
func TestRevokeLeaseRefusesAnAddressHeldByAnotherNic(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	attacker := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	victim := mustParseMAC(t, "aa:bb:cc:dd:ee:02")

	if _, err := p.leaseIP(ctx, attacker, net.ParseIP("10.0.0.2"), time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease for the attacker: %v", err)
	}
	if _, err := p.leaseIP(ctx, victim, net.ParseIP("10.0.0.3"), time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease for the victim: %v", err)
	}

	err := p.revokeLease(ctx, attacker, net.ParseIP("10.0.0.3"))
	if !errors.Is(err, ErrNotLeased) {
		t.Errorf("got %v, want ErrNotLeased", err)
	}

	if got, ok := getKey(t, p.leasedIPKey("10.0.0.3")); !ok || got != victim.String() {
		t.Errorf("the victim's lease was destroyed: 10.0.0.3 now maps to %q (present: %v)", got, ok)
	}
	if got, _ := getKey(t, p.leasedNicKey(victim.String())); got != "10.0.0.3" {
		t.Errorf("the victim's nic mapping was destroyed: %q", got)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.3")); ok {
		t.Error("the victim's address was put back into the free pool")
	}
	if got, _ := getKey(t, p.leasedNicKey(attacker.String())); got != "10.0.0.2" {
		t.Errorf("the caller's own association was corrupted: %q", got)
	}
}

func TestRevokeLeaseRefusesANicWithNoLease(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	stranger := mustParseMAC(t, "aa:bb:cc:dd:ee:09")

	err := p.revokeLease(ctx, stranger, net.ParseIP("10.0.0.2"))
	if !errors.Is(err, ErrNotLeased) {
		t.Errorf("got %v, want ErrNotLeased", err)
	}

	// 10.0.0.2 was never leased, so it must still be exactly where it was and
	// must not have been put into the pool a second time
	if n := countKeys(t, p.freeKey("10.0.0.2")); n != 1 {
		t.Errorf("free pool holds %d copies of 10.0.0.2, want 1", n)
	}
}

// The DNS records hang off the same etcd lease as the address, so that a
// release cannot leave a name pointing at an address that is back in the pool.
func TestRevokeLeaseTearsDownDNSRecords(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	leaseID, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	if err := p.dns.Register(ctx, testClient, "printer", ip, nic, leaseID); err != nil {
		t.Fatalf("could not register dns: %v", err)
	}

	nameKey := p.config.DNSPrefix + constDefaultSeparator + "example.test" +
		constDefaultSeparator + "printer" + constDefaultSeparator + "A"
	if got, ok := getKey(t, nameKey); !ok || got != "10.0.0.2" {
		t.Fatalf("dns record was not registered: got %q, %v", got, ok)
	}

	if err := p.revokeLease(ctx, nic, ip); err != nil {
		t.Fatalf("could not revoke: %v", err)
	}

	if got, ok := getKey(t, nameKey); ok {
		t.Errorf("dns record survived the release and still resolves to %q", got)
	}
}

func TestLeaseIPRecordsTheGrant(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")

	before := time.Now()
	leaseID, err := p.leaseIP(ctx, nic, net.ParseIP("10.0.0.2"), time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	g := p.readGrant(t, nic.String())
	if g.Server != testServerIP.String() {
		t.Errorf("grant names server %q, want %q", g.Server, testServerIP)
	}
	if g.Time.Before(before.Add(-time.Second)) || g.Time.After(time.Now().Add(time.Second)) {
		t.Errorf("grant is timestamped %v, not around now", g.Time)
	}

	// on the binding's own lease, so that it describes this instance of the
	// binding and goes away with it
	if got := keyLease(t, p.grantedNicKey(nic.String())); got != leaseID {
		t.Errorf("grant is on lease %d, want the binding's %d", got, leaseID)
	}
}

func TestLeaseIPRenewalRewritesTheGrant(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")
	other := net.IP{10, 0, 0, 253}

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	renewed, err := p.leaseIP(ctx, nic, ip, time.Minute, other)
	if err != nil {
		t.Fatalf("could not renew: %v", err)
	}

	if g := p.readGrant(t, nic.String()); g.Server != other.String() {
		t.Errorf("grant still names %q after %v renewed the binding", g.Server, other)
	}
	if got := keyLease(t, p.grantedNicKey(nic.String())); got != renewed {
		t.Errorf("grant is on lease %d, want the renewed %d", got, renewed)
	}
}

func TestReleaseLeaseReturnsAddressToPool(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	p.backdateGrant(t, nic.String(), time.Hour)

	if err := p.releaseLease(ctx, nic, ip, testServerIP); err != nil {
		t.Fatalf("could not release: %v", err)
	}

	if got, ok := getKey(t, p.freeKey("10.0.0.2")); !ok || got != "10.0.0.2" {
		t.Error("the address did not go back into the free pool")
	}
	if _, ok := getKey(t, p.leasedNicKey(nic.String())); ok {
		t.Error("the nic->ip mapping survived the release")
	}
	if _, ok := getKey(t, p.grantedNicKey(nic.String())); ok {
		t.Error("the grant survived the binding it describes")
	}
}

// The incident this exists for, 2026-10-13: node3 granted a Shelly .106 at
// 06:01:44, the Shelly reconnected and node2 granted it .106 again at
// 06:33:17.228, and 49ms later the old session's RELEASE of .106 reached node3.
// node3 put .106 back into the free pool and a phone was handed it at 07:26:57
// while the Shelly was still using it.
func TestReleaseLeaseRefusesAReleaseAddressedToAnotherServer(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	shelly := mustParseMAC(t, "dc:b4:d9:c8:90:dc")
	ip := net.ParseIP("10.0.0.2")
	node2, node3 := net.IP{10, 0, 0, 252}, net.IP{10, 0, 0, 253}

	if _, err := p.leaseIP(ctx, shelly, ip, time.Minute, node3); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	leaseID, err := p.leaseIP(ctx, shelly, ip, time.Minute, node2)
	if err != nil {
		t.Fatalf("could not renew: %v", err)
	}
	if err := p.dns.Register(ctx, testClient, "shelly", ip, shelly, leaseID); err != nil {
		t.Fatalf("could not register dns: %v", err)
	}
	// the server is enough on its own, however late the release turns up
	p.backdateGrant(t, shelly.String(), time.Hour)

	err = p.releaseLease(ctx, shelly, ip, node3)
	if !errors.Is(err, ErrStaleRelease) {
		t.Fatalf("got %v, want ErrStaleRelease", err)
	}

	if got, _ := getKey(t, p.leasedNicKey(shelly.String())); got != "10.0.0.2" {
		t.Errorf("the binding node2 granted was destroyed: nic maps to %q", got)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("an address still in use went back into the free pool")
	}
	nameKey := p.config.DNSPrefix + constDefaultSeparator + "example.test" +
		constDefaultSeparator + "shelly" + constDefaultSeparator + "A"
	if _, ok := getKey(t, nameKey); !ok {
		t.Error("the refused release still tore down the dns record")
	}
}

// The same thing when both sessions happened to go to the same server, as at
// 2026-10-13 05:18:21: node4 acked .126 and the RELEASE of the previous lease,
// also node4's, followed 364ms later. Only the timing tells them apart.
func TestReleaseLeaseRefusesAReleaseRightAfterTheGrant(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	shelly := mustParseMAC(t, "dc:b4:d9:c8:90:dc")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, shelly, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	err := p.releaseLease(ctx, shelly, ip, testServerIP)
	if !errors.Is(err, ErrStaleRelease) {
		t.Fatalf("got %v, want ErrStaleRelease", err)
	}

	if got, _ := getKey(t, p.leasedNicKey(shelly.String())); got != "10.0.0.2" {
		t.Errorf("the binding was destroyed: nic maps to %q", got)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("an address still in use went back into the free pool")
	}
}

// Bindings granted before grants existed, or renewed by a node still running a
// version that does not write them, have nothing to check against. Those are
// released exactly as they were before, rather than held on a guess.
func TestReleaseLeaseWithoutAGrantIsReleasedAsBefore(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	if _, err := etcd.NewKV(testClient).Delete(ctx, p.grantedNicKey(nic.String())); err != nil {
		t.Fatalf("could not drop the grant: %v", err)
	}

	if err := p.releaseLease(ctx, nic, ip, net.IP{10, 0, 0, 253}); err != nil {
		t.Fatalf("a binding with no grant was not released: %v", err)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); !ok {
		t.Error("the address did not go back into the free pool")
	}
}

// What a renewal by an older version leaves behind: the binding moved onto a
// fresh lease, and the grant still sitting on the lease it came from. That
// grant is about a previous instance of the binding and must not be taken as
// evidence about the current one.
func TestReleaseLeaseIgnoresAGrantFromAnEarlierLease(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	lease, err := etcd.NewLease(testClient).Grant(ctx, 60)
	if err != nil {
		t.Fatalf("could not grant: %v", err)
	}
	kvc := etcd.NewKV(testClient)
	for key, value := range map[string]string{
		p.leasedNicKey(nic.String()): "10.0.0.2",
		p.leasedIPKey("10.0.0.2"):    nic.String(),
	} {
		if _, err := kvc.Put(ctx, key, value, etcd.WithLease(lease.ID)); err != nil {
			t.Fatalf("could not renew the old way: %v", err)
		}
	}

	if err := p.releaseLease(ctx, nic, ip, net.IP{10, 0, 0, 253}); err != nil {
		t.Fatalf("a grant from an earlier lease blocked the release: %v", err)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); !ok {
		t.Error("the address did not go back into the free pool")
	}
}

// A DECLINE is the client's verdict on the lease it has just been granted, so
// it is expected right after the grant and must not be held to the release
// rules.
func TestRevokeLeaseIgnoresTheGrant(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	if err := p.revokeLease(ctx, nic, ip); err != nil {
		t.Fatalf("could not revoke straight after the grant: %v", err)
	}
	if _, ok := getKey(t, p.leasedNicKey(nic.String())); ok {
		t.Error("the binding survived the revoke")
	}
	if _, ok := getKey(t, p.grantedNicKey(nic.String())); ok {
		t.Error("the grant survived the binding it describes")
	}
}

// The check and the teardown are separate round trips. A renewal that lands
// between them has produced a binding the check never saw, and the teardown
// must leave it alone.
func TestUnbindDoesNotUndoARenewalInBetween(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	b, err := p.readBinding(ctx, nic, ip)
	if err != nil {
		t.Fatalf("could not read the binding: %v", err)
	}

	renewed, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not renew: %v", err)
	}

	if err := p.unbind(ctx, b); !errors.Is(err, ErrNotLeased) {
		t.Errorf("got %v, want ErrNotLeased", err)
	}
	if got := keyLease(t, p.leasedNicKey(nic.String())); got != renewed {
		t.Errorf("the renewed binding was disturbed: nic key is on lease %d, want %d", got, renewed)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("the renewed address went back into the free pool")
	}
}

// An expired lease leaves the address in neither the free nor the leased set.
// Revoking the etcd lease deletes exactly the keys expiry would have deleted,
// which is what lets this run without waiting out a real TTL.
func TestResurrectLeasesReclaimsExpiredAddresses(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	leaseID, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP)
	if err != nil {
		t.Fatalf("could not lease: %v", err)
	}
	if _, err := etcd.NewLease(testClient).Revoke(ctx, leaseID); err != nil {
		t.Fatalf("could not expire the lease: %v", err)
	}

	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Fatal("expiry put the address back into the pool on its own, test is not exercising resurrect")
	}

	if err := p.resurrectLeases(ctx); err != nil {
		t.Fatalf("could not resurrect: %v", err)
	}

	if got, ok := getKey(t, p.freeKey("10.0.0.2")); !ok || got != "10.0.0.2" {
		t.Error("the expired address was not returned to the free pool")
	}
}

func TestResurrectLeasesLeavesLiveLeasesAlone(t *testing.T) {
	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Minute, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	if err := p.resurrectLeases(ctx); err != nil {
		t.Fatalf("could not resurrect: %v", err)
	}

	if _, ok := getKey(t, p.freeKey("10.0.0.2")); ok {
		t.Error("a live lease was returned to the free pool")
	}
	if got, _ := getKey(t, p.leasedIPKey("10.0.0.2")); got != nic.String() {
		t.Error("a live lease was disturbed")
	}
}

// The same thing again, but waiting out a real TTL rather than simulating it,
// to confirm that expiry really does remove both halves together.
func TestLeaseExpiryRemovesBothHalvesTogether(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a real etcd lease TTL")
	}

	ctx := context.Background()
	p := bootstrapped(t, "10.0.0.1", "10.0.0.4")
	nic := mustParseMAC(t, "aa:bb:cc:dd:ee:01")
	ip := net.ParseIP("10.0.0.2")

	if _, err := p.leaseIP(ctx, nic, ip, time.Second, testServerIP); err != nil {
		t.Fatalf("could not lease: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		_, ipOK := getKey(t, p.leasedIPKey("10.0.0.2"))
		_, nicOK := getKey(t, p.leasedNicKey(nic.String()))

		if !ipOK && !nicOK {
			break
		}
		if ipOK != nicOK {
			t.Fatalf("the two halves expired separately: ip key %v, nic key %v", ipOK, nicOK)
		}
		if time.Now().After(deadline) {
			t.Fatal("lease did not expire within 20s")
		}
		time.Sleep(200 * time.Millisecond)
	}

	if err := p.resurrectLeases(ctx); err != nil {
		t.Fatalf("could not resurrect: %v", err)
	}
	if _, ok := getKey(t, p.freeKey("10.0.0.2")); !ok {
		t.Error("the expired address was not returned to the free pool")
	}
}
