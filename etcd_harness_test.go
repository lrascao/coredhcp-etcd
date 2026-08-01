package etcdplugin

import (
	"context"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"testing"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/coredhcp/coredhcp/plugins/allocators/bitmap"
)

// The allocation and deallocation logic is expressed almost entirely in etcd
// transactions: KeyMissing guards, value comparisons, nested transaction
// results and lease attachment. A hand written fake would not reproduce any of
// that, so these tests run against a real etcd started in process once for the
// whole package. Each test namespaces itself with its own key prefix.
var testClient *etcd.Client

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "coredhcp-etcd-test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	cfg := embed.NewConfig()
	cfg.Dir = dir
	cfg.LogLevel = "error"

	// bind to fixed loopback ports away from the etcd defaults
	clientURL := cfg.LCUrls[0]
	clientURL.Host = "127.0.0.1:22379"
	cfg.LCUrls = []neturl.URL{clientURL}
	cfg.ACUrls = []neturl.URL{clientURL}
	peerURL := cfg.LPUrls[0]
	peerURL.Host = "127.0.0.1:22380"
	cfg.LPUrls = []neturl.URL{peerURL}
	cfg.APUrls = []neturl.URL{peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not start embedded etcd: %v\n", err)
		os.Exit(1)
	}

	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(30 * time.Second):
		server.Close()
		fmt.Fprintln(os.Stderr, "embedded etcd took too long to start")
		os.Exit(1)
	}

	testClient, err = etcd.New(etcd.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		server.Close()
		fmt.Fprintf(os.Stderr, "could not connect to embedded etcd: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()

	testClient.Close()
	server.Close()
	os.RemoveAll(dir)
	os.Exit(code)
}

// newTestPlugin builds a PluginState over the inclusive range [start, end],
// keyed under a prefix unique to the calling test.
func newTestPlugin(t *testing.T, start, end string) *PluginState {
	t.Helper()

	allocator, err := bitmap.NewIPv4Allocator(net.ParseIP(start), net.ParseIP(end))
	if err != nil {
		t.Fatalf("could not create allocator for [%s,%s]: %v", start, end, err)
	}

	return &PluginState{
		config: Config{
			Prefix:       t.Name(),
			Separator:    constDefaultSeparator,
			MinLeaseTime: constDefaultMinLeaseTime,
			MaxLeaseTime: constDefaultMaxLeaseTime,
			DNSPrefix:    t.Name() + "-dns",
			DNSZone:      "example.test",
		},
		client:    testClient,
		allocator: allocator,
		dns: &DNS{
			prefix:    t.Name() + "-dns",
			zone:      "example.test",
			separator: constDefaultSeparator,
		},
	}
}

// bootstrapped returns a plugin whose whole range is already in the free pool.
func bootstrapped(t *testing.T, start, end string) *PluginState {
	t.Helper()

	p := newTestPlugin(t, start, end)
	if err := p.bootstrapLeasableRange(context.Background()); err != nil {
		t.Fatalf("could not bootstrap leasable range: %v", err)
	}
	return p
}

func (p *PluginState) freeKey(ip string) string {
	return p.config.Prefix + p.config.Separator + "ips" + p.config.Separator +
		"free" + p.config.Separator + ip
}

func (p *PluginState) leasedIPKey(ip string) string {
	return p.config.Prefix + p.config.Separator + "ips" + p.config.Separator +
		"leased" + p.config.Separator + ip
}

func (p *PluginState) leasedNicKey(nic string) string {
	return p.config.Prefix + p.config.Separator + "nics" + p.config.Separator +
		"leased" + p.config.Separator + nic
}

// getKey returns the value of a key and whether it exists.
func getKey(t *testing.T, key string) (string, bool) {
	t.Helper()

	resp, err := etcd.NewKV(testClient).Get(context.Background(), key)
	if err != nil {
		t.Fatalf("could not read %s: %v", key, err)
	}
	if len(resp.Kvs) == 0 {
		return "", false
	}
	return string(resp.Kvs[0].Value), true
}

// keyLease returns the etcd lease id attached to a key, or 0.
func keyLease(t *testing.T, key string) etcd.LeaseID {
	t.Helper()

	resp, err := etcd.NewKV(testClient).Get(context.Background(), key)
	if err != nil {
		t.Fatalf("could not read %s: %v", key, err)
	}
	if len(resp.Kvs) == 0 {
		return 0
	}
	return etcd.LeaseID(resp.Kvs[0].Lease)
}

// countKeys returns how many keys exist under a prefix.
func countKeys(t *testing.T, prefix string) int {
	t.Helper()

	resp, err := etcd.NewKV(testClient).Get(context.Background(), prefix,
		etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		t.Fatalf("could not count %s: %v", prefix, err)
	}
	return int(resp.Count)
}

// countLeases returns the number of leases the whole etcd server is holding.
func countLeases(t *testing.T) int {
	t.Helper()

	resp, err := etcd.NewLease(testClient).Leases(context.Background())
	if err != nil {
		t.Fatalf("could not list leases: %v", err)
	}
	return len(resp.Leases)
}

// allocatorHas reports whether ip is inside the plugin's configured range.
func (p *PluginState) allocatorHas(ip net.IP) bool {
	for _, ipnet := range p.allocator.Range() {
		if ipnet.IP.Equal(ip) {
			return true
		}
	}
	return false
}

func mustParseMAC(t *testing.T, s string) net.HardwareAddr {
	t.Helper()

	mac, err := net.ParseMAC(s)
	if err != nil {
		t.Fatalf("could not parse mac %s: %v", s, err)
	}
	return mac
}
