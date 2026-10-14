package etcdplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
	etcdutil "go.etcd.io/etcd/client/v3/clientv3util"
)

func (p *PluginState) bootstrapLeasableRange(ctx context.Context) error {
	kvc := etcd.NewKV(p.client)

	for _, ipnet := range p.allocator.Range() {
		freeIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"free" + p.config.Separator +
			ipnet.IP.String()
		leasedIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"leased" + p.config.Separator +
			ipnet.IP.String()

		res, err := kvc.Txn(ctx).If(
			etcdutil.KeyMissing(freeIPKey),
			etcdutil.KeyMissing(leasedIPKey),
		).Then(
			etcd.OpPut(freeIPKey, ipnet.IP.String()),
		).Commit()
		if err != nil {
			return errors.Wrap(err, "could not move ip to free state")
		}

		if res.Succeeded {
			log.Debugf("established %s as free", ipnet.IP)
		}
	}

	return nil
}

func (p *PluginState) monitorLeases(ctx context.Context, interval time.Duration) error {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		err := p.resurrectLeases(ctx)
		if err != nil {
			log.Errorf("could not resurrect leases: %v", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (p *PluginState) resurrectLeases(ctx context.Context) error {
	kvc := etcd.NewKV(p.client)

	leasedIPPrefix := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"leased" + p.config.Separator

	resp, err := kvc.Get(ctx, leasedIPPrefix, etcd.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "could not list leased ips")
	}

	leased := map[string]struct{}{}
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), p.config.Separator)
		ip := parts[len(parts)-1]

		leased[ip] = struct{}{}
	}

	freeIPPrefix := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator

	resp, err = kvc.Get(ctx, freeIPPrefix, etcd.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "could not list free ips")
	}

	free := make(map[string]struct{})
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), p.config.Separator)
		ip := parts[len(parts)-1]

		free[ip] = struct{}{}
	}

	for _, ipnet := range p.allocator.Range() {
		ip := ipnet.IP

		if _, ok := free[ip.String()]; ok {
			continue
		}
		if _, ok := leased[ip.String()]; ok {
			continue
		}

		log.Infof("moving %v from expired to free", ip)
		freeIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"free" + p.config.Separator +
			ip.String()
		leasedIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"leased" + p.config.Separator +
			ip.String()

		res, err := kvc.Txn(ctx).If(
			etcdutil.KeyMissing(freeIPKey),
			etcdutil.KeyMissing(leasedIPKey),
		).Then(
			etcd.OpPut(freeIPKey, ip.String()),
		).Commit()
		if err != nil {
			return errors.Wrap(err, "could not move ip to free state")
		}

		if res.Succeeded {
			log.Infof("resurrected expired %v back to free state", ip)
		}
	}
	return nil
}

func (p *PluginState) nicLeasedIP(ctx context.Context, nic net.HardwareAddr) (net.IP, error) {
	kvc := etcd.NewKV(p.client)

	key := p.config.Prefix + p.config.Separator +
		"nics" + p.config.Separator +
		"leased" + p.config.Separator +
		nic.String()

	resp, err := kvc.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("could not get etcd key: %w", err)
	}

	if len(resp.Kvs) == 0 {
		log.Debugf("%v key not found", key)
		return nil, nil
	}

	ip := net.ParseIP(string(resp.Kvs[0].Value))

	return ip, nil
}

// grant records which server handed out a binding, and when. It lives on the
// same etcd lease as the binding, so it describes that one instance of it: a
// renewal moves the binding onto a fresh lease and writes a fresh grant, and
// whatever ends the binding takes the grant with it.
type grant struct {
	Server string    `json:"server"`
	Time   time.Time `json:"time"`
}

// leaseIP associates nic with ip for ttl, either by taking the ip out of the
// free pool or by renewing an association that already exists, and records
// server as the one that granted it. It returns the etcd lease the association
// is attached to, so that callers can hang related records off the same
// lifetime.
func (p *PluginState) leaseIP(ctx context.Context, nic net.HardwareAddr, ip net.IP, ttl time.Duration, server net.IP) (etcd.LeaseID, error) {
	kvc := etcd.NewKV(p.client)
	lessor := etcd.NewLease(p.client)

	granted, err := json.Marshal(grant{Server: server.String(), Time: time.Now().UTC()})
	if err != nil {
		return 0, errors.Wrap(err, "could not encode grant")
	}

	lease, err := lessor.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, errors.Wrap(err, "could not create new lease")
	}

	// The lease is granted before we know whether we get to use it. Hand it
	// back on every path that doesn't end up attaching a key to it, otherwise a
	// client asking for addresses it can't have leaks one etcd lease per packet.
	attached := false
	defer func() {
		if attached {
			return
		}

		// the caller's context may already have expired, we still want the
		// lease back
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()

		if _, err := lessor.Revoke(ctx, lease.ID); err != nil {
			log.Debugf("could not revoke unused lease %d: %v", lease.ID, err)
		}
	}()

	freeIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator +
		ip.String()

	leasedIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"leased" + p.config.Separator +
		ip.String()

	leasedNicKey := p.config.Prefix + p.config.Separator +
		"nics" + p.config.Separator +
		"leased" + p.config.Separator +
		nic.String()

	grantedNicKey := p.config.Prefix + p.config.Separator +
		"nics" + p.config.Separator +
		"granted" + p.config.Separator +
		nic.String()

	res, err := kvc.Txn(ctx).If(
		// if the ip was previously free
		etcdutil.KeyExists(freeIPKey),
	).Then(
		etcd.OpTxn([]etcd.Cmp{
			etcdutil.KeyMissing(leasedNicKey),
			etcdutil.KeyMissing(leasedIPKey),
		}, []etcd.Op{
			// Unfree it, and associate it with this nic
			etcd.OpDelete(freeIPKey),
			etcd.OpPut(leasedNicKey, ip.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(leasedIPKey, nic.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(grantedNicKey, string(granted), etcd.WithLease(lease.ID)),
		}, nil),
	).Else(
		// Otherwise, we're _probably_ renewing it, so check that the current
		// association still matches
		etcd.OpTxn([]etcd.Cmp{
			etcd.Compare(etcd.Value(leasedNicKey), "=", ip.String()),
			etcd.Compare(etcd.Value(leasedIPKey), "=", nic.String()),
		}, []etcd.Op{
			// if it does, renew the lease
			etcd.OpPut(leasedNicKey, ip.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(leasedIPKey, nic.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(grantedNicKey, string(granted), etcd.WithLease(lease.ID)),
		}, nil),
	).Commit()
	if err != nil {
		return 0, errors.Wrap(err, "could not update for leased ip")
	}

	// If we did an else in the nested transaction, we failed to actually update
	// the lease
	if !res.Responses[0].Response.(*etcdpb.ResponseOp_ResponseTxn).ResponseTxn.Succeeded {
		return 0, fmt.Errorf("ip %+v is no longer free: %w", ip, ErrAlreadyLeased)
	}

	attached = true

	return lease.ID, nil
}

func (p *PluginState) freeIP(ctx context.Context) (net.IP, error) {
	kvc := etcd.NewKV(p.client)

	prefix := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator

	// we only ever look at the first key, ask etcd for just that one instead of
	// dragging the whole free pool over the wire on every DISCOVER
	resp, err := kvc.Get(ctx, prefix, etcd.WithPrefix(),
		etcd.WithSort(etcd.SortByKey, etcd.SortAscend),
		etcd.WithLimit(1))
	if err != nil {
		return nil, errors.Wrap(err, "could not get etcd key")
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("no free IP addresses")
	}

	ip := net.ParseIP(string(resp.Kvs[0].Value))

	return ip, nil
}

// binding is a nic<->ip association and the grant that handed it out, read
// from etcd in one snapshot. A key that does not exist reads as nil.
type binding struct {
	nic net.HardwareAddr
	ip  net.IP

	leasedNicKey, leasedIPKey, grantedNicKey, freeIPKey string
	leasedNic, leasedIP, grantedNic                     *mvccpb.KeyValue
}

func (p *PluginState) readBinding(ctx context.Context, nic net.HardwareAddr, ip net.IP) (binding, error) {
	kvc := etcd.NewKV(p.client)

	b := binding{
		nic: nic,
		ip:  ip,
		leasedNicKey: p.config.Prefix + p.config.Separator +
			"nics" + p.config.Separator +
			"leased" + p.config.Separator +
			nic.String(),
		leasedIPKey: p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"leased" + p.config.Separator +
			ip.String(),
		grantedNicKey: p.config.Prefix + p.config.Separator +
			"nics" + p.config.Separator +
			"granted" + p.config.Separator +
			nic.String(),
		freeIPKey: p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"free" + p.config.Separator +
			ip.String(),
	}

	// an unconditional transaction, so that the three reads are of the same
	// revision and a grant can be matched against the binding it was read with
	res, err := kvc.Txn(ctx).Then(
		etcd.OpGet(b.leasedNicKey),
		etcd.OpGet(b.leasedIPKey),
		etcd.OpGet(b.grantedNicKey),
	).Commit()
	if err != nil {
		return binding{}, errors.Wrap(err, "could not read lease")
	}

	first := func(i int) *mvccpb.KeyValue {
		kvs := res.Responses[i].GetResponseRange().Kvs
		if len(kvs) == 0 {
			return nil
		}
		return kvs[0]
	}
	b.leasedNic, b.leasedIP, b.grantedNic = first(0), first(1), first(2)

	return b, nil
}

// held reports whether both halves of the association point at each other. A
// missing key fails this just the same as one that points somewhere else.
func (b binding) held() bool {
	return b.leasedNic != nil && string(b.leasedNic.Value) == b.ip.String() &&
		b.leasedIP != nil && string(b.leasedIP.Value) == b.nic.String()
}

// granted returns the grant of the binding as it stands, if it has one. A
// grant on a different etcd lease than the binding belongs to an earlier
// instance of it -- a version of this plugin that predates grants renews a
// binding without writing one -- and says nothing about this one.
func (b binding) granted() (grant, bool) {
	if b.grantedNic == nil || b.leasedNic == nil ||
		b.leasedNic.Lease == 0 || b.grantedNic.Lease != b.leasedNic.Lease {
		return grant{}, false
	}

	var g grant
	if err := json.Unmarshal(b.grantedNic.Value, &g); err != nil {
		log.Errorf("could not decode grant %s, treating the lease as having none: %v",
			b.grantedNicKey, err)
		return grant{}, false
	}

	return g, true
}

// unbind tears a binding down and returns its address to the free pool. It only
// goes ahead if neither half has changed since the binding was read and
// checked, so that a renewal landing in between is never undone by it.
func (p *PluginState) unbind(ctx context.Context, b binding) error {
	kvc := etcd.NewKV(p.client)

	txres, err := kvc.Txn(ctx).If(
		etcd.Compare(etcd.ModRevision(b.leasedNicKey), "=", b.leasedNic.ModRevision),
		etcd.Compare(etcd.ModRevision(b.leasedIPKey), "=", b.leasedIP.ModRevision),
	).Then(
		etcd.OpDelete(b.leasedIPKey),
		etcd.OpDelete(b.leasedNicKey),
		etcd.OpDelete(b.grantedNicKey),
		etcd.OpPut(b.freeIPKey, b.ip.String()),
	).Commit()
	if err != nil {
		return errors.Wrap(err, "could not delete lease")
	}

	if !txres.Succeeded {
		return fmt.Errorf("lease of nic %v on %v changed while it was being ended: %w",
			b.nic, b.ip, ErrNotLeased)
	}

	// Whatever else was hanging off the same etcd lease (the DNS records) has to
	// go with it, the address is in the free pool now and the next client to get
	// it would otherwise inherit the previous tenant's name.
	if b.leasedNic.Lease == 0 {
		return nil
	}

	if _, err := etcd.NewLease(p.client).Revoke(ctx, etcd.LeaseID(b.leasedNic.Lease)); err != nil {
		// the address is already back in the pool, and the records still go away
		// on their own once the etcd lease times out
		log.Errorf("could not revoke etcd lease %d of released ip %v, records linger until it expires: %v",
			b.leasedNic.Lease, b.ip, err)
	}

	return nil
}

// revokeLease returns ip to the free pool, but only if nic actually holds it.
// The caller is expected to pass the address the client named itself rather
// than whatever the nic happens to be holding, so that a client can only ever
// give up a lease it can identify.
//
// This is what a DECLINE does. A RELEASE goes through releaseLease, which
// makes the same check and then some.
func (p *PluginState) revokeLease(ctx context.Context, nic net.HardwareAddr, ip net.IP) error {
	b, err := p.readBinding(ctx, nic, ip)
	if err != nil {
		return err
	}

	if !b.held() {
		return fmt.Errorf("nic %v does not hold a lease on %v: %w", nic, ip, ErrNotLeased)
	}

	return p.unbind(ctx, b)
}

// releaseLease is revokeLease for a RELEASE addressed to server, which on top
// of naming the right address has to be about the binding as it stands rather
// than one that came before it.
//
// Some clients bring a new DHCP session up before tearing the old one down,
// and the old session's parting RELEASE names its own address and its own
// server. The Shelly Gen3 devices here routinely reconnect that way:
// DISCOVER, REQUEST, ACK, and within a second a RELEASE of the lease they held
// before, addressed to whichever server granted that one. The address is
// usually the same, since DISCOVER hands a client back the binding it already
// has, so the RELEASE matched the binding granted a moment earlier. It went
// back into the free pool while the client kept using it, and the next client
// to DISCOVER was handed the same address. One Shelly did this 18 times in 4.5
// days.
//
// A server that keeps its own bindings shrugs off most of that: the binding is
// held by whoever granted it, and a RELEASE addressed to another server has
// nothing to release there. Here every server shares one set of bindings, so
// the grant record makes that explicit, and a RELEASE is refused with
// ErrStaleRelease when
//
//   - it is addressed to a server other than the one that granted the
//     binding, or
//   - it is addressed to that server but arrives within ReleaseGrace of the
//     grant. When both sessions went to the same server nothing in the packet
//     tells them apart, so this one is a judgement, and it errs towards keeping
//     the binding: refusing a genuine release only means the address stays
//     with the same client until it expires.
//
// The server check comes first, which is what makes the time check sound:
// only the server that wrote a grant's timestamp ever compares against it, so
// clocks on different nodes never meet.
//
// A binding without a grant of its own was granted or renewed by a version of
// this plugin that predates grants, and is released as it always was.
func (p *PluginState) releaseLease(ctx context.Context, nic net.HardwareAddr, ip net.IP, server net.IP) error {
	b, err := p.readBinding(ctx, nic, ip)
	if err != nil {
		return err
	}

	if !b.held() {
		return fmt.Errorf("nic %v does not hold a lease on %v: %w", nic, ip, ErrNotLeased)
	}

	if g, ok := b.granted(); ok {
		if g.Server != server.String() {
			return fmt.Errorf("%v on %v was granted by %s, not %s: %w",
				nic, ip, g.Server, server, ErrStaleRelease)
		}
		if age := time.Since(g.Time); age < p.config.ReleaseGrace {
			return fmt.Errorf("%v on %v was granted only %v ago: %w",
				nic, ip, age.Round(time.Millisecond), ErrStaleRelease)
		}
	}

	return p.unbind(ctx, b)
}
