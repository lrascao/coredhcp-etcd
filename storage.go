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
		declinedIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"declined" + p.config.Separator +
			ipnet.IP.String()

		res, err := kvc.Txn(ctx).If(
			etcdutil.KeyMissing(freeIPKey),
			etcdutil.KeyMissing(leasedIPKey),
			// a restart must not cut a quarantine short
			etcdutil.KeyMissing(declinedIPKey),
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

	// A declined address is out of both sets on purpose, and only comes back
	// once its probation lease has expired and taken the key with it. See
	// revokeLease.
	declinedIPPrefix := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"declined" + p.config.Separator

	resp, err = kvc.Get(ctx, declinedIPPrefix, etcd.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "could not list declined ips")
	}

	declined := make(map[string]struct{})
	for _, kv := range resp.Kvs {
		parts := strings.Split(string(kv.Key), p.config.Separator)
		ip := parts[len(parts)-1]

		declined[ip] = struct{}{}
	}

	for _, ipnet := range p.allocator.Range() {
		ip := ipnet.IP

		if _, ok := free[ip.String()]; ok {
			continue
		}
		if _, ok := leased[ip.String()]; ok {
			continue
		}
		if _, ok := declined[ip.String()]; ok {
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
		declinedIPKey := p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"declined" + p.config.Separator +
			ip.String()

		res, err := kvc.Txn(ctx).If(
			etcdutil.KeyMissing(freeIPKey),
			etcdutil.KeyMissing(leasedIPKey),
			// declined since the listing above
			etcdutil.KeyMissing(declinedIPKey),
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

// renewLease is leaseIP for a REQUEST that names no server, see confirm. The
// client is not asking for an address but holding on to one it was given, so
// nothing comes out of the free pool: nic has to hold ip already, and the
// binding moves onto a fresh etcd lease of ttl with a grant naming server. It
// returns that lease, and the one the binding was renewed from.
//
// It fails with ErrNoBinding when there is no binding for nic at all, and with
// ErrNotLeased when nic holds some other address or ip is somebody else's.
//
// A REQUEST without a server identifier is broadcast in INIT-REBOOT and
// REBINDING, so every server sharing these bindings gets a copy, and every one
// of them could vouch for it. Only one does. The renewal only goes in if the
// binding is still as it was read, so of the servers that read it together
// just one gets its renewal in, and the others read it again to find out why.
// A server that finds the binding granted by another one within
// constAnsweredWindow takes it to be that server answering this same
// broadcast, and backs off with ErrAnswered. The client gets a single ACK, and
// the grant names the server that sent it, which is the one its unicast
// renewals and its RELEASE go to. releaseLease depends on that, and with every
// server answering, the grant would name whichever renewal landed last rather
// than the ACK the client took.
//
// Unlike releaseLease, this compares a grant's timestamp against this node's
// clock when another node wrote it. Here that only ever decides who answers:
// two nodes further apart than the window can both answer one broadcast, which
// costs the client a duplicate ACK, and it never decides whether a binding
// survives.
func (p *PluginState) renewLease(ctx context.Context, nic net.HardwareAddr, ip net.IP, ttl time.Duration, server net.IP) (etcd.LeaseID, etcd.LeaseID, error) {
	kvc := etcd.NewKV(p.client)
	lessor := etcd.NewLease(p.client)

	granted, err := json.Marshal(grant{Server: server.String(), Time: time.Now().UTC()})
	if err != nil {
		return 0, 0, errors.Wrap(err, "could not encode grant")
	}

	lease, err := lessor.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, 0, errors.Wrap(err, "could not create new lease")
	}

	// as in leaseIP, hand the lease back on every path that doesn't end up
	// attaching the binding to it
	attached := false
	defer func() {
		if attached {
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()

		if _, err := lessor.Revoke(ctx, lease.ID); err != nil {
			log.Debugf("could not revoke unused lease %d: %v", lease.ID, err)
		}
	}()

	// an attempt only fails because the binding changed after it was read, and
	// the next read finds out how
	for attempt := 0; attempt < 3; attempt++ {
		b, err := p.readBinding(ctx, nic, ip)
		if err != nil {
			return 0, 0, err
		}

		if b.leasedNic == nil {
			return 0, 0, fmt.Errorf("there is no binding for nic %v: %w", nic, ErrNoBinding)
		}
		if !b.held() {
			return 0, 0, fmt.Errorf("nic %v does not hold a lease on %v: %w", nic, ip, ErrNotLeased)
		}

		if g, ok := b.granted(); ok && g.Server != server.String() {
			if age := time.Since(g.Time); age > -constAnsweredWindow && age < constAnsweredWindow {
				return 0, 0, fmt.Errorf("%v on %v was granted by %s %v ago: %w",
					nic, ip, g.Server, age.Round(time.Millisecond), ErrAnswered)
			}
		}

		res, err := kvc.Txn(ctx).If(
			etcd.Compare(etcd.ModRevision(b.leasedNicKey), "=", b.leasedNic.ModRevision),
			etcd.Compare(etcd.ModRevision(b.leasedIPKey), "=", b.leasedIP.ModRevision),
		).Then(
			etcd.OpPut(b.leasedNicKey, ip.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(b.leasedIPKey, nic.String(), etcd.WithLease(lease.ID)),
			etcd.OpPut(b.grantedNicKey, string(granted), etcd.WithLease(lease.ID)),
		).Commit()
		if err != nil {
			return 0, 0, errors.Wrap(err, "could not renew lease")
		}

		if res.Succeeded {
			attached = true
			return lease.ID, etcd.LeaseID(b.leasedNic.Lease), nil
		}
	}

	return 0, 0, fmt.Errorf("binding of nic %v on %v kept changing while it was being renewed", nic, ip)
}

// carryOver moves whatever is still on the etcd lease a binding was renewed
// from onto the lease it was renewed to. That is the DNS records. Register puts
// them on the binding's lease so that they live and die with it, but only when
// the client sends its hostname, and a renewal without one would leave them to
// expire with the old lease while the binding carries on. A key only moves if
// nothing has rewritten it since it was read, so a name that another client
// has registered over in the meantime stays theirs.
func (p *PluginState) carryOver(ctx context.Context, from, to etcd.LeaseID) error {
	if from == 0 || from == to {
		return nil
	}

	kvc := etcd.NewKV(p.client)

	// an old lease that has already expired comes back with no keys
	ttl, err := etcd.NewLease(p.client).TimeToLive(ctx, from, etcd.WithAttachedKeys())
	if err != nil {
		return errors.Wrap(err, "could not list the keys of the previous lease")
	}

	for _, key := range ttl.Keys {
		res, err := kvc.Get(ctx, string(key))
		if err != nil {
			return errors.Wrapf(err, "could not read %s", key)
		}
		if len(res.Kvs) == 0 || etcd.LeaseID(res.Kvs[0].Lease) != from {
			continue
		}
		kv := res.Kvs[0]

		if _, err := kvc.Txn(ctx).If(
			etcd.Compare(etcd.ModRevision(string(key)), "=", kv.ModRevision),
		).Then(
			etcd.OpPut(string(key), string(kv.Value), etcd.WithLease(to)),
		).Commit(); err != nil {
			return errors.Wrapf(err, "could not move %s", key)
		}
	}

	return nil
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

	leasedNicKey, leasedIPKey, grantedNicKey, freeIPKey, declinedIPKey string
	leasedNic, leasedIP, grantedNic                                    *mvccpb.KeyValue
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
		declinedIPKey: p.config.Prefix + p.config.Separator +
			"ips" + p.config.Separator +
			"declined" + p.config.Separator +
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

// unbind tears a binding down and, in the same transaction, puts its address
// wherever next says: back into the free pool for a release, into quarantine
// for a decline. It only goes ahead if neither half has changed since the
// binding was read and checked, so that a renewal landing in between is never
// undone by it.
func (p *PluginState) unbind(ctx context.Context, b binding, next etcd.Op) error {
	kvc := etcd.NewKV(p.client)

	txres, err := kvc.Txn(ctx).If(
		etcd.Compare(etcd.ModRevision(b.leasedNicKey), "=", b.leasedNic.ModRevision),
		etcd.Compare(etcd.ModRevision(b.leasedIPKey), "=", b.leasedIP.ModRevision),
	).Then(
		etcd.OpDelete(b.leasedIPKey),
		etcd.OpDelete(b.leasedNicKey),
		etcd.OpDelete(b.grantedNicKey),
		next,
	).Commit()
	if err != nil {
		return errors.Wrap(err, "could not delete lease")
	}

	if !txres.Succeeded {
		return fmt.Errorf("lease of nic %v on %v changed while it was being ended: %w",
			b.nic, b.ip, ErrNotLeased)
	}

	// Whatever else was hanging off the same etcd lease (the DNS records) has to
	// go with it, the address no longer belongs to this client and the next one
	// to get it would otherwise inherit the previous tenant's name.
	if b.leasedNic.Lease == 0 {
		return nil
	}

	if _, err := etcd.NewLease(p.client).Revoke(ctx, etcd.LeaseID(b.leasedNic.Lease)); err != nil {
		// the binding is already gone, and the records still go away on their
		// own once the etcd lease times out
		log.Errorf("could not revoke etcd lease %d of unbound ip %v, records linger until it expires: %v",
			b.leasedNic.Lease, b.ip, err)
	}

	return nil
}

// revokeLease takes ip away from nic, but only if nic actually holds it, and
// quarantines the address instead of returning it to the free pool. The caller
// is expected to pass the address the client named itself rather than whatever
// the nic happens to be holding, so that a client can only ever give up a lease
// it can identify -- and, since the address is then withheld from everyone,
// so that a forged DECLINE cannot take somebody else's address out of
// circulation.
//
// This is what a DECLINE does. A RELEASE goes through releaseLease, which
// makes the same check and then some.
//
// A client declines an address when it finds another host already answering
// ARP for it, and RFC 2131 4.3.3 has the server mark such an address as not
// available. Putting it straight back into the free pool, as this used to do,
// made it the lowest free address again, so the very next DISCOVER was
// offered it: on 2026-10-10 one client declined 192.168.3.106 47 times in two
// hours, and on 2026-10-11 two clients declined .116 55 times between them,
// each time because a Shelly was still using an address whose binding had been
// freed underneath it.
//
// So the address goes under ips/declined instead, on an etcd lease of its own
// that runs for DeclineProbation, with the declining nic as the value for
// whoever comes looking. bootstrapLeasableRange and resurrectLeases leave
// declined addresses alone, so it is the expiry of that lease that hands the
// address back to resurrectLeases, which then returns it to the pool like any
// other expired one.
//
// A node running a version from before quarantine sees a declined address as
// being in neither the free nor the leased set, exactly like an expired one,
// and its resurrectLeases puts it back in the pool within 10s. Until every
// node is upgraded a quarantine only lasts as long as no old node looks.
func (p *PluginState) revokeLease(ctx context.Context, nic net.HardwareAddr, ip net.IP) error {
	b, err := p.readBinding(ctx, nic, ip)
	if err != nil {
		return err
	}

	if !b.held() {
		return fmt.Errorf("nic %v does not hold a lease on %v: %w", nic, ip, ErrNotLeased)
	}

	return p.quarantine(ctx, b)
}

// quarantine is unbind for a declined binding: the address is held back from
// the free pool for DeclineProbation rather than returned to it.
func (p *PluginState) quarantine(ctx context.Context, b binding) error {
	lessor := etcd.NewLease(p.client)

	probation, err := lessor.Grant(ctx, int64(p.config.DeclineProbation.Seconds()))
	if err != nil {
		return errors.Wrap(err, "could not create probation lease")
	}

	// as in leaseIP, the lease is granted before we know whether we get to use
	// it, and has to be handed back if we don't
	quarantined := false
	defer func() {
		if quarantined {
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()

		if _, err := lessor.Revoke(ctx, probation.ID); err != nil {
			log.Debugf("could not revoke unused probation lease %d: %v", probation.ID, err)
		}
	}()

	if err := p.unbind(ctx, b,
		etcd.OpPut(b.declinedIPKey, b.nic.String(), etcd.WithLease(probation.ID))); err != nil {
		return err
	}

	quarantined = true

	return nil
}

// releaseLease is revokeLease for a RELEASE addressed to server. The address
// goes back into the free pool rather than into quarantine, and on top of
// naming the right address the RELEASE has to be about the binding as it
// stands rather than one that came before it.
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

	return p.unbind(ctx, b, etcd.OpPut(b.freeIPKey, b.ip.String()))
}
