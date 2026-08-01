package etcdplugin

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	etcdpb "go.etcd.io/etcd/api/v3/etcdserverpb"
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

// leaseIP associates nic with ip for ttl, either by taking the ip out of the
// free pool or by renewing an association that already exists. It returns the
// etcd lease the association is attached to, so that callers can hang related
// records off the same lifetime.
func (p *PluginState) leaseIP(ctx context.Context, nic net.HardwareAddr, ip net.IP, ttl time.Duration) (etcd.LeaseID, error) {
	kvc := etcd.NewKV(p.client)
	lessor := etcd.NewLease(p.client)

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

// revokeLease returns ip to the free pool, but only if nic actually holds it.
// The caller is expected to pass the address the client named itself rather
// than whatever the nic happens to be holding, so that a client can only ever
// give up a lease it can identify.
func (p *PluginState) revokeLease(ctx context.Context, nic net.HardwareAddr, ip net.IP) error {
	kvc := etcd.NewKV(p.client)

	leasedNicKey := p.config.Prefix + p.config.Separator +
		"nics" + p.config.Separator +
		"leased" + p.config.Separator +
		nic.String()
	leasedIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"leased" + p.config.Separator +
		ip.String()
	freeIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator +
		ip.String()

	txres, err := kvc.Txn(ctx).If(
		// both halves of the association have to point at each other, a missing
		// key compares as empty and fails here just the same
		etcd.Compare(etcd.Value(leasedNicKey), "=", ip.String()),
		etcd.Compare(etcd.Value(leasedIPKey), "=", nic.String()),
	).Then(
		// read the etcd lease id back before the deletes take it away with them
		etcd.OpGet(leasedNicKey),
		etcd.OpDelete(leasedIPKey),
		etcd.OpDelete(leasedNicKey),
		etcd.OpPut(freeIPKey, ip.String()),
	).Commit()
	if err != nil {
		return errors.Wrap(err, "could not delete lease")
	}

	if !txres.Succeeded {
		return fmt.Errorf("nic %v does not hold a lease on %v: %w", nic, ip, ErrNotLeased)
	}

	// Whatever else was hanging off the same etcd lease (the DNS records) has to
	// go with it, the address is in the free pool now and the next client to get
	// it would otherwise inherit the previous tenant's name.
	kvs := txres.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 || kvs[0].Lease == 0 {
		return nil
	}

	if _, err := etcd.NewLease(p.client).Revoke(ctx, etcd.LeaseID(kvs[0].Lease)); err != nil {
		// the address is already back in the pool, and the records still go away
		// on their own once the etcd lease times out
		log.Errorf("could not revoke etcd lease %d of released ip %v, records linger until it expires: %v",
			kvs[0].Lease, ip, err)
	}

	return nil
}
