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
			log.Infof("resurrected expired %v", ip)
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

func (p *PluginState) leaseIP(ctx context.Context, nic net.HardwareAddr, ip net.IP, ttl time.Duration) error {
	kvc := etcd.NewKV(p.client)

	lease, err := etcd.NewLease(p.client).
		Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return errors.Wrap(err, "could not create new lease")
	}

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
		return errors.Wrap(err, "could not update for leased ip")
	}

	// If we did an else in the nested transaction, we failed to actually update
	// the lease
	if !res.Responses[0].Response.(*etcdpb.ResponseOp_ResponseTxn).ResponseTxn.Succeeded {
		return fmt.Errorf("ip %+v is no longer free: %w", ip, ErrAlreadyLeased)
	}

	return nil
}

func (p *PluginState) freeIP(ctx context.Context) (net.IP, error) {
	kvc := etcd.NewKV(p.client)

	prefix := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator

	resp, err := kvc.Get(ctx, prefix, etcd.WithPrefix(),
		etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, errors.Wrap(err, "could not get etcd key")
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("no free IP addresses")
	}

	ip := net.ParseIP(string(resp.Kvs[0].Value))

	return ip, nil
}

func (p *PluginState) revokeLease(ctx context.Context, nic net.HardwareAddr) error {
	kvc := etcd.NewKV(p.client)

	leasedNicKey := p.config.Prefix + p.config.Separator +
		"nics" + p.config.Separator +
		"leased" + p.config.Separator +
		nic.String()

	res, err := kvc.Get(ctx, leasedNicKey)
	if err != nil {
		return errors.Wrap(err, "could not get nic's current lease")
	}

	ip := string(res.Kvs[0].Value)

	leasedIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"leased" + p.config.Separator +
		ip
	freeIPKey := p.config.Prefix + p.config.Separator +
		"ips" + p.config.Separator +
		"free" + p.config.Separator +
		ip

	txres, err := kvc.Txn(ctx).If(
		etcdutil.KeyExists(leasedIPKey),
		etcdutil.KeyExists(leasedNicKey),
	).Then(
		etcd.OpDelete(leasedIPKey),
		etcd.OpDelete(leasedNicKey),
		etcd.OpPut(freeIPKey, ip),
	).Commit()

	if !txres.Succeeded {
		return errors.Wrap(err, "could not delete lease")
	}

	return nil
}
