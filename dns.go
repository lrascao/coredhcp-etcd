package etcdplugin

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	etcd "go.etcd.io/etcd/client/v3"
)

type DNS struct {
	prefix    string
	zone      string
	separator string
	// map static MAC to DNS name
	static map[string]string
	// map DNS alias
	aliases map[string]string
}

func NewDNS(prefix, zone, separator, namesFile string) (*DNS, error) {
	static, aliases, err := LoadNames(namesFile)
	if err != nil {
		return nil, err
	}

	dns := &DNS{
		prefix:    prefix,
		zone:      zone,
		separator: separator,
		static:    static,
		aliases:   aliases,
	}

	return dns, nil
}

func (d DNS) Register(ctx context.Context, client *etcd.Client,
	hostname string, ip net.IP,
	mac net.HardwareAddr,
	ttl time.Duration) error {
	kvc := etcd.NewKV(client)

	lease, err := etcd.NewLease(client).
		Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return errors.Wrap(err, "could not create new lease")
	}

	// is this a static entry?
	if name, ok := d.static[mac.String()]; ok {
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			"A" + d.separator +
			name

		if _, err := kvc.Put(ctx, nameKey, ip.String()); err != nil {
			return errors.Wrap(err, "could not register name")
		}
	} else if alias, ok := d.aliases[hostname]; ok {
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			"A" + d.separator +
			alias
		cnameKey := d.prefix + d.separator +
			d.zone + d.separator +
			"CNAME" + d.separator +
			hostname

		if _, err := kvc.Put(ctx, nameKey, ip.String(),
			etcd.WithLease(lease.ID)); err != nil {
			return errors.Wrap(err, "could not register A name")
		}

		if _, err := kvc.Put(ctx, cnameKey, alias,
			etcd.WithLease(lease.ID)); err != nil {
			return errors.Wrap(err, "could not register CNAME name")
		}
	} else {
		// not static, no alias, simply register
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			"A" + d.separator +
			hostname

		if _, err := kvc.Put(ctx, nameKey, ip.String(),
			etcd.WithLease(lease.ID)); err != nil {
			return errors.Wrap(err, "could not register A name")
		}
	}

	return nil
}

func LoadNames(filename string) (map[string]string, map[string]string, error) {
	log.Infof("reading names from %s", filename)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, nil, err
	}

	static := make(map[string]string)
	aliases := make(map[string]string)

	for _, lineBytes := range bytes.Split(data, []byte{'\n'}) {
		line := string(lineBytes)
		if len(line) == 0 {
			continue
		}
		// comment
		if strings.HasPrefix(line, "#") {
			continue
		}

		tokens := strings.Fields(line)
		if len(tokens) != 3 {
			return nil, nil, fmt.Errorf("malformed line, want 3 fields, got %d: %s", len(tokens), line)
		}
		switch tokens[0] {
		case "static":
			name := tokens[1]
			hwaddr, err := net.ParseMAC(tokens[2])
			if err != nil {
				return nil, nil, fmt.Errorf("malformed hardware address: %s", tokens[2])
			}

			static[hwaddr.String()] = name
		case "alias":
			name := tokens[1]
			alias := tokens[2]

			aliases[alias] = name
		}
	}

	return static, aliases, nil
}
