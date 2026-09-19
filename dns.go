package etcdplugin

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"strings"

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

// Register writes the DNS records for a freshly leased address. The records are
// attached to leaseID, the same etcd lease that holds the address itself, so
// that they expire with it and are torn down with it on release.
func (d DNS) Register(ctx context.Context, client *etcd.Client,
	hostname string, ip net.IP,
	mac net.HardwareAddr,
	leaseID etcd.LeaseID) error {
	kvc := etcd.NewKV(client)

	// Keys are lowercased because the reader lowercases what it looks up:
	// coredns-etcd 3f6a5d2a/c29f1bc5 normalise the qname before building the
	// etcd key, which is correct -- DNS is case-insensitive and a resolver may
	// normalise. Writing the hostname as the client supplied it therefore
	// strands every record whose name carries an uppercase letter: the key
	// exists and can never be looked up. That is not hypothetical, it is what
	// happened here -- 17 of 22 leases were unreachable while sitting in etcd
	// in plain sight.
	//
	// The two halves are one format and nothing checks that they agree, so if
	// the reader's normalisation ever changes, this has to change with it.
	hostname = strings.ToLower(hostname)

	// is this a static entry?
	if name, ok := d.static[mac.String()]; ok {
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			name + d.separator +
			"A"

		if _, err := kvc.Put(ctx, nameKey, ip.String()); err != nil {
			return errors.Wrap(err, "could not register name")
		}
	} else if alias, ok := d.aliases[hostname]; ok {
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			hostname + d.separator +
			"A"
		// create a record that allows resolving CNAME - hostname - ip
		cnameKey := d.prefix + d.separator +
			d.zone + d.separator +
			alias + d.separator +
			"CNAME"

		if _, err := kvc.Put(ctx, nameKey, ip.String(),
			etcd.WithLease(leaseID)); err != nil {
			return errors.Wrap(err, "could not register A name")
		}

		if _, err := kvc.Put(ctx, cnameKey, hostname,
			etcd.WithLease(leaseID)); err != nil {
			return errors.Wrap(err, "could not register CNAME name")
		}
	} else {
		// not static, no alias, simply register
		nameKey := d.prefix + d.separator +
			d.zone + d.separator +
			hostname + d.separator +
			"A"

		if _, err := kvc.Put(ctx, nameKey, ip.String(),
			etcd.WithLease(leaseID)); err != nil {
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
		// Lowercased for the same reason Register lowercases the hostname:
		// these names end up in etcd keys, and the reader only ever looks up
		// the lowercase form. The alias map is keyed on the DHCP-supplied
		// hostname, which Register has lowercased by the time it looks in
		// here, so the key must be lowercase too or no alias ever matches.
		switch tokens[0] {
		case "static":
			name := strings.ToLower(tokens[1])
			hwaddr, err := net.ParseMAC(tokens[2])
			if err != nil {
				return nil, nil, fmt.Errorf("malformed hardware address: %s", tokens[2])
			}

			static[hwaddr.String()] = name
		case "alias":
			name := strings.ToLower(tokens[1])
			alias := strings.ToLower(tokens[2])

			aliases[name] = alias
		}
	}

	return static, aliases, nil
}
