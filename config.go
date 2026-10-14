package etcdplugin

import (
	"fmt"
	"time"
)

type Config struct {
	CA        string
	Cert      string
	Key       string
	Endpoints []string
	Start     string
	End       string
	Prefix    string
	Separator string
	DNSZone   string
	DNSPrefix string
	DNSNames  string
	// bounds on the lease time we're willing to hand out, a client is free to
	// ask for anything at all and we're the ones that have to honour it
	MinLeaseTime time.Duration
	MaxLeaseTime time.Duration
	// how soon after a binding is granted a RELEASE for it is taken to be a
	// leftover from before the grant rather than about the grant itself, see
	// releaseLease
	ReleaseGrace time.Duration
	// how long an address a client has declined is kept out of the free pool,
	// see revokeLease
	DeclineProbation time.Duration
}

// clampLeaseTime bounds a lease time to the configured range
func (c Config) clampLeaseTime(d time.Duration) time.Duration {
	if d < c.MinLeaseTime {
		return c.MinLeaseTime
	}
	if d > c.MaxLeaseTime {
		return c.MaxLeaseTime
	}
	return d
}

func (c Config) String() string {
	return fmt.Sprintf("CA=%s Cert=%s Key=%s Endpoints=%v Start=%s End=%s Prefix=%s Separator=%s DNSZone=%s DNSPrefix=%s DNSNames=%s MinLeaseTime=%s MaxLeaseTime=%s ReleaseGrace=%s DeclineProbation=%s",
		c.CA, c.Cert, c.Key, c.Endpoints, c.Start, c.End, c.Prefix, c.Separator, c.DNSZone, c.DNSPrefix, c.DNSNames,
		c.MinLeaseTime, c.MaxLeaseTime, c.ReleaseGrace, c.DeclineProbation)
}
