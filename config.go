package etcdplugin

import "fmt"

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
}

func (c Config) String() string {
	return fmt.Sprintf("CA=%s Cert=%s Key=%s Endpoints=%v Start=%s End=%s Prefix=%s Separator=%s DNSZone=%s DNSPrefix=%s DNSNames=%s",
		c.CA, c.Cert, c.Key, c.Endpoints, c.Start, c.End, c.Prefix, c.Separator, c.DNSZone, c.DNSPrefix, c.DNSNames)
}
