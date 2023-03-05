package etcdplugin

import (
	"encoding/binary"
	"net"
)

// IPAdd returns a copy of start + add.
// IPAdd(net.IP{192,168,1,1},30) returns net.IP{192.168.1.31}
func IPAdd(start net.IP, add int) net.IP { // IPv4 only
	start = start.To4()
	result := make(net.IP, 4)
	binary.BigEndian.PutUint32(result, binary.BigEndian.Uint32(start)+uint32(add))
	return result
}
