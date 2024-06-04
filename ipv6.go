package fast

import (
	"context"
	"crypto/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/dnscache"
	"github.com/sirupsen/logrus"
)

var resolver = &dnscache.Resolver{
	Resolver: dnscache.NewResolverOnlyV6(),
	OnCacheMiss: func() {
		logrus.Debug("Dns Lookup!")
	},
}

var lastFlush = time.Now()

func FlushResolver() {
	if time.Since(lastFlush) < 20*time.Second {
		return
	}
	resolver.Refresh(true)
	lastFlush = time.Now()
}

type RandIpv6Dialer struct {
	Prefix    *net.IPNet
	dialCount atomic.Int32
}

func (r *RandIpv6Dialer) randomIpv6InSubnet() net.IP {
	ip := make(net.IP, net.IPv6len)
	copy(ip, r.Prefix.IP)
	ones, _ := r.Prefix.Mask.Size()

	_, err := rand.Read(ip[ones/8:])
	if err != nil {
		panic(err)
	}

	return ip
}

var dnsLock sync.Mutex

func (r *RandIpv6Dialer) DialTimeout(addr string, timeout time.Duration) (conn net.Conn, err error) {
	randomIp := r.randomIpv6InSubnet()
	dialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: randomIp}}
	//logrus.Infof("dialing as %s", randomIp.String())

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	dnsLock.Lock()
	ips, err := resolver.LookupHost(context.Background(), host)
	dnsLock.Unlock()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// first dial random from it
	idx := int(r.dialCount.Add(1)) % len(ips)
	conn, err = dialer.DialContext(ctx, "tcp6", "["+ips[idx]+"]:"+port)
	if err == nil {
		return
	}

	for i, ip := range ips {
		if i == idx {
			continue
		}
		conn, err = dialer.DialContext(ctx, "tcp6", "["+ip+"]:"+port)
		if err == nil {
			break
		}
	}
	return
}
