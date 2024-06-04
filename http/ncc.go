package http

import "net"

type ncc struct {
	net.Conn
	haveb bool
	b     byte
}

func (nc *ncc) Read(b []byte) (n int, err error) {
	if nc.haveb {
		nc.haveb = false
		b[0] = nc.b
		b = b[1:]
	}
	n, err = nc.Conn.Read(b)
	n += 1
	return
}

func (n *ncc) Wait() error {
	var b = [1]byte{}
	_, err := n.Conn.Read(b[:])
	if err != nil {
		return err
	}
	n.b = b[0]
	n.haveb = true
	return nil
}
