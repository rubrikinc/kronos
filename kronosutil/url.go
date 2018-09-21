package kronosutil

import (
	"net"
	"net/url"
	"path/filepath"

	"github.com/rubrikinc/kronos/pb"
)

// NodeAddr converts address in host:port format to NodeAddr.
func NodeAddr(addr string) (*kronospb.NodeAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	return &kronospb.NodeAddr{Host: host, Port: port}, nil
}

// NodeAddrToString joins host and port and returns the address in the form of "127.0.0.1:5766"
func NodeAddrToString(addr *kronospb.NodeAddr) string {
	return net.JoinHostPort(addr.Host, addr.Port)
}

// AddrToURL converts kronos NodeAddr to http URL
func AddrToURL(addr *kronospb.NodeAddr, secure bool) url.URL {
	host := NodeAddrToString(addr)
	var scheme string
	if secure {
		scheme = "https"
	} else {
		scheme = "http"
	}
	return url.URL{
		Scheme: scheme,
		Host:   host,
	}
}

// AddToURLPath resolves baseURL to the given path.
func AddToURLPath(baseURL url.URL, path string) url.URL {
	addNodeURL := baseURL
	addNodeURL.Path = filepath.Join(baseURL.Path, path)
	return addNodeURL
}
