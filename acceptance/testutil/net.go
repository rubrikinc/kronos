package testutil

import (
	"context"
	"net"

	"github.com/rubrikinc/kronos/kronosutil"
)

// GetFreePorts asks the kernel for free open ports that are ready to use.
func GetFreePorts(ctx context.Context, numPorts int) (ports []int, err error) {
	for i := 0; i < numPorts; i++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return nil, err
		}
		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return nil, err
		}
		defer kronosutil.CloseWithErrorLog(ctx, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}
