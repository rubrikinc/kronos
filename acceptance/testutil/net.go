package testutil

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/rubrikinc/kronos/kronosutil"
)

func randomInt(min, max int) int {
	return min + rand.New(rand.NewSource(time.Now().UnixNano())).Intn(max-min+1)
}

// GetFreePorts asks the kernel for free open ports that are ready to use.
func GetFreePorts(ctx context.Context, numPorts int) (ports []int, err error) {
	for len(ports) < numPorts {
		port := randomInt(10000, 12000)
		l, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			continue
		}
		defer kronosutil.CloseWithErrorLog(ctx, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}
