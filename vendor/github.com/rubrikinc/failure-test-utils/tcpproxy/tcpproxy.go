package tcpproxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rubrikinc/failure-test-utils/failuregen"
	"github.com/rubrikinc/failure-test-utils/log"
)

// TCPProxy is the interface for test L4 proxy
type TCPProxy interface {
	Stop()
	Stats() ProxyStats
	BlockIncomingConns()
	BlockAllTraffic()
	UnblockIncomingConns()
	UnblockAllTraffic()
	BackendHostPort() string
	FrontendHostPort() string
}

// ProxyStats stores TCP proxy stats
type ProxyStats struct {
	// connections accepted by proxy from client, includes only the ones that
	// are currently getting served by proxy. It should not account for dropped
	// connections
	activeConnCtr int64
	// Connections dropped due to failure policies (not accounted in
	// activeConnCtr
	FrontendDropCtr int64
	// Connection those were getting served but got dropped due to failure
	// policy set on the response receiving side.
	backendDropCtr int64
}

type proxyStatsWrapper struct {
	sync.Mutex
	value ProxyStats
}

type testTCPProxy struct {
	ctx              context.Context
	listener         net.Listener
	frontendHostPort string
	backendHostPort  string
	quit             chan interface{}
	wg               sync.WaitGroup
	recvFg           failuregen.FailureGenerator
	acceptFg         failuregen.FailureGenerator
	stats            proxyStatsWrapper
}

func (t *testTCPProxy) BackendHostPort() string {
	return t.backendHostPort
}

func (t *testTCPProxy) FrontendHostPort() string {
	return t.frontendHostPort
}

// NewTCPProxy creates a new instance of an L4 test proxy
func NewTCPProxy(
	ctx context.Context,
	frontendHostPort string,
	backendHostPort string,
	recvFg failuregen.FailureGenerator,
	acceptFg failuregen.FailureGenerator,
) (TCPProxy, error) {
	uuidStr := uuid.New().String()
	t := &testTCPProxy{
		ctx:              log.WithLogTag(ctx, uuidStr, nil),
		quit:             make(chan interface{}),
		frontendHostPort: frontendHostPort,
		backendHostPort:  backendHostPort,
		recvFg:           recvFg,
		acceptFg:         acceptFg,
		stats:            proxyStatsWrapper{value: ProxyStats{}},
	}
	l, err := net.Listen("tcp", frontendHostPort)
	if err != nil {
		return nil, errors.Wrap(err, "listen")
	}
	t.listener = l
	t.wg.Add(1)
	go t.serve()
	log.Infof(t.ctx, "Started TCP-proxy on %d", frontendHostPort)
	return t, nil
}

// Stop stops the proxy from listening and also forcibly closes any connections.
func (t *testTCPProxy) Stop() {
	log.Warningf(
		t.ctx,
		"Stopping %s -> %s TCP-proxy",
		t.frontendHostPort,
		t.backendHostPort)
	close(t.quit)
	if err := t.listener.Close(); err != nil {
		log.Error(t.ctx, err)
	}
	t.wg.Wait()

	activeConn := t.stats.getActiveConnCtr()

	if activeConn != int64(0) {
		panic(fmt.Sprintf("found %d active conn should be 0", activeConn))
	}
}

func (stats *proxyStatsWrapper) getActiveConnCtr() int64 {
	stats.Lock()
	defer stats.Unlock()

	return stats.value.activeConnCtr
}

// Stats provides the tcp proxy stats
func (t *testTCPProxy) Stats() ProxyStats {
	t.stats.Lock()
	defer t.stats.Unlock()

	return t.stats.value
}

func (st ProxyStats) String() string {
	return fmt.Sprintf(
		"stats{activeConn: %d, frontendDrop: %d, backendDrop: %d}\n",
		st.activeConnCtr,
		st.FrontendDropCtr,
		st.backendDropCtr)
}

// BlockIncomingConns blocks all new incoming connections to the TCP proxy by
// dropping them. Existing connections which are already accepted and handled
// by TCP proxy will continue to serve till completed.
func (t *testTCPProxy) BlockIncomingConns() {
	t.acceptFg.SetFailureProbability(1.0)
	if log.V(3) {
		log.Infof(t.ctx, "Going to drop new connections from client")
	}
}

// BlockAllTraffic blocks all traffic in both directions
func (t *testTCPProxy) BlockAllTraffic() {
	t.acceptFg.SetFailureProbability(1.0)
	t.recvFg.SetFailureProbability(1.0)
	if log.V(3) {
		log.Infof(t.ctx, "Going to drop all traffic")
	}
}

// UnblockIncomingConns unblocks all new incoming connections to the TCP proxy
func (t *testTCPProxy) UnblockIncomingConns() {
	t.acceptFg.SetFailureProbability(0.0)
	if log.V(3) {
		log.Infof(t.ctx, "Unblocking new connections from client")
	}
}

// UnblockAllTraffic unblocks all traffic in both directions
func (t *testTCPProxy) UnblockAllTraffic() {
	t.acceptFg.SetFailureProbability(0.0)
	t.recvFg.SetFailureProbability(0.0)
	if log.V(3) {
		log.Infof(t.ctx, "Unblocking all traffic")
	}
}

// close connections gracefully
func (t *testTCPProxy) closeFrontendConn(
	conn net.Conn,
	reason string,
) {
	if log.V(3) {
		log.Infof(t.ctx, "closing connection to %v reason %s",
			conn.RemoteAddr(), reason)
	}
	_ = conn.Close()

	if reason == "drop" {
		t.stats.incrementFrontendDropCtr()
	}
	t.stats.decrementActiveConnCtr()
}

func (t *testTCPProxy) serve() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.quit:
				// error was because the proxy was stopped, safe to ignore
				return
			default:
				log.Errorf(t.ctx, "accept error: %v", err)
			}
		} else {
			log.Infof(t.ctx, "Accepted connection from %v", conn.RemoteAddr())

			t.stats.incrementActiveConnCtr()

			if err := t.acceptFg.FailMaybe(); err != nil {
				log.Warningf(
					t.ctx,
					"injected accept failure %v,  %v",
					conn.RemoteAddr(),
					err)
				t.closeFrontendConn(conn, "drop")
				continue
			}
			t.wg.Add(1)
			go func() {
				log.Infof(t.ctx, "Accepted connection from %v",
					conn.RemoteAddr())
				if err := t.handle(conn); err != nil {
					log.Errorf(t.ctx, "handle err: %v", err)
				}
				t.wg.Done()
			}()
		}
	}
}

func (stats *proxyStatsWrapper) incrementActiveConnCtr() {
	stats.Lock()
	defer stats.Unlock()
	stats.value.activeConnCtr++
}

func (stats *proxyStatsWrapper) decrementActiveConnCtr() {
	stats.Lock()
	defer stats.Unlock()
	stats.value.activeConnCtr--
}

func (stats *proxyStatsWrapper) incrementBackendDropCtr() {
	stats.Lock()
	defer stats.Unlock()
	stats.value.backendDropCtr++
}

func (stats *proxyStatsWrapper) incrementFrontendDropCtr() {
	stats.Lock()
	defer stats.Unlock()
	stats.value.FrontendDropCtr++
}

func (t *testTCPProxy) copy(
	dest, src net.Conn,
	selfTermCh chan struct{},
	peerTermCh chan struct{},
) error {
	defer close(selfTermCh)
	buf := make([]byte, 1024)
	// Robustly close connections when proxy closes
	// https://eli.thegreenplace.net/2020/graceful-shutdown-of-a-tcp-server-in-go/#id1
	for {
		var nr int
		select {
		case <-peerTermCh:
			return nil
		case <-t.quit:
			return nil
		default:
			if err := src.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
				return errors.Wrap(err, "set source deadline")
			}
			var err error
			nr, err = src.Read(buf)
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				} else if err != io.EOF {
					return errors.Wrap(err, "read")
				}
			}
			if nr == 0 {
				return nil
			}
			if log.V(4) {
				log.Infof(t.ctx, "received from %v: %s", src.RemoteAddr(),
					string(buf[:nr]))
			}

			// TODO(CDM-362117)(Ambar) Change to a KMP filter to make this robust
			condFailGen, ok := (t.recvFg).(failuregen.ConditionalFailureGenerator)
			if ok {
				if err := condFailGen.FailOnCondition(buf); err != nil {
					t.stats.incrementBackendDropCtr()
					return errors.Wrap(err, "injected recv failure on satisfying condition")
				}
			} else {
				if err := t.recvFg.FailMaybe(); err != nil {
					t.stats.incrementBackendDropCtr()
					return errors.Wrap(err, "injected recv failure")
				}
			}
			return nil

		}
		_, err := dest.Write(buf[:nr])

		if err != nil {
			return errors.Wrap(err, "write")
		}
		if log.V(4) {
			log.Infof(t.ctx, "written to %v: %s", dest.RemoteAddr(),
				string(buf[:nr]))
		}
	}
}

func (t *testTCPProxy) handle(frontendConn net.Conn) error {
	defer t.closeFrontendConn(frontendConn, "task completed")
	backendConn, err := net.Dial("tcp", t.backendHostPort)
	if err != nil {
		return errors.Wrap(err, "failed dialing to backend port")
	}
	defer backendConn.Close()
	log.Infof(
		t.ctx,
		"Created proxy connection %v -> %v",
		backendConn.LocalAddr(),
		backendConn.RemoteAddr())

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	onwardTermCh := make(chan struct{})
	returnTermCh := make(chan struct{})

	go func() {
		err := t.copy(backendConn, frontendConn, onwardTermCh, returnTermCh)
		if err != nil {
			log.Errorf(
				t.ctx,
				"copy from %s to %s err: %v",
				frontendConn.RemoteAddr(),
				backendConn.RemoteAddr(),
				err)
		}
		wg.Done()
	}()
	return t.copy(frontendConn, backendConn, returnTermCh, onwardTermCh)
}

func localhostAddress(port int) string {
	return fmt.Sprintf("localhost:%v", port)
}
