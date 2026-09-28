package kronos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/atomic"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosstats"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

var kronosServer *server.Server

const (
	// stallLogInterval caps the steady-state rate of the clock-failure log.
	// One line per second is enough to see a stall start, persist and end.
	stallLogInterval = time.Second
	// stallLogChangeInterval floors how soon a changed cause may re-emit, so a
	// flapping cause cannot reopen the storm.
	stallLogChangeInterval = 50 * time.Millisecond
)

// stallCauses lists the failure causes in a fixed order so the throttle can
// hold the current one in an atomic. causeOf returns index+1; 0 is unrecognised.
var stallCauses = []error{
	server.ErrNotInitialized,
	server.ErrTimeCapNotInited,
	server.ErrTimeCapStale,
	server.ErrUptimeCapNotInited,
	server.ErrUptimeCapStale,
}

func causeOf(err error) int32 {
	for i, cause := range stallCauses {
		if errors.Is(err, cause) {
			return int32(i + 1)
		}
	}
	return 0
}

// stallLogThrottle rate-limits a clock-failure log process-wide.
//
// It is lock-free by necessity: during a stall hundreds of clock readers in the
// process fail at once, so a mutex would recreate the contention being
// diagnosed. The per-call retry count alone bounded nothing -- each caller had
// its own counter, so N concurrent callers produced N times the output (2374
// lines in one second in a field stall).
type stallLogThrottle struct {
	last  atomic.Int64 // unix nanos of the last emitted line
	cause atomic.Int32
}

// shouldLog reports whether to emit a line for cause at now. A changed cause
// always emits: the transition is the diagnostic, and waiting out the interval
// could hide it entirely.
func (t *stallLogThrottle) shouldLog(cause int32, now int64) bool {
	interval := int64(stallLogInterval)
	if t.cause.Load() != cause {
		interval = int64(stallLogChangeInterval)
	}
	last := t.last.Load()
	// CAS loser stays quiet too: exactly one caller emits per interval.
	if now-last < interval || !t.last.CAS(last, now) {
		return false
	}
	t.cause.Store(cause)
	return true
}

// shouldCheckStall reports whether a caller's retry consults the throttle: the
// first 10, then every 10th until 200, then every 100th. A stall is hundreds of
// callers failing at once, and new callers start at retry 0, so the throttle is
// still consulted many times a second; a caller stuck past its first second
// stops paying for causeOf and time.Now on every retry.
func shouldCheckStall(retry int) bool {
	return retry < 10 || (retry < 200 && retry%10 == 0) || retry%100 == 0
}

var (
	kronosTimeLog   stallLogThrottle
	kronosUptimeLog stallLogThrottle
)

// logStall reports a caller's failed clock read through t. what names the value
// being read, e.g. "KronosTime", and appears in the message people grep for.
// retry is how many times this caller has already retried; it only gates the
// check and is not logged, since in a crowd of callers it describes one caller,
// not the stall.
func logStall(ctx context.Context, t *stallLogThrottle, what string, err error, retry int) {
	if !shouldCheckStall(retry) || !t.shouldLog(causeOf(err), time.Now().UnixNano()) {
		return
	}
	log.Errorf(ctx, "Failed to get %s: %v", what, err)
}

// Initialize initializes the kronos server.
// After Initialization, Now() in this package returns kronos time.
// If not initialized, Now() in this package returns system time
func Initialize(ctx context.Context, config server.Config) error {
	// Stop previous server
	if kronosServer != nil {
		kronosServer.Stop()
	}

	var err error
	kronosServer, err = server.NewKronosServer(ctx, config)
	if err != nil {
		return err
	}

	go func() {
		if err := kronosServer.RunServer(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}()

	log.Info(ctx, "Kronos server initialized")
	return nil
}

// Stop stops the kronos server
func Stop() {
	if kronosServer != nil {
		kronosServer.Stop()
		log.Info(context.TODO(), "Kronos server stopped")
	}
}

// IsActive returns whether kronos is running.
func IsActive() bool {
	return kronosServer != nil
}

// Now returns Kronos time, blocking until it is available. It has no timeout,
// so during a stall a caller never returns; prefer GetTime, which does.
// Fatals if Kronos was never initialized.
func Now() int64 {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 100 * time.Millisecond
	ctx := context.TODO()

	for retry := 0; ; retry++ {
		t, _, err := kronosServer.KronosTimeNowRaw(ctx)
		if err == nil {
			return t
		}
		logStall(ctx, &kronosTimeLog, "KronosTime", err, retry)
		time.Sleep(timePollInterval)
	}
}

// Uptime returns Kronos uptime. This function can block if kronos uptime
// is invalid.
func Uptime() int64 {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 500 * time.Millisecond
	ctx := context.TODO()

	for retry := 0; ; retry++ {
		u, _, err := kronosServer.KronosUptimeNowRaw(ctx)
		if err == nil {
			return u
		}
		logStall(ctx, &kronosUptimeLog, "KronosUptime", err, retry)
		time.Sleep(timePollInterval)
	}
}

// NodeID returns the NodeID of the kronos server in the kronos raft cluster.
// NodeID returns an empty string if kronosServer is not initialized
func NodeID(ctx context.Context) string {
	if kronosServer == nil {
		return ""
	}

	id, err := kronosServer.ID()
	if err != nil {
		log.Fatalf(ctx, "Failed to get kronosServer.ID, err: %v", err)
	}

	return id
}

// RemoveNode removes the given node from the kronos raft cluster
func RemoveNode(ctx context.Context, nodeID string) error {
	if len(nodeID) == 0 {
		return errors.New("node id is empty")
	}

	log.Infof(ctx, "Removing kronos node %s", nodeID)
	client, err := kronosServer.NewClusterClient()
	if err != nil {
		return err
	}
	defer client.Close()

	return client.RemoveNode(ctx, &kronoshttp.RemoveNodeRequest{
		NodeID: nodeID,
	})
}

// Metrics returns KronosMetrics
func Metrics() *kronosstats.KronosMetrics {
	if kronosServer == nil {
		return nil
	}
	return kronosServer.Metrics
}

// GetTime returns kronos time and an error if not able to
// get time within timeout
func GetTime(timeout time.Duration) (int64, error) {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 100 * time.Millisecond
	ctx := context.TODO()
	var lastErr error
	start := time.Now()
	for retry := 0; timeout == 0 || time.Since(start) < timeout; retry++ {
		t, _, err := kronosServer.KronosTimeNowRaw(ctx)
		if err == nil {
			return t, nil
		}
		lastErr = err
		logStall(ctx, &kronosTimeLog, "KronosTime", err, retry)
		time.Sleep(timePollInterval)
	}
	if lastErr == nil {
		// Only reachable for a non-positive timeout: no attempt was made, so
		// there is no cause to report. Never claim one we did not observe.
		return 0, fmt.Errorf("Couldn't get kronos time: non-positive timeout - %v", timeout)
	}
	// Wrapped, not flattened: CockroachDB fatals on this, and which cause fired
	// is the difference between a startup race and a lost oracle.
	return 0, fmt.Errorf(
		"Couldn't get kronos time within timeout - %v: %w", timeout, lastErr)
}

func Bootstrap(ctx context.Context, expectedNodeCount int32) error {
	if kronosServer == nil {
		return errors.New("kronos server is not initialized")
	}
	_, err := kronosServer.Bootstrap(ctx, &kronospb.BootstrapRequest{
		ExpectedNodeCount: expectedNodeCount,
	})
	return err
}
