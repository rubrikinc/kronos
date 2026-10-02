package kronos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	leaktest "github.com/rubrikinc/kronos/crdbutils"
	"github.com/rubrikinc/kronos/mock"
	"github.com/rubrikinc/kronos/server"
)

// setKronosServer points the package global at s and restores it afterwards.
// Tests that swap it must not run in parallel.
func setKronosServer(t *testing.T, s *server.Server) func() {
	t.Helper()
	prev := kronosServer
	kronosServer = s
	return func() { kronosServer = prev }
}

// TestGetTimeSurfacesCause checks that GetTime reports *why* it failed, not
// merely that the budget expired. CockroachDB prints this in its stall fatal
// and previously had to recover the cause from metric gauges.
func TestGetTimeSurfacesCause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	cluster := mock.NewKronosCluster(1, 15*time.Second, 5*time.Second)
	defer cluster.Stop()
	// An un-ticked node has not initialized yet.
	defer setKronosServer(t, cluster.Node(0).Server)()

	_, err := GetTime(300 * time.Millisecond)
	a.Error(err)
	a.True(errors.Is(err, server.ErrNotInitialized), "got %v", err)
	a.Contains(err.Error(), "Couldn't get kronos time within timeout")
}

// TestGetTimeNonPositiveTimeout covers the one path where no attempt is made,
// so the error must not name a cause that was never observed.
func TestGetTimeNonPositiveTimeout(t *testing.T) {
	a := assert.New(t)
	defer setKronosServer(t, &server.Server{})()

	_, err := GetTime(-1)
	a.Error(err)
	a.Contains(err.Error(), "non-positive timeout")
	a.Nil(errors.Unwrap(err), "must not wrap a cause it did not observe")
}

func TestStallLogThrottle(t *testing.T) {
	a := assert.New(t)
	var th stallLogThrottle
	const cause, other = int32(1), int32(2)
	t0 := time.Now().UnixNano()

	// The first failure of a stall always emits.
	emit, suppressed := th.shouldLog(cause, t0)
	a.True(emit)
	a.EqualValues(0, suppressed)

	// The same cause inside the interval is counted, not printed.
	for i := 1; i <= 100; i++ {
		emit, _ = th.shouldLog(cause, t0+int64(i))
		a.False(emit)
	}
	emit, suppressed = th.shouldLog(cause, t0+int64(stallLogInterval))
	a.True(emit)
	a.EqualValues(100, suppressed)

	// A changed cause emits without waiting out the interval: the transition
	// is the diagnostic.
	t1 := t0 + int64(stallLogInterval) + int64(stallLogChangeInterval)
	emit, _ = th.shouldLog(other, t1)
	a.True(emit)

	// But no faster than stallLogChangeInterval, so a flapping cause cannot
	// restore the storm this throttle exists to stop.
	emit, _ = th.shouldLog(cause, t1+int64(stallLogChangeInterval)-1)
	a.False(emit)
	emit, _ = th.shouldLog(cause, t1+int64(stallLogChangeInterval))
	a.True(emit)
}

// TestStallLogThrottleConcurrent holds the clock still so that exactly one of
// N racing callers may emit and every other must be counted. This is the shape
// the throttle actually sees: hundreds of clock readers failing at once.
func TestStallLogThrottleConcurrent(t *testing.T) {
	a := assert.New(t)
	var th stallLogThrottle
	const goroutines, perGoroutine = 32, 200
	now := time.Now().UnixNano()

	var emitted atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				if emit, _ := th.shouldLog(1, now); emit {
					emitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	a.EqualValues(1, emitted.Load())

	// Everything else was counted and surfaces on the next emission.
	_, suppressed := th.shouldLog(1, now+int64(stallLogInterval))
	a.EqualValues(goroutines*perGoroutine-1, suppressed)
}

// captureLogs redirects the default kronos logger, which writes through logrus,
// into a buffer. The returned func stops capturing and returns what was logged;
// SetOutput takes logrus' write lock, so no write is in flight once it returns.
func captureLogs(t testing.TB) (stop func() string) {
	buf := &bytes.Buffer{}
	logrus.SetOutput(buf)
	restore := func() { logrus.SetOutput(os.Stderr) }
	t.Cleanup(restore)
	return func() string { restore(); return buf.String() }
}

// resetStallThrottles clears the package-level throttles. They remember the last
// emission, which would otherwise suppress the first line a test expects to see.
func resetStallThrottles(t *testing.T) {
	reset := func() { kronosTimeLog, kronosUptimeLog = stallLogThrottle{}, stallLogThrottle{} }
	reset()
	t.Cleanup(reset)
}

// TestCauseOf checks every exported sentinel is recognised and maps to a distinct
// value; the throttle treats a changed cause as "emit now", so aliasing two causes
// would hide a transition.
func TestCauseOf(t *testing.T) {
	a := assert.New(t)
	seen := map[int32]error{}
	for _, cause := range []error{
		server.ErrNotInitialized, server.ErrTimeCapNotInited, server.ErrTimeCapStale,
		server.ErrUptimeCapNotInited, server.ErrUptimeCapStale,
	} {
		got := causeOf(fmt.Errorf("%w: kronos time: 1", cause))
		a.NotZero(got, "%v not recognised", cause)
		prev, dup := seen[got]
		a.False(dup, "%v and %v map to the same cause", cause, prev)
		seen[got] = cause
	}
	a.Zero(causeOf(errors.New("unrelated")))
	a.Zero(causeOf(nil))
}

// TestLogStallMessage pins what an operator reads: the cause, how many failures
// were suppressed, and that a changed cause is reported at once.
func TestLogStallMessage(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()
	stop := captureLogs(t)
	var th stallLogThrottle
	const poll = 100 * time.Millisecond
	stale := fmt.Errorf("%w: kronos time: 1", server.ErrTimeCapStale)

	logStall(ctx, &th, "KronosTime", stale, poll) // first failure of a stall emits
	for i := 0; i < 5; i++ {
		logStall(ctx, &th, "KronosTime", stale, poll) // same cause, inside the interval
	}
	time.Sleep(2 * stallLogChangeInterval)
	logStall(ctx, &th, "KronosTime", fmt.Errorf("%w: kronos time: 2", server.ErrNotInitialized), poll)
	out := stop()

	a.Equal(2, strings.Count(out, "Failed to get KronosTime"), out)
	a.Contains(out, "Failed to get KronosTime, err: kronos time is beyond current time cap, "+
		"time cap is too stale: kronos time: 1. 0 further failures suppressed. Retrying every 100ms.")
	a.Contains(out, "Failed to get KronosTime, err: kronos server not yet initialized: "+
		"kronos time: 2. 5 further failures suppressed. Retrying every 100ms.")
}

// TestClockReadersReturnOnceInitialized covers the success return of each
// package-level reader, which the failure-path tests above never reach.
func TestClockReadersReturnOnceInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	cluster := mock.NewKronosCluster(1, 15*time.Second, 5*time.Second)
	defer cluster.Stop()
	node := cluster.Node(0)
	node.Clock.SetTime(int64(time.Hour))
	node.Clock.SetUptime(int64(time.Second))
	defer setKronosServer(t, node.Server)()
	cluster.TickN(node, 2) // a server needs two ticks to initialize

	tm, err := GetTime(time.Second)
	a.NoError(err)
	a.NotZero(tm)
	a.NotZero(Now())
	a.NotZero(Uptime())
}

// TestNowAndUptimeBlockUntilInitialized checks the retry loops: neither reader may
// return while Kronos is uninitialized, both must return once it is, and each must
// say why it is waiting.
func TestNowAndUptimeBlockUntilInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	resetStallThrottles(t)
	stop := captureLogs(t)
	cluster := mock.NewKronosCluster(1, 15*time.Second, 5*time.Second)
	defer cluster.Stop()
	node := cluster.Node(0)
	node.Clock.SetTime(int64(time.Hour))
	node.Clock.SetUptime(int64(time.Second))
	defer setKronosServer(t, node.Server)()

	gotNow, gotUptime := make(chan int64, 1), make(chan int64, 1)
	go func() { gotNow <- Now() }()
	go func() { gotUptime <- Uptime() }()

	time.Sleep(300 * time.Millisecond)
	select {
	case <-gotNow:
		t.Fatal("Now returned before Kronos was initialized")
	case <-gotUptime:
		t.Fatal("Uptime returned before Kronos was initialized")
	default:
	}

	cluster.TickN(node, 2)
	for name, ch := range map[string]chan int64{"Now": gotNow, "Uptime": gotUptime} {
		select {
		case v := <-ch:
			a.NotZero(v, name)
		case <-time.After(3 * time.Second):
			t.Fatalf("%s did not return after Kronos initialized", name)
		}
	}
	out := stop()
	a.Contains(out, "Failed to get KronosTime, err: kronos server not yet initialized")
	a.Contains(out, "Failed to get KronosUptime, err: kronos server not yet initialized")
}
