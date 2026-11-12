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
	a.True(th.shouldLog(cause, t0))

	// The same cause inside the interval is not printed.
	for i := 1; i <= 100; i++ {
		a.False(th.shouldLog(cause, t0+int64(i)))
	}
	a.True(th.shouldLog(cause, t0+int64(stallLogInterval)))

	// A changed cause emits without waiting out the interval: the transition
	// is the diagnostic.
	t1 := t0 + int64(stallLogInterval) + int64(stallLogChangeInterval)
	a.True(th.shouldLog(other, t1))

	// But no faster than stallLogChangeInterval, so a flapping cause cannot
	// restore the storm this throttle exists to stop.
	a.False(th.shouldLog(cause, t1+int64(stallLogChangeInterval)-1))
	a.True(th.shouldLog(cause, t1+int64(stallLogChangeInterval)))
}

// TestStallLogThrottleConcurrent holds the clock still so that exactly one of
// N racing callers may emit. This is the shape the throttle actually sees:
// hundreds of clock readers failing at once.
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
				if th.shouldLog(1, now) {
					emitted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	a.EqualValues(1, emitted.Load())
}

// TestShouldCheckStall pins which of a caller's retries consult the throttle.
func TestShouldCheckStall(t *testing.T) {
	a := assert.New(t)
	var checked []int
	for retry := 0; retry <= 400; retry++ {
		if shouldCheckStall(retry) {
			checked = append(checked, retry)
		}
	}
	want := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	for r := 10; r < 200; r += 10 {
		want = append(want, r)
	}
	want = append(want, 200, 300, 400)
	a.Equal(want, checked)
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

// logWatcher is a log sink a test can wait on while other goroutines are still
// logging. A plain bytes.Buffer is not safe to read during writes.
type logWatcher struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	notify chan struct{} // signalled after every write
}

func (w *logWatcher) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	select {
	case w.notify <- struct{}{}:
	default:
	}
	return n, err
}

func (w *logWatcher) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// watchLogs redirects the default kronos logger into a logWatcher until the
// test ends.
func watchLogs(t *testing.T) *logWatcher {
	w := &logWatcher{notify: make(chan struct{}, 1)}
	logrus.SetOutput(w)
	t.Cleanup(func() { logrus.SetOutput(os.Stderr) })
	return w
}

// hangLimit bounds each wait on a blocked reader. Normal runs wait a few
// milliseconds, so it fires only when a reader hangs, and does not make the
// result depend on how fast the machine runs.
const hangLimit = 30 * time.Second

// waitFor blocks until every one of want has been logged. After hangLimit it
// fails the test with everything logged so far.
func (w *logWatcher) waitFor(t *testing.T, want ...string) {
	t.Helper()
	deadline := time.After(hangLimit)
	for {
		out := w.String()
		var missing []string
		for _, s := range want {
			if !strings.Contains(out, s) {
				missing = append(missing, s)
			}
		}
		if len(missing) == 0 {
			return
		}
		select {
		case <-w.notify:
		case <-deadline:
			t.Fatalf("not logged within %v: %q\nlogs so far:\n%s", hangLimit, missing, out)
		}
	}
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

// TestLogStallMessage pins what an operator reads -- the cause and its details --
// and that a changed cause is reported at once while a retry the gate skips
// never logs. The throttle runs on a fake clock that only the test moves, so
// the result does not depend on how fast the machine runs.
func TestLogStallMessage(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()
	stop := captureLogs(t)
	// Start well past zero: the throttle's last emission starts at 0, and the
	// first failure must emit.
	now := int64(time.Hour)
	th := stallLogThrottle{now: func() int64 { return now }}
	stale := fmt.Errorf("%w: kronos time: 1", server.ErrTimeCapStale)

	logStall(ctx, &th, "KronosTime", stale, 0) // first failure of a stall emits
	for i := 1; i <= 5; i++ {
		logStall(ctx, &th, "KronosTime", stale, i) // same cause, inside the interval
	}
	now += int64(2 * stallLogChangeInterval)
	// Both retries carry a changed cause after the change interval, so either
	// would emit if it consulted the throttle; their texts tell them apart.
	skipped := fmt.Errorf("%w: kronos time: 11", server.ErrNotInitialized)
	checked := fmt.Errorf("%w: kronos time: 20", server.ErrNotInitialized)
	logStall(ctx, &th, "KronosTime", skipped, 11) // a skipped retry: never consults the throttle
	logStall(ctx, &th, "KronosTime", checked, 20) // a checked retry: the changed cause emits
	out := stop()

	a.Equal(2, strings.Count(out, "Failed to get KronosTime"), out)
	a.Contains(out, "Failed to get KronosTime: kronos time is beyond current time cap, "+
		"time cap is too stale: kronos time: 1")
	a.Contains(out, "Failed to get KronosTime: kronos server not yet initialized: kronos time: 20")
	a.NotContains(out, "kronos time: 11", "retry 11 is not a checked retry")
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
// say why it is waiting. It waits on the readers' own log lines rather than on
// sleeps, so its result does not depend on how fast the machine runs.
func TestNowAndUptimeBlockUntilInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	resetStallThrottles(t)
	logs := watchLogs(t)
	cluster := mock.NewKronosCluster(1, 15*time.Second, 5*time.Second)
	defer cluster.Stop()
	node := cluster.Node(0)
	node.Clock.SetTime(int64(time.Hour))
	node.Clock.SetUptime(int64(time.Second))
	restore := setKronosServer(t, node.Server)
	// Restore the global only once both readers have returned: a reader the
	// test gave up on is still inside Now or Uptime, reading it.
	var running atomic.Int32
	defer func() {
		if running.Load() == 0 {
			restore()
		}
	}()

	gotNow, gotUptime := make(chan int64, 1), make(chan int64, 1)
	running.Store(2)
	go func() { defer running.Dec(); gotNow <- Now() }()
	go func() { defer running.Dec(); gotUptime <- Uptime() }()

	// Each reader logs its first failure before it sleeps and retries, so once
	// both lines are in, both have tried and failed while uninitialized.
	logs.waitFor(t,
		"Failed to get KronosTime: kronos server not yet initialized",
		"Failed to get KronosUptime: kronos server not yet initialized",
	)
	select {
	case <-gotNow:
		t.Fatal("Now returned before Kronos was initialized")
	case <-gotUptime:
		t.Fatal("Uptime returned before Kronos was initialized")
	default:
	}

	cluster.TickN(node, 2)
	// The values are the node's clock, which the test set and never moves.
	for _, r := range []struct {
		name string
		got  chan int64
		want time.Duration
	}{{"Now", gotNow, time.Hour}, {"Uptime", gotUptime, time.Second}} {
		select {
		case v := <-r.got:
			a.EqualValues(r.want, v, r.name)
		case <-time.After(hangLimit):
			t.Fatalf("%s did not return within %v after Kronos initialized\nlogs so far:\n%s",
				r.name, hangLimit, logs.String())
		}
	}
}
