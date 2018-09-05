package kronos

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/kronosutil/log"
	"github.com/scaledata/kronos/mock"
	"github.com/scaledata/kronos/pb"
)

func TestSingleNodeKronos(t *testing.T) {
	// Detect leaked goroutines after test.
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster := mock.NewKronosCluster(1)
	defer cluster.Stop()
	node := cluster.Node(0)
	a.NotNil(node)
	node.Clock.SetTime(int64(time.Hour))
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, node.Server.ServerStatus())
	// KronosTimeNow() should give an error when not initialized.
	kt, err := node.Server.KronosTimeNow(ctx)
	a.Nil(kt)
	a.Error(err)
	// Time Server needs 2 ticks to initialize.
	cluster.Tick(node)
	cluster.Tick(node)
	a.Equal(kronospb.ServerStatus_INITIALIZED, node.Server.ServerStatus())

	// Verify KronosTimeNow() works.
	kt, err = node.Server.KronosTimeNow(ctx)
	kronosTime1, cap := kt.Time, kt.TimeCap
	a.NoError(err)
	a.Equal(int64(time.Hour+15*time.Second+1), cap)
	timeCap1 := cluster.StateMachine.State(ctx).TimeCap
	delta1 := node.Server.OracleDelta.Load()
	a.Equal(int64(time.Hour), kronosTime1)
	a.True(kronosTime1 < timeCap1)
	// First time the Server initializes.
	a.Equal(delta1, int64(0))

	// Bump Clock by 30 seconds. Kronos time should give an error as it is more
	// than time cap.
	node.Clock.AdvanceTime(30 * time.Second)
	kt, err = node.Server.KronosTimeNow(ctx)
	a.Nil(kt)
	a.Error(err)

	// After a Tick, a new time cap will be updated and KronosTime should work.
	cluster.Tick(node)
	kt, err = node.Server.KronosTimeNow(ctx)
	kronosTime2, cap2 := kt.Time, kt.TimeCap
	timeCap2 := cluster.StateMachine.State(ctx).TimeCap
	// Time Cap metric has the last seen time cap. The node does not have a fresh
	// reading of the state machine
	a.Equal(timeCap1, node.Server.Metrics.TimeCap.Value())
	delta2 := node.Server.OracleDelta.Load()
	a.NoError(err)
	a.Equal(timeCap2, cap2)
	a.Equal(cap2, int64(time.Hour+45*time.Second))
	a.Equal(int64(time.Hour+30*time.Second), kronosTime2)
	a.True(kronosTime2 < timeCap2)
	a.True(timeCap1 < timeCap2)
	a.Equal(delta2, int64(0))

	// After kronos restarts it should continue time from Time Cap.
	node = cluster.RestartNode(ctx, node)
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, node.Server.ServerStatus())
	// Node waits for 4 ticks before state machine management so that an
	// initialized server can become the oracle.
	cluster.TickN(node, 4)
	// Time Cap metric has the last seen time cap. The node does not have a fresh
	// reading of the state machine
	a.Equal(timeCap2, node.Server.Metrics.TimeCap.Value())
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, node.Server.ServerStatus())
	cluster.Tick(node)
	a.Equal(kronospb.ServerStatus_INITIALIZED, node.Server.ServerStatus())
	kt, err = node.Server.KronosTimeNow(ctx)
	a.NoError(err)
	kronosTime3, cap3 := kt.Time, kt.TimeCap
	a.Equal(cap3, int64(time.Hour+60*time.Second+1))
	a.True(kronosTime3 > timeCap2)
	// Time Cap metric has the last seen time cap. The node does not have a fresh
	// reading of the state machine
	a.Equal(timeCap2, node.Server.Metrics.TimeCap.Value())
	cluster.Tick(node)
	a.Equal(cap3, node.Server.Metrics.TimeCap.Value())
}

func TestMultiNodeClientStatus(t *testing.T) {
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 2)
	defer cluster.Stop()
	// This is the first ever oracle so after one Tick it should have proposed
	// itself.
	cluster.Tick(nodes[1])
	// Other nodes can tick 2 times and get errors until node 1 becomes
	// initialized, but they would not try to overthrow it.
	cluster.TickN(nodes[0], 2)
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[0].Server.ServerStatus())
	// Client should give status of kronos Server
	// Query Node 1 for status
	status01, err := nodes[0].Server.Client.Status(ctx, nodes[1].Server.GRPCAddr)
	a.NoError(err)
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, status01.ServerStatus)
	a.Equal(cluster.StateMachine.State(ctx), status01.OracleState)
	// Node 1 will initialize after another Tick
	cluster.Tick(nodes[1])
	status01, err = nodes[0].Server.Client.Status(ctx, nodes[1].Server.GRPCAddr)
	a.NoError(err)
	a.Equal(kronospb.ServerStatus_INITIALIZED, status01.ServerStatus)
	a.Equal(cluster.StateMachine.State(ctx), status01.OracleState)

	// Stop Node 1 and verify error
	cluster.StopNode(ctx, nodes[1])
	_, err = nodes[0].Server.Client.Status(ctx, nodes[1].Server.GRPCAddr)
	a.Error(err)
}

func TestMultiNodeServerStatus(t *testing.T) {
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()
	for _, node := range nodes {
		a.Equal(
			kronospb.ServerStatus_NOT_INITIALIZED,
			node.Server.ServerStatus(),
		)
	}

	// Node 1 needs two ticks to initialize.
	cluster.Tick(nodes[1])
	// When node 1 is trying to become oracle, it cannot be overthrown by other
	// nodes upto two ticks.
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[2])
	cluster.Tick(nodes[2])
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[0].Server.ServerStatus())
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[2].Server.ServerStatus())
	// When node 1 is still not oracle, it should return errors to OracleTime
	// requests.
	resp, err := nodes[1].Server.OracleTime(context.TODO(), &kronospb.OracleTimeRequest{})
	a.Error(err)
	a.Nil(resp)

	// After another tick, node 1 is initialized, so node 0 and node 2 can
	// initialize.
	cluster.Tick(nodes[1])
	log.Error(context.TODO(), nodes[1].Server.ServerStatus())
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[1].Server.ServerStatus())
	cluster.Tick(nodes[0])
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[0].Server.ServerStatus())
	cluster.Tick(nodes[2])
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[2].Server.ServerStatus())

	// Verify kronos time works
	_, err = nodes[0].Server.KronosTimeNow(ctx)
	a.NoError(err)
}

func TestMultiNodeBackwardJump(t *testing.T) {
	// Detect leaked goroutines after test
	ctx := context.TODO()
	cases := []struct {
		name         string
		isSameOracle bool
	}{
		{
			name:         "same oracle after restart",
			isSameOracle: true,
		},
		{
			name:         "different oracle after restart",
			isSameOracle: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			cluster, nodes := mock.InitializeCluster(a, 3)
			defer cluster.Stop()

			// Time Server needs 2 ticks to initialize
			cluster.Tick(nodes[1])
			cluster.Tick(nodes[1])
			cluster.Tick(nodes[0])
			cluster.Tick(nodes[2])
			// Node 1 should be oracle as it ticked first and respond to Time()
			a.Equal(
				cluster.StateMachine.State(ctx).Oracle,
				nodes[1].Server.GRPCAddr,
			)
			resp, err := nodes[1].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
			a.NoError(err)
			a.True(resp.Time > 0)

			kt, err := nodes[2].Server.KronosTimeNow(ctx)
			kronosTime0 := kt.Time
			a.NoError(err)
			a.Equal(int64(2*time.Hour), kronosTime0)

			timeCap0 := cluster.StateMachine.State(ctx).TimeCap
			a.Equal(int64(2*time.Hour+15*time.Second+1), timeCap0)

			var newOracle, nonOracle1, nonOracle2 *mock.Node
			// Node 1 is oracle at this point
			if tc.isSameOracle {
				newOracle = nodes[1]
				nonOracle1 = nodes[0]
				nonOracle2 = nodes[2]
			} else {
				newOracle = nodes[2]
				nonOracle1 = nodes[0]
				nonOracle2 = nodes[1]
			}

			cluster.StopNode(ctx, nonOracle2)
			cluster.StopNode(ctx, newOracle)
			cluster.StopNode(ctx, nonOracle1)

			nonOracle2 = cluster.RestartNode(ctx, nonOracle2)
			newOracle = cluster.RestartNode(ctx, newOracle)
			nonOracle1 = cluster.RestartNode(ctx, nonOracle1)

			nonOracle2.Clock.SetTime(int64(-10 * time.Hour))
			newOracle.Clock.SetTime(int64(-10 * time.Hour))
			nonOracle1.Clock.SetTime(int64(-10 * time.Hour))

			// Node waits for 4 ticks before state machine management so that an
			// initialized Server can become the oracle
			cluster.TickN(newOracle, 4)

			cluster.Tick(newOracle)
			if !tc.isSameOracle {
				// Time Server needs two ticks to initialize after overthrowing oracle
				// One Tick to overthrow oracle
				// One Tick to initialize time cap if it successfully overthrew oracle
				cluster.Tick(newOracle)
			}
			cluster.Tick(nonOracle2)
			cluster.Tick(nonOracle1)
			// NewOracle should be oracle and respond to Time()
			a.Equal(
				cluster.StateMachine.State(ctx).Oracle,
				newOracle.Server.GRPCAddr,
			)
			resp, err = newOracle.Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
			a.NoError(err)
			a.True(resp.Time > 0)

			kt, err = nonOracle1.Server.KronosTimeNow(ctx)
			kronosTime1 := kt.Time
			a.NoError(err)
			// A non initialized orcle continues time from time cap
			a.True(kronosTime1 > timeCap0)

			timeCap1 := cluster.StateMachine.State(ctx).TimeCap
			a.True(kronosTime1 < timeCap1)

			nonOracle2.Clock.AdvanceTime(time.Second / 2)
			newOracle.Clock.AdvanceTime(time.Second)
			nonOracle1.Clock.AdvanceTime(time.Second / 3)
			kt, err = nonOracle1.Server.KronosTimeNow(ctx)
			kronosTime2 := kt.Time
			a.NoError(err)
			a.True(kronosTime2 > kronosTime1)

			// Clusters are not in sync as they advanced by different time
			err = cluster.IsClusterInSync(ctx, nonOracle2, newOracle, nonOracle1)
			a.NotNil(err)
			// Clusters should be in sync after a Tick to adjust deltas
			cluster.Tick(newOracle)
			cluster.Tick(nonOracle2)
			cluster.Tick(nonOracle1)
			err = cluster.IsClusterInSync(ctx, nonOracle2, newOracle, nonOracle1)
			a.NoError(err)
		})
	}
}

func TestMultiNodesDeltaComputation(t *testing.T) {
	// Detect leaked goroutines after test
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()

	a.Equal(int64(0), nodes[0].Server.Metrics.IsOracle.Value())
	a.Equal(int64(0), nodes[1].Server.Metrics.IsOracle.Value())
	a.Equal(int64(0), nodes[2].Server.Metrics.IsOracle.Value())

	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[2])

	// Node 1 should be oracle since it ticked first and respond to Time()
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[1].Server.GRPCAddr,
	)
	a.Equal(int64(0), nodes[0].Server.Metrics.IsOracle.Value())
	a.Equal(int64(1), nodes[1].Server.Metrics.IsOracle.Value())
	a.Equal(int64(0), nodes[2].Server.Metrics.IsOracle.Value())
	resp, err := nodes[1].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.NoError(err)
	a.True(resp.Time > 0)

	// Verify deltas. Kronos Time now is ~2 Hours (Since Node 1 is oracle)
	kt, err := nodes[1].Server.KronosTimeNow(ctx)
	kronosTime := kt.Time
	a.NoError(err)
	a.Equal(int64(2*time.Hour), kronosTime)
	// Node 1 is oracle. It has 0 delta
	a.Equal(int64(0), nodes[1].Server.OracleDelta.Load())
	// Node 0 has delta of +1 hour
	a.Equal(int64(time.Hour), nodes[0].Server.OracleDelta.Load())
	// Node 2 has delta of -1 hour
	a.Equal(int64(-time.Hour), nodes[2].Server.OracleDelta.Load())

	nodes[0].Clock.AdvanceTime(time.Second)
	nodes[1].Clock.AdvanceTime(time.Second)
	nodes[2].Clock.AdvanceTime(time.Second)

	// Stop Node 1. Node 0 should become oracle after 3 ticks (but not before)
	// because we look for 3 consecutive errors.
	cluster.StopNode(ctx, nodes[1])
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[0])
	resp, err = nodes[0].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.Error(err)
	a.Nil(resp)
	cluster.Tick(nodes[0])
	resp, err = nodes[0].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.NoError(err)
	a.True(resp.Time > 0)
	// Node 2 should not respond to time as it is not oracle
	resp, err = nodes[2].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.Error(err)
	a.Nil(resp)
	// Verify deltas
	// Node 0 is oracle. It continues with delta of 1 hour
	a.Equal(int64(time.Hour), nodes[0].Server.OracleDelta.Load())
	// Node 2 continues with delta of -1 hour
	a.Equal(int64(-time.Hour), nodes[2].Server.OracleDelta.Load())

	// Advance clocks of live Nodes by different amounts
	nodes[0].Clock.AdvanceTime(1 * time.Hour)
	nodes[2].Clock.AdvanceTime(2 * time.Hour)
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[2])
	// Verify deltas
	// Node 0 is oracle. It continues with delta of 1 hour
	a.InDelta(
		int64(time.Hour),
		nodes[0].Server.OracleDelta.Load(),
		float64(10*time.Millisecond),
	)
	a.InDelta(
		int64(time.Hour),
		nodes[0].Server.Metrics.Delta.Value(),
		float64(10*time.Millisecond),
	)
	// Node 2 delta is now -2 hour
	a.InDelta(
		int64(-2*time.Hour),
		nodes[2].Server.OracleDelta.Load(),
		float64(10*time.Millisecond),
	)
	a.InDelta(
		int64(-2*time.Hour),
		nodes[2].Server.Metrics.Delta.Value(),
		float64(10*time.Millisecond),
	)

	// Node 1 is down. Expect an error
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NotNil(err)
	// Expect Node 0 and Node 2 to be in sync
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[2])
	a.NoError(err)

	nodes[0].Clock.AdvanceTime(2 * time.Second)
	nodes[2].Clock.AdvanceTime(1 * time.Second)
	// Stop Node 0. Node 2 should become oracle after 3 ticks.
	cluster.StopNode(ctx, nodes[0])
	cluster.TickN(nodes[2], 3)
	resp, err = nodes[2].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.NoError(err)
	a.True(resp.Time > 0)

	// Restart Node 0 and Node 1.
	nodes[0] = cluster.RestartNode(ctx, nodes[0])
	nodes[0].Clock.SetTime(0)
	nodes[1] = cluster.RestartNode(ctx, nodes[1])
	nodes[1].Clock.SetTime(int64(time.Hour))
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	// Verify deltas. Kronos time now is ~3 hours and 2 seconds
	kt, err = nodes[2].Server.KronosTimeNow(ctx)
	kronosTime = kt.Time
	a.NoError(err)
	a.InDelta(
		int64(3*time.Hour+2*time.Second),
		kronosTime,
		float64(10*time.Millisecond),
	)
	// Node 2 is oracle. It continues with same delta
	a.Equal(int64(0), nodes[0].Server.Metrics.IsOracle.Value())
	a.Equal(int64(0), nodes[1].Server.Metrics.IsOracle.Value())
	a.Equal(int64(1), nodes[2].Server.Metrics.IsOracle.Value())
	a.InDelta(
		int64(-2*time.Hour),
		nodes[2].Server.OracleDelta.Load(),
		float64(10*time.Millisecond),
	)
	a.InDelta(
		int64(-2*time.Hour),
		nodes[2].Server.Metrics.Delta.Value(),
		float64(10*time.Millisecond),
	)
	// Node 0 has delta of 3 hours
	a.InDelta(
		int64(3*time.Hour+2*time.Second),
		nodes[0].Server.OracleDelta.Load(),
		float64(10*time.Millisecond),
	)
	a.InDelta(
		int64(3*time.Hour+2*time.Second),
		nodes[0].Server.Metrics.Delta.Value(),
		float64(10*time.Millisecond),
	)
	// Node 1 has delta of -2 hours
	a.InDelta(
		int64(2*time.Hour+2*time.Second),
		nodes[1].Server.OracleDelta.Load(),
		float64(10*time.Millisecond),
	)
}

func TestMultiNodeHighRTT(t *testing.T) {
	// Detect leaked goroutines after test
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()

	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[1])
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[1].Server.ServerStatus())
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[1].Server.GRPCAddr,
	)

	// Set high Latency in Client
	cluster.Client.Latency = 250 * time.Millisecond
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[2].Server.ServerStatus())
	a.Equal(int64(0), nodes[2].Server.Metrics.RTT.Snapshot().ValueAtQuantile(100))
	// Node 2 will wait for two errors (high rtt) before attempting to become
	// oracle.
	cluster.Tick(nodes[2])
	cluster.Tick(nodes[2])
	a.Equal(int64(2), nodes[2].Server.Metrics.SyncFailureCount.Count())
	a.InDelta(
		int64(250*time.Millisecond),
		nodes[2].Server.Metrics.RTT.Snapshot().ValueAtQuantile(99),
		float64(10*time.Millisecond),
	)
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[2].Server.ServerStatus())
	// Node 1 is still the oracle
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[1].Server.GRPCAddr,
	)

	a.Equal(int64(0), nodes[2].Server.Metrics.OverthrowAttemptCount.Count())
	// Node 2 will try to become the oracle
	cluster.Tick(nodes[2])
	a.Equal(int64(3), nodes[2].Server.Metrics.SyncFailureCount.Count())
	a.Equal(int64(0), nodes[2].Server.Metrics.SyncSuccessCount.Count())
	a.Equal(int64(1), nodes[2].Server.Metrics.OverthrowAttemptCount.Count())
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[2].Server.GRPCAddr,
	)
	// Node 2 is the oracle
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[2].Server.GRPCAddr,
	)

	// Reduce Latency now that Node 2 is oracle. Node 1 had Latency problems
	cluster.Client.Latency = 150 * time.Millisecond
	// Multiple ticks of Node 0 will not overthrow Node 2 as it is trying to
	// become the oracle.  Node 0 does not initialize till Node 2 transitions to
	// INITIALIZED
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[0])
	a.Equal(kronospb.ServerStatus_NOT_INITIALIZED, nodes[0].Server.ServerStatus())

	// Another Tick will initialize Node 2 since it realizes that it is the
	// oracle
	cluster.Tick(nodes[2])
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[2].Server.ServerStatus())

	// Client latencies reduced. Node 0 will not overthrow the oracle
	cluster.Client.Latency = 40 * time.Millisecond
	cluster.Tick(nodes[0])
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[0].Server.ServerStatus())
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[2].Server.GRPCAddr,
	)

	// Increase Client Latency again. Node 0 will overthrow Node 2 after three
	// errors.
	cluster.Client.Latency = 300 * time.Millisecond
	a.Equal(int64(0), nodes[0].Server.Metrics.OverthrowAttemptCount.Count())
	cluster.TickN(nodes[0], 3)
	a.Equal(int64(1), nodes[0].Server.Metrics.OverthrowAttemptCount.Count())
	a.Equal(kronospb.ServerStatus_INITIALIZED, nodes[0].Server.ServerStatus())
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[0].Server.GRPCAddr,
	)
	a.InDelta(
		int64(300*time.Millisecond),
		nodes[0].Server.Metrics.RTT.Snapshot().Max(),
		float64(10*time.Millisecond),
	)

	cluster.Client.Latency = 30 * time.Millisecond
	cluster.TickN(nodes[2], 100)
	a.InDelta(
		int64(36*time.Millisecond),
		nodes[2].Server.Metrics.RTT.Snapshot().Mean(),
		float64(10*time.Millisecond),
	)
	a.Equal(int64(100), nodes[2].Server.Metrics.SyncSuccessCount.Count())

	// Set high latency. Make node 2 overthrow the oracle again.
	cluster.Client.Latency = 250 * time.Millisecond
	cluster.TickN(nodes[2], 3)
	a.Equal(int64(6), nodes[2].Server.Metrics.SyncFailureCount.Count())
	a.Equal(int64(100), nodes[2].Server.Metrics.SyncSuccessCount.Count())
	a.Equal(int64(2), nodes[2].Server.Metrics.OverthrowAttemptCount.Count())
}

func TestMultiNodeClusterSync(t *testing.T) {
	// Detect leaked goroutines after test
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()

	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	cluster.Tick(nodes[0])

	// Cluster should be in sync initially
	err := cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NoError(err)

	// Cluster should be in sync as all Nodes advanced by same amount
	nodes[0].Clock.AdvanceTime(1 * time.Second)
	nodes[1].Clock.AdvanceTime(1 * time.Second)
	nodes[2].Clock.AdvanceTime(1 * time.Second)
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NoError(err)

	// Cluster should not be in sync as all Nodes advanced by different amounts
	nodes[0].Clock.AdvanceTime(1 * time.Second)
	nodes[1].Clock.AdvanceTime(3 * time.Second)
	nodes[2].Clock.AdvanceTime(2 * time.Second)
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NotNil(err)

	// Cluster should be in sync after ticks (delta re-computation)
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NoError(err)
}

func TestMultiNodeExceedsTimeCap(t *testing.T) {
	// Detect leaked goroutines after test
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()

	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	cluster.Tick(nodes[0])

	_, err := nodes[0].Server.KronosTimeNow(ctx)
	a.NoError(err)
	_, err = nodes[1].Server.KronosTimeNow(ctx)
	a.NoError(err)
	_, err = nodes[2].Server.KronosTimeNow(ctx)
	a.NoError(err)

	nodes[0].Clock.AdvanceTime(16 * time.Second)
	nodes[1].Clock.AdvanceTime(16 * time.Second)
	nodes[2].Clock.AdvanceTime(16 * time.Second)

	// Kronos Time should exceed time cap
	_, err = nodes[0].Server.KronosTimeNow(ctx)
	a.Error(err)
	_, err = nodes[1].Server.KronosTimeNow(ctx)
	a.Error(err)
	_, err = nodes[2].Server.KronosTimeNow(ctx)
	a.Error(err)

	// After oracle persists new time cap Kronos Time should not error out
	cluster.Tick(nodes[1])
	_, err = nodes[0].Server.KronosTimeNow(ctx)
	a.NoError(err)
	_, err = nodes[1].Server.KronosTimeNow(ctx)
	a.NoError(err)
	_, err = nodes[2].Server.KronosTimeNow(ctx)
	a.NoError(err)
}

func TestMultiNodeOracleOverthrow(t *testing.T) {
	// Detect leaked goroutines after test
	defer leaktest.AfterTest(t)()
	a := assert.New(t)
	ctx := context.TODO()
	cluster, nodes := mock.InitializeCluster(a, 3)
	defer cluster.Stop()

	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	cluster.Tick(nodes[0])

	// Node 1 should be oracle since it ticked first and respond to Time()
	a.Equal(
		cluster.StateMachine.State(ctx).Oracle,
		nodes[1].Server.GRPCAddr,
	)
	resp, err := nodes[1].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.NoError(err)
	a.True(resp.Time > 0)
	// Node 0 and Node 2 should not respond to time as they are not oracle
	_, err = nodes[0].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.Error(err)
	_, err = nodes[2].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.Error(err)
	err = cluster.IsClusterInSync(ctx, nodes[0], nodes[1], nodes[2])
	a.NoError(err)

	nodes[0].Clock.AdvanceTime(1 * time.Second)
	nodes[1].Clock.AdvanceTime(1 * time.Second)
	nodes[2].Clock.AdvanceTime(1 * time.Second)

	// Stop Node 1. Node 0 should become oracle after 3 ticks.
	cluster.StopNode(ctx, nodes[1])
	cluster.TickN(nodes[0], 3)
	resp, err = nodes[0].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.NoError(err)
	a.True(resp.Time > 0)
	// Node 2 should not respond to time as it is not oracle
	_, err = nodes[2].Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	a.Error(err)
}

// TestKronosTime validates a Node querying another Node for KronosTimeWithRetries
// More elaborate testing of Kronos servers is done in server_mock.go
func TestKronosTimeWithRetries(t *testing.T) {
	a := assert.New(t)
	ctx := context.TODO()

	cluster, nodes := mock.InitializeCluster(a, 3)
	// Time Server needs 2 ticks to initialize
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[0])
	cluster.Tick(nodes[1])
	cluster.Tick(nodes[2])
	nodes[0].Clock.AdvanceTime(time.Second)
	nodes[1].Clock.AdvanceTime(1 * time.Second)
	nodes[2].Clock.AdvanceTime(2 * time.Second)

	// Node 2 queries Node 0 for kronos time
	kronosTime20, err := nodes[2].Server.Client.OracleTime(
		ctx,
		nodes[0].Server.GRPCAddr,
	)
	a.NoError(err)
	kt, err := nodes[0].Server.KronosTimeNow(ctx)
	kronosTime0 := kt.Time
	a.NoError(err)
	// Verify kronos time is of Node 0
	a.Equal(kronosTime0, kronosTime20.Time)
	kt, err = nodes[2].Server.KronosTimeNow(ctx)
	node2Time := kt.Time
	a.NoError(err)
	a.NotEqual(kronosTime20.Time, node2Time)

	// Advance Node 0 Clock and query kronos time again
	nodes[0].Clock.SetTime(int64(2*time.Hour + 5*time.Second))
	// Persist new time cap
	cluster.Tick(nodes[0])
	newKronosTime12, err := nodes[2].Server.Client.OracleTime(
		ctx,
		nodes[0].Server.GRPCAddr,
	)
	a.NoError(err)
	kt, err = nodes[0].Server.KronosTimeNow(ctx)
	newKronosTime2 := kt.Time
	a.NoError(err)
	a.Equal(newKronosTime12.Time, newKronosTime2)
	// Verify kronos time is not of Node 1
	kt, err = nodes[2].Server.KronosTimeNow(ctx)
	node2Time = kt.Time
	a.NoError(err)
	a.NotEqual(newKronosTime2, node2Time)
	a.True(newKronosTime2 > kronosTime0)

	// Querying Node 1 should give an error since it is not the oracle
	_, err = nodes[2].Server.Client.OracleTime(
		ctx,
		nodes[1].Server.GRPCAddr,
	)
	a.Error(err)
}
