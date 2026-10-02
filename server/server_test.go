package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/rubrikinc/kronos/kronosstats"
	"github.com/rubrikinc/kronos/oracle"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
	"github.com/rubrikinc/kronos/tm"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestOracleTime(t *testing.T) {
	clock := tm.NewManualClock()
	clock.SetTime(101)
	clock.SetUptime(51)
	const delta = 49
	const uptimeDelta = 20
	const expectedKronosTime = 150
	const expectedKronosUptime = 71
	localGRPCAddr := &kronospb.NodeAddr{
		Host: "host123",
		Port: "123",
	}

	cases := []struct {
		name             string
		oracleState      *kronospb.OracleState
		serverStatus     kronospb.ServerStatus
		expectedResponse *kronospb.OracleTimeResponse
		expectedErr      error
	}{
		{
			name: "valid response is oracle",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         200,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			expectedResponse: &kronospb.OracleTimeResponse{
				Time:   expectedKronosTime,
				Uptime: expectedKronosUptime,
			},
			expectedErr: nil,
		},
		{
			name: "not oracle",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         200,
				KronosUptimeCap: 210,
				Oracle: &kronospb.NodeAddr{
					Host: "newOracle",
					Port: "123",
				},
			},
			serverStatus:     kronospb.ServerStatus_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`server (host:"host123" port:"123" ) is not oracle, current oracle state:` +
					` id:1 time_cap:200 oracle:<host:"newOracle" port:"123" > kronos_uptime_cap:210 `,
			),
		},
		{
			name: "not intialized",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         200,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus:     kronospb.ServerStatus_NOT_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`kronos server not yet initialized:` +
					` kronos time: 150, status: NOT_INITIALIZED, time cap: 200`,
			),
		},
		{
			name:             "no oracle state",
			oracleState:      &kronospb.OracleState{},
			serverStatus:     kronospb.ServerStatus_NOT_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`server (host:"host123" port:"123" ) is not oracle, current oracle state: `,
			),
		},
		{
			name: "stale time",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         100,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus:     kronospb.ServerStatus_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`kronos time is beyond current time cap, time cap is too stale:` +
					` kronos time: 150, status: INITIALIZED, time cap: 100`,
			),
		},
		{
			name: "stale uptime",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         200,
				KronosUptimeCap: 10,
				Oracle:          localGRPCAddr,
			},
			serverStatus:     kronospb.ServerStatus_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`kronos up time is beyond current time cap, time cap is too stale: ` +
					`kronos uptime: 71, status: INITIALIZED, uptime time cap: 10`,
			),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			a := assert.New(t)
			sm := oracle.NewMemStateMachine()
			proposal := &kronospb.OracleProposal{
				ProposedState: tc.oracleState,
			}
			sm.SubmitProposal(ctx, proposal)
			server := &Server{
				Clock:    clock,
				OracleSM: sm,
				GRPCAddr: localGRPCAddr,
			}
			server.OracleDelta.Store(delta)
			server.OracleUptimeDelta.Store(uptimeDelta)
			server.status.Store(tc.serverStatus)

			timeResponse, err := server.OracleTime(
				ctx,
				&kronospb.OracleTimeRequest{},
			)
			if tc.expectedErr == nil {
				a.NoError(err)
				a.Equal(tc.expectedResponse, timeResponse)
			} else {
				a.Equal(tc.expectedErr.Error(), err.Error())
				a.Nil(timeResponse)
			}
		})
	}
}

func TestKronosTimeNow(t *testing.T) {
	clock := tm.NewManualClock()
	clock.SetTime(101)
	const delta = 1
	localGRPCAddr := &kronospb.NodeAddr{
		Host: "host123",
		Port: "123",
	}

	cases := []struct {
		name         string
		oracleState  *kronospb.OracleState
		serverStatus kronospb.ServerStatus
		physicalTime int64
		expectedTime int64
		expectedErr  error
	}{
		{
			name: "valid time",
			oracleState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         200,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 150,
			expectedTime: 151,
			expectedErr:  nil,
		},
		{
			name: "valid time not oracle",
			oracleState: &kronospb.OracleState{
				Id:              2,
				TimeCap:         201,
				KronosUptimeCap: 210,
				Oracle: &kronospb.NodeAddr{
					Host: "oracle",
					Port: "123",
				},
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 155,
			expectedTime: 156,
			expectedErr:  nil,
		},
		{
			name: "stale time",
			oracleState: &kronospb.OracleState{
				Id:              3,
				TimeCap:         202,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 258,
			expectedTime: 0,
			expectedErr: errors.New(
				`kronos time is beyond current time cap, time cap is too stale:` +
					` kronos time: 259, status: INITIALIZED, time cap: 202`,
			),
		},
		{
			name: "not initialized",
			oracleState: &kronospb.OracleState{
				Id:              4,
				TimeCap:         300,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_NOT_INITIALIZED,
			physicalTime: 259,
			expectedTime: 0,
			expectedErr: errors.New(
				`kronos server not yet initialized:` +
					` kronos time: 260, status: NOT_INITIALIZED, time cap: 300`,
			),
		},
		{
			name: "valid time 2",
			oracleState: &kronospb.OracleState{
				Id:              5,
				TimeCap:         301,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 270,
			expectedTime: 271,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity",
			oracleState: &kronospb.OracleState{
				Id:              6,
				TimeCap:         302,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 240,
			expectedTime: 271,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity corner case",
			oracleState: &kronospb.OracleState{
				Id:              7,
				TimeCap:         303,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 271,
			expectedTime: 272,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity 3",
			oracleState: &kronospb.OracleState{
				Id:              8,
				TimeCap:         305,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 210,
			expectedTime: 272,
			expectedErr:  nil,
		},
		{
			name: "valid time 3",
			oracleState: &kronospb.OracleState{
				Id:              9,
				TimeCap:         307,
				KronosUptimeCap: 210,
				Oracle:          localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 290,
			expectedTime: 291,
			expectedErr:  nil,
		},
	}

	sm := oracle.NewMemStateMachine()
	server := &Server{
		Clock:    clock,
		OracleSM: sm,
		GRPCAddr: localGRPCAddr,
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			a := assert.New(t)
			proposal := &kronospb.OracleProposal{
				ProposedState: tc.oracleState,
			}
			sm.SubmitProposal(ctx, proposal)
			clock.SetTime(tc.physicalTime)
			server.OracleDelta.Store(delta)
			server.status.Store(tc.serverStatus)

			kt, err := server.KronosTimeNow(ctx)
			if tc.expectedErr == nil {
				a.NoError(err)
				a.NotNil(kt)
				tm, cap := kt.Time, kt.TimeCap
				a.Equal(tc.expectedTime, tm)
				a.Equal(tc.oracleState.TimeCap, cap)
			} else {
				a.Equal(tc.expectedErr.Error(), err.Error())
				a.Nil(kt)
			}
		})
	}
}

func TestProposeSelf(t *testing.T) {
	sm := oracle.NewMemStateMachine()
	localGRPCAddr := &kronospb.NodeAddr{
		Host: "host123",
		Port: "123",
	}

	cases := []struct {
		name          string
		physicalTime  int64
		proposalState *kronospb.OracleState
		expectedState *kronospb.OracleState
	}{
		{
			name: "valid proposal 1",
			proposalState: &kronospb.OracleState{
				Id:              0,
				TimeCap:         200,
				KronosUptimeCap: 20,
				Oracle:          localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         int64(160 * time.Second),
				KronosUptimeCap: int64(130 * time.Second),
				Oracle:          localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
		{
			name: "valid proposal 2",
			proposalState: &kronospb.OracleState{
				Id:              1,
				TimeCap:         int64(115 * time.Second),
				KronosUptimeCap: int64(110 * time.Second),
				Oracle:          localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:              2,
				TimeCap:         int64(175 * time.Second),
				KronosUptimeCap: int64(145 * time.Second),
				Oracle:          localGRPCAddr,
			},
			physicalTime: int64(115 * time.Second),
		},
		{
			name: "stale time",
			proposalState: &kronospb.OracleState{
				Id:              2,
				TimeCap:         int64(1000 * time.Second),
				KronosUptimeCap: int64(145 * time.Second),
				Oracle:          localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:              3,
				TimeCap:         int64(1000*time.Second) + 1,
				KronosUptimeCap: int64(145*time.Second) + 1,
				Oracle:          localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
		{
			name: "invalid id",
			proposalState: &kronospb.OracleState{
				Id:              4,
				TimeCap:         int64(1000 * time.Second),
				KronosUptimeCap: int64(145 * time.Second),
				Oracle:          localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:              3,
				TimeCap:         int64(1000*time.Second) + 1,
				KronosUptimeCap: int64(145*time.Second) + 1,
				Oracle:          localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			a := assert.New(t)
			clock := tm.NewManualClock()
			clock.AdvanceTime(time.Duration(tc.physicalTime))
			server := &Server{
				OracleSM:             sm,
				GRPCAddr:             localGRPCAddr,
				Clock:                clock,
				OracleTimeCapDelta:   DefaultOracleTimeCapDelta,
				OracleUptimeCapDelta: DefaultOracleUptimeCapDelta,
			}

			server.proposeSelf(ctx, tc.proposalState)
			a.Equal(tc.expectedState, sm.State(ctx))
		})
	}
}

type simpleMockClient struct {
	response *kronospb.OracleTimeResponse
	err      error
}

func (c *simpleMockClient) Bootstrap(ctx context.Context, server *kronospb.NodeAddr, req *kronospb.BootstrapRequest) (*kronospb.BootstrapResponse, error) {
	return nil, nil
}

func (c *simpleMockClient) KronosTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.KronosTimeResponse, error) {
	return &kronospb.KronosTimeResponse{}, nil
}

func (c *simpleMockClient) KronosUptime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.KronosUptimeResponse, error) {
	return &kronospb.KronosUptimeResponse{}, nil
}

func (c *simpleMockClient) OracleTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.OracleTimeResponse, error) {
	return c.response, c.err
}

func (c *simpleMockClient) Status(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.StatusResponse, error) {
	return nil, nil
}

func (c *simpleMockClient) Close() error {
	return nil
}

var _ Client = &simpleMockClient{}

func TestSyncWithOracle(t *testing.T) {
	const initTime = int64(200)
	cases := []struct {
		name        string
		mockClient  Client
		expectedErr error
		delta       int64
	}{
		{
			name: "rtt low end adjustment",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time:   int64(2 * time.Hour),
					Uptime: int64(2 * time.Hour),
					Rtt:    int64(10 * time.Millisecond),
				},
				err: nil,
			},
			expectedErr: nil,
			delta:       int64(2*time.Hour) - initTime,
		},
		{
			name: "rtt error no adjustment",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time:   -int64(20 * time.Millisecond),
					Uptime: -int64(20 * time.Millisecond),
					Rtt:    int64(100 * time.Millisecond),
				},
				err: nil,
			},
			expectedErr: nil,
			delta:       0,
		},
		{
			name: "rtt high end adjustment",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time:   -int64(20 * time.Millisecond),
					Uptime: -int64(20 * time.Millisecond),
					Rtt:    int64(10 * time.Millisecond),
				},
				err: nil,
			},
			expectedErr: nil,
			delta:       -int64(10*time.Millisecond) - initTime,
		},
		{
			name: "rtt too high",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time:   int64(2 * time.Hour),
					Uptime: int64(2 * time.Hour),
					Rtt:    int64(300 * time.Millisecond),
				},
				err: nil,
			},
			expectedErr: errors.New("rtt too high (more than 200ms): 300ms"),
		},
		{
			name: "server error",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time:   int64(2 * time.Hour),
					Uptime: int64(2 * time.Hour),
					Rtt:    int64(10 * time.Millisecond),
				},
				err: errors.New("test error"),
			},
			expectedErr: errors.New("test error"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			clk := tm.NewManualClock()
			clk.AdvanceTime(time.Duration(initTime))
			server := &Server{
				Client:  tc.mockClient,
				Clock:   clk,
				Metrics: kronosstats.NewTestMetrics(),
			}

			err := server.trySyncWithOracle(
				context.TODO(),
				&kronospb.NodeAddr{
					Host: "oracle",
					Port: "123",
				},
			)
			if tc.expectedErr == nil {
				a.NoError(err)
				a.Equal(tc.delta, server.OracleDelta.Load())
				a.Equal(tc.delta, server.OracleUptimeDelta.Load())
			} else if a.Error(err) {
				a.Equal(tc.expectedErr.Error(), err.Error())
			}
		})
	}
}

func TestOverthrowPolicy(t *testing.T) {
	ctx := context.Background()
	a := assert.New(t)
	c := &simpleMockClient{}
	sm := oracle.NewMemStateMachine()
	nodes := []*kronospb.NodeAddr{
		{Host: "h0", Port: "p0"},
		{Host: "h1", Port: "p1"},
		{Host: "h2", Port: "p2"},
	}
	s := &Server{
		Client:   c,
		Clock:    tm.NewManualClock(),
		OracleSM: sm,
		GRPCAddr: nodes[0],
		Metrics:  kronosstats.NewTestMetrics(),
	}
	sm.SubmitProposal(ctx, &kronospb.OracleProposal{
		ProposedState: &kronospb.OracleState{
			Oracle:  nodes[1],
			TimeCap: int64(1),
			Id:      1,
		},
	})
	c.err = errors.New("not oracle")
	// No overthrow or time adjustment for two errors
	for i := 0; i < 2; i++ {
		a.False(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
		a.Equal(int64(0), s.adjustedTime())
		a.Equal((sm.State(ctx)).Oracle.Host, nodes[1].Host)
	}
	sm.SubmitProposal(ctx, &kronospb.OracleProposal{
		ProposedState: &kronospb.OracleState{
			Oracle:          nodes[2],
			TimeCap:         int64(2),
			KronosUptimeCap: int64(2),
			Id:              2,
		},
	})
	// No overthrow or time adjustment for next two errors because the oracle has
	// changed. We look for three errors on the same oracle.
	for i := 0; i < 2; i++ {
		a.False(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
		a.Equal(int64(0), s.adjustedTime())
		a.Equal(nodes[2].Host, (sm.State(ctx)).Oracle.Host)
	}
	// Return a valid response.
	c.response = &kronospb.OracleTimeResponse{
		Time:   int64(time.Second),
		Uptime: int64(time.Second),
		Rtt:    int64(time.Millisecond),
	}
	c.err = nil
	a.True(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
	a.Equal(int64(time.Second), s.adjustedTime())
	c.response = nil
	c.err = errors.New("some other error")
	// No overthrow for next two errors even with the same oracle because there
	// was a successful response.
	for i := 0; i < 2; i++ {
		a.False(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
		a.Equal(int64(time.Second), s.adjustedTime())
		a.Equal((sm.State(ctx)).Oracle.Host, nodes[2].Host)
	}
	// Overthrow on the third error.
	a.False(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
	a.Equal(int64(time.Second), s.adjustedTime())
	a.Equal(nodes[0].Host, (sm.State(ctx)).Oracle.Host)
	// Make node 2 the oracle again.
	sm.SubmitProposal(ctx, &kronospb.OracleProposal{
		ProposedState: &kronospb.OracleState{
			Oracle:          nodes[2],
			TimeCap:         int64(time.Hour),
			KronosUptimeCap: int64(time.Hour),
			Id:              4,
		},
	})
	// No overthrow for next two errors because we proposed self as the oracle one
	// tick back.
	for i := 0; i < 2; i++ {
		a.False(s.syncOrOverthrowOracle(ctx, sm.State(ctx)))
		a.Equal(int64(time.Second), s.adjustedTime())
		a.Equal(nodes[2].Host, (sm.State(ctx)).Oracle.Host)
	}
}

func TestNodeAddrEqual(t *testing.T) {
	addr := func(h, p string) *kronospb.NodeAddr { return &kronospb.NodeAddr{Host: h, Port: p} }
	cases := []struct {
		name string
		a, b *kronospb.NodeAddr
		want bool
	}{
		{"both nil", nil, nil, true},
		{"a nil", nil, addr("h", "1"), false},
		{"b nil", addr("h", "1"), nil, false},
		{"equal", addr("h", "1"), addr("h", "1"), true},
		{"host differs", addr("h1", "1"), addr("h2", "1"), false},
		{"port differs", addr("h", "1"), addr("h", "2"), false},
		{"both empty", addr("", ""), addr("", ""), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, nodeAddrEqual(tc.a, tc.b))
			// Sanity: our helper must agree with proto.Equal for the shapes
			// NodeAddr can take.
			assert.Equal(t, proto.Equal(tc.a, tc.b), nodeAddrEqual(tc.a, tc.b))
		})
	}
}

// newBenchServer builds a Server wired up for the KronosTimeNow /
// KronosUptime happy path: initialized status, valid time cap, valid uptime
// cap, self as oracle. Used by BenchmarkKronosTimeNow* to measure the hot
// path CockroachDB HLC reads travel through.
func newBenchServer(b testing.TB) *Server {
	b.Helper()
	clock := tm.NewManualClock()
	clock.SetTime(100)
	clock.SetUptime(100)
	local := &kronospb.NodeAddr{Host: "127.0.0.1", Port: "5766"}
	sm := oracle.NewMemStateMachine()
	sm.SubmitProposal(context.Background(), &kronospb.OracleProposal{
		ProposedState: &kronospb.OracleState{
			Id:              1,
			TimeCap:         int64(time.Hour),
			KronosUptimeCap: int64(time.Hour),
			Oracle:          local,
		},
	})
	s := &Server{
		Clock:    clock,
		OracleSM: sm,
		GRPCAddr: local,
	}
	s.status.Store(kronospb.ServerStatus_INITIALIZED)
	return s
}

// BenchmarkKronosTimeNow documents the CDM-552295 hot path: every
// CockroachDB HLC clock read reaches this function via kronos.GetTime.
// The proto variant allocates one KronosTimeResponse per call; the raw
// variant returns primitives and reports 0 allocs/op.
func BenchmarkKronosTimeNow(b *testing.B) {
	ctx := context.Background()
	s := newBenchServer(b)
	b.Run("proto", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = s.KronosTimeNow(ctx)
			}
		})
	})
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = s.KronosTimeNowRaw(ctx)
			}
		})
	})
}

func BenchmarkKronosUptime(b *testing.B) {
	ctx := context.Background()
	s := newBenchServer(b)
	b.Run("proto", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = s.KronosUptimeNow(ctx)
			}
		})
	})
	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _, _ = s.KronosUptimeNowRaw(ctx)
			}
		})
	})
}

// BenchmarkNodeAddrEqual documents the alloc/CPU savings for CDM-552295: the
// hand-written comparison runs at reflection-free speed and reports 0
// allocs/op, whereas gogo/proto.Equal — used by every isOracle call on the
// KronosTimeNow hot path — allocates on each invocation.
func BenchmarkNodeAddrEqual(b *testing.B) {
	a := &kronospb.NodeAddr{Host: "127.0.0.1", Port: "5766"}
	c := &kronospb.NodeAddr{Host: "127.0.0.1", Port: "5766"}
	b.Run("nodeAddrEqual", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = nodeAddrEqual(a, c)
		}
	})
	b.Run("proto.Equal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = proto.Equal(a, c)
		}
	})
}

// notTheOracle points the server away from the oracle in its state machine, so
// KronosTimeNowRaw stops refreshing TimeCap/UptimeCap from the Raft state and
// the caps can be driven directly.
func notTheOracle(s *Server) {
	s.GRPCAddr = &kronospb.NodeAddr{Host: "127.0.0.1", Port: "9999"}
}

// TestKronosTimeNowRawCauses pins two things per failure state: the wrapped
// sentinel, which callers classify on, and the message text, which CockroachDB
// prints verbatim in its stall fatal.
func TestKronosTimeNowRawCauses(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name      string
		setup     func(*Server)
		cause     error
		msgPrefix string
		wantStack bool
	}{
		{
			name:      "not initialized",
			setup:     func(s *Server) { s.status.Store(kronospb.ServerStatus_NOT_INITIALIZED) },
			cause:     ErrNotInitialized,
			msgPrefix: "kronos server not yet initialized: kronos time: ",
			wantStack: true,
		},
		{
			name:      "time cap not initialized",
			setup:     notTheOracle,
			cause:     ErrTimeCapNotInited,
			msgPrefix: "kronos time cap not yet initialized: kronos time: ",
			wantStack: true,
		},
		{
			name: "time cap stale",
			setup: func(s *Server) {
				notTheOracle(s)
				s.TimeCap.Store(50) // clock is at 100
			},
			cause:     ErrTimeCapStale,
			msgPrefix: "kronos time is beyond current time cap, time cap is too stale: kronos time: ",
			wantStack: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			s := newBenchServer(t)
			tc.setup(s)

			_, _, err := s.KronosTimeNowRaw(ctx)
			a.Error(err)
			a.True(errors.Is(err, tc.cause), "want %v, got %v", tc.cause, err)
			a.True(strings.HasPrefix(err.Error(), tc.msgPrefix), "got %q", err.Error())

			// %+v renders a stack when one was captured; without it the two
			// formats are identical.
			hasStack := fmt.Sprintf("%+v", err) != fmt.Sprintf("%v", err)
			a.Equal(tc.wantStack, hasStack)
		})
	}
}

// TestKronosUptimeNowRawCauses is the uptime twin of TestKronosTimeNowRawCauses.
func TestKronosUptimeNowRawCauses(t *testing.T) {
	ctx := context.Background()
	a := assert.New(t)

	s := newBenchServer(t)
	notTheOracle(s)
	_, _, err := s.KronosUptimeNowRaw(ctx)
	a.True(errors.Is(err, ErrUptimeCapNotInited), "got %v", err)

	s.UptimeCap.Store(50) // clock uptime is at 100
	_, _, err = s.KronosUptimeNowRaw(ctx)
	a.True(errors.Is(err, ErrUptimeCapStale), "got %v", err)
	a.True(strings.HasPrefix(err.Error(),
		"kronos up time is beyond current time cap, time cap is too stale: kronos uptime: "),
		"got %q", err.Error())
}

// BenchmarkKronosTimeNowRawFailure prices the two error paths: the stale cause
// skips the stack capture, which is the bulk of the cost. The stale path is the
// one that runs thousands of times a second during a stall.
func BenchmarkKronosTimeNowRawFailure(b *testing.B) {
	ctx := context.Background()
	b.Run("stale_no_stack", func(b *testing.B) {
		s := newBenchServer(b)
		notTheOracle(s)
		s.TimeCap.Store(50)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = s.KronosTimeNowRaw(ctx)
		}
	})
	b.Run("notinit_with_stack", func(b *testing.B) {
		s := newBenchServer(b)
		notTheOracle(s)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = s.KronosTimeNowRaw(ctx)
		}
	})
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

// proposalFrom returns a marshalled OracleProposal naming oracle as the proposer.
func proposalFrom(t *testing.T, oracle *kronospb.NodeAddr) []byte {
	t.Helper()
	b, err := protoutil.Marshal(&kronospb.OracleProposal{ProposedState: &kronospb.OracleState{
		Id: 2, TimeCap: int64(2 * time.Hour), Oracle: oracle,
	}})
	assert.NoError(t, err)
	return b
}

// TestProposalFilter covers a node proposing itself as oracle: refused while the
// current oracle is healthy, accepted once it has failed numConsecutiveErrsForOverthrow
// syncs. Both messages must name the oracles as host:port, not proto text.
func TestProposalFilter(t *testing.T) {
	ctx := context.Background()
	proposer := &kronospb.NodeAddr{Host: "127.0.0.2", Port: "5766"}

	t.Run("own proposal from the current oracle passes", func(t *testing.T) {
		s := newBenchServer(t) // the oracle in its state machine is 127.0.0.1:5766
		current := s.OracleSM.State(ctx).Oracle
		assert.NoError(t, s.proposalFilter(ctx, proposalFrom(t, current)))
	})

	t.Run("refused while the oracle is healthy", func(t *testing.T) {
		s := newBenchServer(t)
		notTheOracle(s)
		err := s.proposalFilter(ctx, proposalFrom(t, proposer))
		assert.EqualError(t, err,
			"cannot accept proposal from non-oracle 127.0.0.2:5766 since oracle 127.0.0.1:5766 is active")
	})

	t.Run("accepted once the oracle has failed repeatedly", func(t *testing.T) {
		a := assert.New(t)
		stop := captureLogs(t)
		s := newBenchServer(t)
		notTheOracle(s)
		current := s.OracleSM.State(ctx).Oracle
		for i := range s.oracleSyncErrs {
			s.oracleSyncErrs[i].oracle = current
			s.oracleSyncErrs[i].err = errors.New("boom")
		}
		s.syncedWithOracleAtleastOnce.Store(true)

		a.NoError(s.proposalFilter(ctx, proposalFrom(t, proposer)))
		out := stop()
		a.Contains(out, "Eligible to overthrow oracle due to 3 consecutive errors on the same "+
			"oracle 127.0.0.1:5766, errs: [boom; boom; boom]")
		a.Contains(out, "Accepting proposal from non-oracle 127.0.0.2:5766 since current "+
			"oracle 127.0.0.1:5766 is down")
	})
}

// jitterClock mostly moves forward but regularly steps backward, so a served
// time that goes backward means the clamp failed, not that the clock did.
type jitterClock struct{ n atomic.Int64 }

func (c *jitterClock) Now() int64    { return c.n.Add(100) - rand.Int63n(5000) + 1_000_000 }
func (c *jitterClock) Uptime() int64 { return c.Now() }

// maxSeen tracks the largest value reported so far. It uses a mutex, not
// advance, so the test does not rely on the code it checks.
type maxSeen struct {
	mu sync.Mutex
	v  int64
}

func (m *maxSeen) get() int64 { m.mu.Lock(); defer m.mu.Unlock(); return m.v }
func (m *maxSeen) put(x int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if x > m.v {
		m.v = x
	}
}

// TestClockReadsNeverGoBackward reads time and uptime from many goroutines over
// a clock that steps backward. A call that starts after another finished must
// not return less than it, and no goroutine may see its own readings decrease.
func TestClockReadsNeverGoBackward(t *testing.T) {
	// A lost update only shows when callers truly run in parallel, so do not
	// let a low GOMAXPROCS (-cpu 1) turn this into a vacuous pass.
	if prev := runtime.GOMAXPROCS(0); prev < 4 {
		runtime.GOMAXPROCS(4)
		defer runtime.GOMAXPROCS(prev)
	}
	ctx := context.Background()
	s := newBenchServer(t)
	s.Clock = &jitterClock{}

	var maxT, maxU maxSeen
	var violations, failures atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ownT, ownU int64
			for i := 0; i < 4000; i++ {
				floorT, floorU := maxT.get(), maxU.get()
				tv, _, errT := s.KronosTimeNowRaw(ctx)
				uv, _, errU := s.KronosUptimeNowRaw(ctx)
				if errT != nil || errU != nil {
					failures.Inc()
					continue
				}
				if tv < ownT || tv < floorT || uv < ownU || uv < floorU {
					violations.Inc()
				}
				ownT, ownU = tv, uv
				maxT.put(tv)
				maxU.put(uv)
			}
		}()
	}
	wg.Wait()
	assert.Zero(t, failures.Load(), "clock reads failed")
	assert.Zero(t, violations.Load(), "served time went backward")
}

func TestAdvance(t *testing.T) {
	var last atomic.Int64
	a := assert.New(t)
	a.EqualValues(5, advance(&last, 5)) // the first value is recorded
	a.EqualValues(5, advance(&last, 3)) // an older reading is raised to what was served
	a.EqualValues(5, last.Load())       // and does not lower the record
	a.EqualValues(9, advance(&last, 9))
	a.EqualValues(9, last.Load())
}

// BenchmarkKronosClockReadAdvancing reads through the production clock, which
// moves on every call, so each read also publishes a new last-served value.
// The frozen ManualClock in BenchmarkKronosTimeNow hides that write, which is
// where the contention is.
func BenchmarkKronosClockReadAdvancing(b *testing.B) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		read func(*Server)
	}{
		{"time", func(s *Server) { _, _, _ = s.KronosTimeNowRaw(ctx) }},
		{"uptime", func(s *Server) { _, _, _ = s.KronosUptimeNowRaw(ctx) }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s := newBenchServer(b)
			s.Clock = tm.NewMonotonicClock()
			// Caps far enough ahead for reads to succeed against the real clock.
			s.OracleSM.SubmitProposal(ctx, &kronospb.OracleProposal{ProposedState: &kronospb.OracleState{
				Id: 2, TimeCap: math.MaxInt64 / 2, KronosUptimeCap: math.MaxInt64 / 2, Oracle: s.GRPCAddr,
			}})
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tc.read(s)
				}
			})
		})
	}
}
