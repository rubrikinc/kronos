package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/kronosstats"
	"github.com/scaledata/kronos/oracle"
	"github.com/scaledata/kronos/pb"
	"github.com/scaledata/kronos/tm"
)

func TestOracleTime(t *testing.T) {
	clock := tm.NewManualClock()
	clock.SetTime(101)
	const delta = 49
	const expectedKronosTime = 150
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
				Id:      1,
				TimeCap: 200,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			expectedResponse: &kronospb.OracleTimeResponse{
				Time: expectedKronosTime,
			},
			expectedErr: nil,
		},
		{
			name: "not oracle",
			oracleState: &kronospb.OracleState{
				Id:      1,
				TimeCap: 200,
				Oracle: &kronospb.NodeAddr{
					Host: "newOracle",
					Port: "123",
				},
			},
			serverStatus:     kronospb.ServerStatus_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`server (host:"host123" port:"123" ) is not oracle, current oracle state:` +
					` id:1 time_cap:200 oracle:<host:"newOracle" port:"123" > `,
			),
		},
		{
			name: "not intialized",
			oracleState: &kronospb.OracleState{
				Id:      1,
				TimeCap: 200,
				Oracle:  localGRPCAddr,
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
				Id:      1,
				TimeCap: 100,
				Oracle:  localGRPCAddr,
			},
			serverStatus:     kronospb.ServerStatus_INITIALIZED,
			expectedResponse: nil,
			expectedErr: errors.New(
				`kronos time is beyond current time cap, time cap is too stale:` +
					` kronos time: 150, status: INITIALIZED, time cap: 100`,
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
				Id:      1,
				TimeCap: 200,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 150,
			expectedTime: 151,
			expectedErr:  nil,
		},
		{
			name: "valid time not oracle",
			oracleState: &kronospb.OracleState{
				Id:      2,
				TimeCap: 201,
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
				Id:      3,
				TimeCap: 202,
				Oracle:  localGRPCAddr,
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
				Id:      4,
				TimeCap: 300,
				Oracle:  localGRPCAddr,
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
				Id:      5,
				TimeCap: 301,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 270,
			expectedTime: 271,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity",
			oracleState: &kronospb.OracleState{
				Id:      6,
				TimeCap: 302,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 240,
			expectedTime: 271,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity corner case",
			oracleState: &kronospb.OracleState{
				Id:      7,
				TimeCap: 303,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 271,
			expectedTime: 272,
			expectedErr:  nil,
		},
		{
			name: "ensure monotonicity 3",
			oracleState: &kronospb.OracleState{
				Id:      8,
				TimeCap: 305,
				Oracle:  localGRPCAddr,
			},
			serverStatus: kronospb.ServerStatus_INITIALIZED,
			physicalTime: 210,
			expectedTime: 272,
			expectedErr:  nil,
		},
		{
			name: "valid time 3",
			oracleState: &kronospb.OracleState{
				Id:      9,
				TimeCap: 307,
				Oracle:  localGRPCAddr,
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
				Id:      0,
				TimeCap: 200,
				Oracle:  localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:      1,
				TimeCap: int64(160 * time.Second),
				Oracle:  localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
		{
			name: "valid proposal 2",
			proposalState: &kronospb.OracleState{
				Id:      1,
				TimeCap: int64(115 * time.Second),
				Oracle:  localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:      2,
				TimeCap: int64(175 * time.Second),
				Oracle:  localGRPCAddr,
			},
			physicalTime: int64(115 * time.Second),
		},
		{
			name: "stale time",
			proposalState: &kronospb.OracleState{
				Id:      2,
				TimeCap: int64(1000 * time.Second),
				Oracle:  localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:      3,
				TimeCap: int64(1000*time.Second) + 1,
				Oracle:  localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
		{
			name: "invalid id",
			proposalState: &kronospb.OracleState{
				Id:      4,
				TimeCap: int64(1000 * time.Second),
				Oracle:  localGRPCAddr,
			},
			expectedState: &kronospb.OracleState{
				Id:      3,
				TimeCap: int64(1000*time.Second) + 1,
				Oracle:  localGRPCAddr,
			},
			physicalTime: int64(100 * time.Second),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.TODO()
			a := assert.New(t)
			clock := tm.NewManualClock()
			clock.SetTime(tc.physicalTime)
			server := &Server{
				OracleSM:           sm,
				GRPCAddr:           localGRPCAddr,
				Clock:              clock,
				OracleTimeCapDelta: DefaultOracleTimeCapDelta,
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

func (c *simpleMockClient) KronosTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.KronosTimeResponse, error) {
	return &kronospb.KronosTimeResponse{}, nil
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
					Time: int64(2 * time.Hour),
					Rtt:  int64(10 * time.Millisecond),
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
					Time: -int64(20 * time.Millisecond),
					Rtt:  int64(100 * time.Millisecond),
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
					Time: -int64(20 * time.Millisecond),
					Rtt:  int64(10 * time.Millisecond),
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
					Time: int64(2 * time.Hour),
					Rtt:  int64(300 * time.Millisecond),
				},
				err: nil,
			},
			expectedErr: errors.New("rtt too high (more than 200ms): 300ms"),
		},
		{
			name: "server error",
			mockClient: &simpleMockClient{
				response: &kronospb.OracleTimeResponse{
					Time: int64(2 * time.Hour),
					Rtt:  int64(10 * time.Millisecond),
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
			clk.SetTime(initTime)
			server := &Server{
				Client:  tc.mockClient,
				Clock:   clk,
				Metrics: kronosstats.NewMetrics(),
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
		{"h0", "p0"},
		{"h1", "p1"},
		{"h2", "p2"},
	}
	s := &Server{
		Client:   c,
		Clock:    tm.NewManualClock(),
		OracleSM: sm,
		GRPCAddr: nodes[0],
		Metrics:  kronosstats.NewMetrics(),
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
			Oracle:  nodes[2],
			TimeCap: int64(2),
			Id:      2,
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
		Time: int64(time.Second),
		Rtt:  int64(time.Millisecond),
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
			Oracle:  nodes[2],
			TimeCap: int64(time.Hour),
			Id:      4,
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
