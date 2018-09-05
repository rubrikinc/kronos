package oracle

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/pb"
)

func TestSubmitProposal(t *testing.T) {
	cases := []struct {
		name     string
		state    *kronospb.OracleState
		proposal *kronospb.OracleProposal
		// set expectedState to nil if transition is not expected
		expectedState *kronospb.OracleState
	}{
		{
			name: "overthrow oracle",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      101,
					TimeCap: 30,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: &kronospb.OracleState{
				Id:      101,
				TimeCap: 30,
				Oracle: &kronospb.NodeAddr{
					Host: "B",
					Port: "1234",
				},
			},
		},
		{
			name: "lower time cap",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      101,
					TimeCap: 19,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: nil,
		},
		{
			name: "extend oracle",
			state: &kronospb.OracleState{
				Id:      109,
				TimeCap: 10,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      110,
					TimeCap: 20,
					Oracle: &kronospb.NodeAddr{
						Host: "A",
						Port: "123",
					},
				},
			},
			expectedState: &kronospb.OracleState{
				Id:      110,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
		},
		{
			name:  "no initial state",
			state: nil,
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      1,
					TimeCap: 35,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: &kronospb.OracleState{
				Id:      1,
				TimeCap: 35,
				Oracle: &kronospb.NodeAddr{
					Host: "B",
					Port: "1234",
				},
			},
		},
		{
			name: "higher ID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      102,
					TimeCap: 30,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: nil,
		},
		{
			name: "equal PrevID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      100,
					TimeCap: 30,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: nil,
		},
		{
			name: "lower PrevID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      99,
					TimeCap: 30,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: nil,
		},
		{
			name:  "no initial state wrong PrevID",
			state: nil,
			proposal: &kronospb.OracleProposal{
				ProposedState: &kronospb.OracleState{
					Id:      6,
					TimeCap: 35,
					Oracle: &kronospb.NodeAddr{
						Host: "B",
						Port: "1234",
					},
				},
			},
			expectedState: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			ctx := context.TODO()
			stateMachine := NewMemStateMachine().(*inMemStateMachine)
			defer stateMachine.Close()
			if tc.state != nil {
				stateMachine.state = tc.state
			}
			initialState := protoutil.Clone(stateMachine.State(ctx))
			stateMachine.SubmitProposal(ctx, tc.proposal)
			if tc.expectedState != nil {
				a.Equal(tc.expectedState, stateMachine.State(ctx))
			} else {
				a.Equal(initialState, stateMachine.State(ctx))
			}
		})
	}
}

func TestProposeState(t *testing.T) {
	cases := []struct {
		name             string
		state            *kronospb.OracleState
		proposedState    *kronospb.OracleState
		expectTransition bool
	}{
		{
			name: "higher PrevID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposedState: &kronospb.OracleState{
				Id:      101,
				TimeCap: 30,
				Oracle: &kronospb.NodeAddr{
					Host: "B",
					Port: "1234",
				},
			},
			expectTransition: true,
		},
		{
			name: "lower PrevID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposedState: &kronospb.OracleState{
				Id:      99,
				TimeCap: 30,
				Oracle: &kronospb.NodeAddr{
					Host: "B",
					Port: "1234",
				},
			},
			expectTransition: false,
		},
		{
			name: "equal PrevID",
			state: &kronospb.OracleState{
				Id:      100,
				TimeCap: 20,
				Oracle: &kronospb.NodeAddr{
					Host: "A",
					Port: "123",
				},
			},
			proposedState: &kronospb.OracleState{
				Id:      99,
				TimeCap: 30,
				Oracle: &kronospb.NodeAddr{
					Host: "B",
					Port: "1234",
				},
			},
			expectTransition: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			ctx := context.TODO()
			stateMachine := NewMemStateMachine().(*inMemStateMachine)
			defer stateMachine.Close()
			if tc.state != nil {
				stateMachine.state = tc.state
			}
			initialState := protoutil.Clone(stateMachine.State(ctx))
			stateMachine.restoreState(*tc.proposedState)
			if tc.expectTransition {
				a.Equal(tc.proposedState, stateMachine.State(ctx))
			} else {
				a.Equal(initialState, stateMachine.State(ctx))
			}
		})
	}
}
