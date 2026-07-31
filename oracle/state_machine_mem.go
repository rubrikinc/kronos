package oracle

import (
	"context"
	"sync/atomic"

	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/syncutil"
)

// inMemStateMachine is an in-memory implementation of StateMachine.
//
// state is published atomically so State() can return a shared pointer
// without locking or cloning. Writers serialize on the embedded RWMutex to
// protect the compare-then-swap validation, but always publish a fresh
// *OracleState via state.Store — the pointee is never mutated in place.
// Callers of State() must treat the returned proto as immutable.
type inMemStateMachine struct {
	syncutil.RWMutex
	state atomic.Pointer[kronospb.OracleState]
}

var _ StateMachine = &inMemStateMachine{}

// State returns an immutable snapshot of the current state of the state
// machine. The returned proto must not be mutated by the caller.
func (s *inMemStateMachine) State(ctx context.Context) *kronospb.OracleState {
	return s.state.Load()
}

// SubmitProposal submits a new proposal to the StateMachine. The state machine
// accepts the proposal only if the proposed ID sequences immediately after the
// current ID, and the proposed time cap is more than the current time cap.
func (s *inMemStateMachine) SubmitProposal(ctx context.Context, proposal *kronospb.OracleProposal) {
	s.Lock()
	defer s.Unlock()
	cur := s.state.Load()
	if proposal.ProposedState == nil {
		log.Warningf(
			ctx,
			"Ignoring unexpected proposal: proposed state is nil, current state: %v, proposal: %v",
			cur, proposal,
		)
		return
	}
	newState := proposal.ProposedState
	// Only accept proposals that are sequenced immediately after the current
	// state. This is so that if multiple entities decide to make a proposal based
	// on their reading of the current state, only one of them succeeds and we do
	// not observe state thrashing. This is not completely secure because the
	// proposers can pretend to be anywhere in the sequence, we would probably be
	// better off enforcing by matching uuids. But in our application, proposers
	// are not adversarial, and they always set the next ID to one more than what
	// they see.
	if newState.Id != cur.Id+1 {
		log.Infof(
			ctx,
			"Proposal rejected because it does not sequence immediately after the current ID,"+
				" current state: %v, proposal: %v",
			cur, proposal,
		)
		return
	}
	// Ensure monotonicity
	if newState.TimeCap <= cur.TimeCap {
		log.Infof(
			ctx,
			"Proposal rejected because the proposed time cap is not more than the current time cap,"+
				" current state: %v, proposal: %v",
			cur, proposal,
		)
		return
	}
	s.state.Store(&kronospb.OracleState{
		Oracle:          newState.Oracle,
		TimeCap:         newState.TimeCap,
		Id:              newState.Id,
		KronosUptimeCap: newState.KronosUptimeCap,
	})
}

// restoreState restores the StateMachine to the given state
func (s *inMemStateMachine) restoreState(state kronospb.OracleState) {
	s.Lock()
	defer s.Unlock()
	cur := s.state.Load()
	if cur == nil || state.Id > cur.Id {
		s.state.Store(&state)
	}
}

// Close does not need to do anything. It is required for inMemStateMachine to
// implement StateMachine
func (s *inMemStateMachine) Close() {}

// NewMemStateMachine returns an instance of in memory oracle state machine
func NewMemStateMachine() StateMachine {
	memStateMachine := &inMemStateMachine{}
	memStateMachine.state.Store(&kronospb.OracleState{})
	return memStateMachine
}
