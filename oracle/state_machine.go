package oracle

import (
	"context"

	"github.com/rubrikinc/kronos/pb"
)

// StateMachine is used for managing current oracle and time cap
type StateMachine interface {
	// State returns a snapshot of the current state of the state machine.
	State(ctx context.Context) *kronospb.OracleState
	// SubmitProposal submits a proposal to update the StateMachine.
	// This function does not return anything as the proposal is async, it may get
	// applied at some point in the future, or it may get rejected as well.
	SubmitProposal(ctx context.Context, proposal *kronospb.OracleProposal)
	// Close performs any necessary cleanups.
	Close()
}
