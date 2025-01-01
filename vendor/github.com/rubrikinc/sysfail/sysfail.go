/*
 * Copyright © 2024 Rubrik, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sysfail

/*
#cgo CFLAGS: -I../../include
#cgo LDFLAGS: -L../../build/src -lsysfail

#include "sysfail.h"
#include <stdlib.h>
#include <sys/stat.h>

#ifndef SYSFAIL_GO_H
#define SYSFAIL_GO_H

int Selector(void* ctx, sysfail_tid_t tid);

static int RegularFileOtherThanStdInOutErr(void* ctx, const greg_t* regs) {
	// This function assumes that the first argument to the syscall is the file descriptor
	int fd = sysfail_syscall_arg(regs, 0);
	struct stat st;
	if (fstat(fd, &st) != 0) {
		return 0;  // Could not determine, hence treat as not a regular file
	}
	if(S_ISREG(st.st_mode) && fd > 2) {
		return 1;
	}
	return 0;
}

static void set_poll_itvl(sysfail_thread_discovery_t* disc, uint32_t value) {
    if(disc) disc->poll_itvl_usec = value;
}

static void stop_sysfail(sysfail_session_t* session) {
    if (session) session->stop(session);
}

static void add_this_thread(sysfail_session_t* session) {
    if (session) session->add_this_thread(session);
}

static void remove_this_thread(sysfail_session_t* session) {
    if (session) session->remove_this_thread(session);
}

static void add_thread(sysfail_session_t* session, sysfail_tid_t tid) {
    if (session) session->add_thread(session, tid);
}

static void remove_thread(sysfail_session_t* session, sysfail_tid_t tid) {
    if (session) session->remove_thread(session, tid);
}

static void discover_threads(sysfail_session_t* session) {
    if (session) session->discover_threads(session);
}

static void set_error_weights(sysfail_outcome_t* outcome, sysfail_error_wt_t* weights) {
	if(outcome) outcome->error_wts = weights;
}

static void set_no_next_outcome(sysfail_syscall_outcome_t* outcome) {
	if(outcome) outcome->next = NULL;
}

static void set_eligible_func_null(sysfail_outcome_t* outcome) {
	if(outcome) outcome->eligible = NULL;
}

static void set_ctx(sysfail_outcome_t* outcome, void* ctx) {
	if(outcome) outcome->ctx = ctx;
}

static void set_eligible_func_not_stdin_out_err(sysfail_outcome_t* outcome) {
	if(outcome) outcome->eligible = RegularFileOtherThanStdInOutErr;
}

#endif
*/
import "C"
import (
	"errors"
	"unsafe"
)

type Regs = C.greg_t

type ThdSelectorP func(ctx unsafe.Pointer, tid C.sysfail_tid_t) int

type CallEligibleP func(ctx unsafe.Pointer, regs *C.greg_t) int

// Probability represents the probability of a failure or delay.
type Probability struct {
	// P should be in the range [0, 1].
	P float64
	// Bias of failure, lower values make failure more likely before syscall
	// and higher values make failure more likely after syscall.
	// This is especially useful for testing idempotency properties of the
	// application.
	AfterBias float64
}

// ErrorWeight is used to determine the mix of errors presented
// when a syscall fails.
type ErrorWeight struct {
	// Error number (errno) to be presented to the application.
	Nerror int
	// Relative weight, higher weight makes the error more likely. This does not
	// affect the probability of failure, only the distribution of errors.
	Weight float64
}

type CallEligibilityPType int

const (
	AlwaysEligible CallEligibilityPType = iota
	CustomPredicate
	EligibleIfFDNotStdInOutErr
)

type Eligibility struct {
	// Function to determine if the thread is eligible for failure injection.
	Eligible CallEligibleP
	// Type of eligibility function
	Type CallEligibilityPType
}

type Outcome struct {
	// Probability and bias of failure.
	Fail Probability
	// Probability and bias of delay.
	Delay Probability
	// Maximum delay in microseconds.
	MaxDelayUsec uint32
	// User data.
	Ctx unsafe.Pointer
	// Function to determine if the thread is eligible for failure injection.
	Eligible Eligibility
	// Number of errors to form the mix to be drawn from when the syscall fails
	NumErrors uint32
	// Actual error codes and their weights
	ErrorWeights []ErrorWeight
}

// ThreadDiscoveryStrategy is the strategy to discover threads.
// Test can choose to have the threads automatically discovered or manually
// add / remove threads that are failure injected.
// Alternatively, the test can choose to poll the threads using the session API
// to discover threads at specific points in the execution of test.
type ThreadDiscoveryStrategy uint32

const (
	// ThreadDiscoveryNone No thread discovery strategy, add / remove / discover threads manually
	ThreadDiscoveryNone ThreadDiscoveryStrategy = iota
	// ThreadDiscoveryPoll Poll to discover threads at regular intervals, manual controls can also
	// be used in conjunction with automatic discovery.
	ThreadDiscoveryPoll
)

// ThreadDiscoveryConfig is the configuration for thread discovery
type ThreadDiscoveryConfig struct {
	PollIntervalUsec uint32
}

// SyscallOutcome is the outcome of a syscall.
type SyscallOutcome struct {
	// Syscall number.
	Syscall int
	// Outcome of the syscall.
	Outcome Outcome
}

// Plan is the plan for failure injection.
type Plan struct {
	// Thread discovery strategy.
	Strategy ThreadDiscoveryStrategy
	// Configuration for thread discovery.
	Config ThreadDiscoveryConfig
	// User data.
	Ctx unsafe.Pointer
	// Syscall outcomes.
	SyscallOutcomes []*SyscallOutcome
}

// Session is the failure injection session.
type Session struct {
	session *C.sysfail_session_t
	plan    *C.sysfail_plan_t
}

var (
	// ErrStartFailed is returned when starting a session fails.
	ErrStartFailed = errors.New("failed to start session")
	// ErrMallocFailed is returned when memory allocation fails.
	ErrMallocFailed = errors.New("failed to allocate memory")
	// ErrSelectorNotSupported is returned when selector is not supported.
	ErrSelectorNotSupported = errors.New("selector not supported")
	// ErrCustomEligibilityNotSupported is returned when custom eligibility function is not supported.
	ErrCustomEligibilityNotSupported = errors.New("custom eligibility function not supported")
)

// NewPlan creates a new plan for failure injection.
func NewPlan(
	strategy ThreadDiscoveryStrategy,
	pollIntervalUsec uint32,
	syscallOutcomes []*SyscallOutcome,
	sel ThdSelectorP,
	ctx unsafe.Pointer,
) (*Plan, error) {
	if sel != nil {
		return nil, ErrSelectorNotSupported
	}
	return &Plan{
		Strategy: strategy,
		Config: ThreadDiscoveryConfig{
			PollIntervalUsec: pollIntervalUsec, // not used if strategy is ThreadDiscoveryNone
		},
		SyscallOutcomes: syscallOutcomes,
		Ctx:             nil, // not used
	}, nil
}

// StartSession starts a new failure injection session.
func StartSession(plan *Plan) (*Session, error) {
	var cOutcomes *C.sysfail_syscall_outcome_t = nil
	if len(plan.SyscallOutcomes) > 0 {
		cOutcomes = (*C.sysfail_syscall_outcome_t)(C.malloc(C.size_t(len(plan.SyscallOutcomes) * int(unsafe.Sizeof(C.sysfail_syscall_outcome_t{})))))
		if cOutcomes == nil {
			return nil, ErrMallocFailed
		}
		var prev *C.sysfail_syscall_outcome_t = nil
		for i, outcome := range plan.SyscallOutcomes {
			curr := (*C.sysfail_syscall_outcome_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cOutcomes)) + uintptr(i)*unsafe.Sizeof(*cOutcomes)))
			curr.syscall = C.int(outcome.Syscall)
			curr.outcome.fail.p = C.double(outcome.Outcome.Fail.P)
			curr.outcome.fail.after_bias = C.double(outcome.Outcome.Fail.AfterBias)
			curr.outcome.delay.p = C.double(outcome.Outcome.Delay.P)
			curr.outcome.delay.after_bias = C.double(outcome.Outcome.Delay.AfterBias)
			curr.outcome.max_delay_usec = C.uint(outcome.Outcome.MaxDelayUsec)
			C.set_ctx(&curr.outcome, unsafe.Pointer(nil))
			switch outcome.Outcome.Eligible.Type {
			case CustomPredicate:
				return nil, ErrCustomEligibilityNotSupported
			case EligibleIfFDNotStdInOutErr:
				C.set_eligible_func_not_stdin_out_err(&curr.outcome)
			default:
				C.set_eligible_func_null(&curr.outcome)
			}
			curr.outcome.num_errors = C.uint(outcome.Outcome.NumErrors)
			if outcome.Outcome.NumErrors > 0 {
				ewSize := int(unsafe.Sizeof(C.sysfail_error_wt_t{}))
				var cErrors *C.sysfail_error_wt_t
				cErrors = (*C.sysfail_error_wt_t)(C.malloc(C.size_t(outcome.Outcome.NumErrors) * C.size_t(ewSize)))
				if cErrors == nil {
					return nil, ErrMallocFailed
				}
				for j, ew := range outcome.Outcome.ErrorWeights {
					ewPtr := (*C.sysfail_error_wt_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cErrors)) + uintptr(j)*unsafe.Sizeof(*cErrors)))
					ewPtr.nerror = C.int(ew.Nerror)
					ewPtr.weight = C.double(ew.Weight)
				}
				C.set_error_weights(&curr.outcome, cErrors)
			}
			if prev != nil {
				prev.next = curr
			}
			C.set_no_next_outcome(curr)
			prev = curr
		}
	}

	cPlan := &C.sysfail_plan_t{
		strategy:         C.sysfail_thread_discovery_strategy_t(plan.Strategy),
		ctx:              nil,
		selector:         nil, //C.Selector,
		syscall_outcomes: cOutcomes,
	}

	C.set_poll_itvl(&cPlan.config, C.uint(plan.Config.PollIntervalUsec))

	session := C.sysfail_start(cPlan)

	if session == nil {
		return nil, ErrStartFailed
	}

	return &Session{session: session, plan: cPlan}, nil
}

// Stop stops the failure injection session.
func (s *Session) Stop() {
	C.stop_sysfail(s.session)
}

// AddThisThread enables failure injection for the calling thread.
func (s *Session) AddThisThread() {
	C.add_this_thread(s.session)
}

// RemoveThisThread disables failure injection for the calling thread.
func (s *Session) RemoveThisThread() {
	C.remove_this_thread(s.session)
}

// AddThread enables failure injection for the specified thread.
func (s *Session) AddThread(tid int) {
	C.add_thread(s.session, C.sysfail_tid_t(tid))
}

// RemoveThread disables failure injection for the specified thread.
func (s *Session) RemoveThread(tid int) {
	C.remove_thread(s.session, C.sysfail_tid_t(tid))
}

// DiscoverThreads discovers threads for failure injection.
func (s *Session) DiscoverThreads() {
	C.discover_threads(s.session)
}
