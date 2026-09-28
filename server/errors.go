package server

// This file uses the standard library's errors rather than github.com/pkg/errors
// (which server.go imports) so the sentinels below carry no package-init stack.
import "errors"

// Causes returned, wrapped with %w, by KronosTimeNowRaw and KronosUptimeNowRaw.
// Callers classify with errors.Is instead of matching message text: CockroachDB
// fatals on these, and which one fired is the difference between a startup race
// and a lost oracle.
var (
	ErrNotInitialized     = errors.New("kronos server not yet initialized")
	ErrTimeCapNotInited   = errors.New("kronos time cap not yet initialized")
	ErrTimeCapStale       = errors.New("kronos time is beyond current time cap, time cap is too stale")
	ErrUptimeCapNotInited = errors.New("kronos up time cap not yet initialized")
	ErrUptimeCapStale     = errors.New("kronos up time is beyond current time cap, time cap is too stale")
)
