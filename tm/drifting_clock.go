package tm

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
)

// DriftingClock is an implementation of Clock which can be used to simulate
// drifts and jumps in clocks
type DriftingClock struct {
	mu              *syncutil.RWMutex
	lastActualTime  time.Time
	lastDriftedTime time.Time
	clockConfig     *kronospb.DriftTimeConfig
}

// Now returns the time at the given moment assuming drift and offset as
// described by clockConfig.
// Returned time should be offset + lastDriftedTime + (currentActualTime - lastActualTime)*driftFactor
// = lastDriftedTime +
// (currentActualTime - lastActualTime) +
// (currentActualTime - lastActualTime) * (driftFactor - 1) +
// offset
// The drift part is broken to reduce errors due to precision of driftFactor.
func (t *DriftingClock) Now() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	// durationSinceLastSeenTime = (currentActualTime - lastActualTime)
	durationSinceLastSeenTime := time.Since(t.lastActualTime)
	t.lastActualTime = t.lastActualTime.Add(durationSinceLastSeenTime)
	// diffDuration = (now - lastActualTime) * (driftFactor - 1)
	diffDuration := time.Duration(
		(t.clockConfig.DriftFactor - 1) * float64(durationSinceLastSeenTime),
	)
	// lastDriftedTime + (currentActualTime - lastActualTime) + (currentActualTime - lastActualTime) * (driftFactor - 1)
	driftedTime := t.lastDriftedTime.Add(durationSinceLastSeenTime).Add(diffDuration)
	t.lastDriftedTime = driftedTime
	// Add offset at the end.
	return driftedTime.UnixNano() + t.clockConfig.Offset
}

// UpdateDriftConfig changes the driftFactor according to dtc and jumps the
// clock by the the offset in dtc.
func (t *DriftingClock) UpdateDriftConfig(dtc *kronospb.DriftTimeConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clockConfig.DriftFactor = dtc.DriftFactor
	t.clockConfig.Offset += dtc.Offset
}

// NewDriftingClock returns an instance of DriftingClock
func NewDriftingClock(driftFactor float64, offset time.Duration) *DriftingClock {
	currentTime := time.Now()
	return &DriftingClock{
		mu:              &syncutil.RWMutex{},
		lastActualTime:  currentTime,
		lastDriftedTime: currentTime,
		clockConfig: &kronospb.DriftTimeConfig{
			DriftFactor: driftFactor,
			Offset:      offset.Nanoseconds(),
		},
	}
}

// UpdateDriftClockServer server is used to handle grpc calls to change the
// drift config of drifting clock
type UpdateDriftClockServer struct {
	Clock *DriftingClock
}

var _ kronospb.UpdateDriftTimeServiceServer = &UpdateDriftClockServer{}

// UpdateDriftConfig is the method to update the DriftConfig
func (ds *UpdateDriftClockServer) UpdateDriftConfig(
	ctx context.Context, dtc *kronospb.DriftTimeConfig,
) (*kronospb.DriftTimeResponse, error) {
	ds.Clock.UpdateDriftConfig(dtc)
	log.Infof(ctx, "Clock config changed to %v", ds.Clock.clockConfig)
	return &kronospb.DriftTimeResponse{}, nil
}

// NewUpdateDriftClockServer returns an instance of UpdateDriftClockServer
func NewUpdateDriftClockServer(startConfig *kronospb.DriftTimeConfig) *UpdateDriftClockServer {
	var c *DriftingClock
	if startConfig == nil {
		c = NewDriftingClock(1.0, 0)
	} else {
		c = NewDriftingClock(startConfig.DriftFactor, time.Duration(startConfig.Offset))
	}
	return &UpdateDriftClockServer{c}
}
