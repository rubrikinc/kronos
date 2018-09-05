package tm

import (
	"sync/atomic"
	"time"
)

// ManualClock is used in tests to mock time
type ManualClock struct {
	time int64
}

// Now returns the manually set time
func (c *ManualClock) Now() int64 {
	return atomic.LoadInt64(&c.time)
}

// SetTime sets the time of the ManualClock
func (c *ManualClock) SetTime(t int64) {
	atomic.StoreInt64(&c.time, t)
}

// AdvanceTime progesses time by the given duration
func (c *ManualClock) AdvanceTime(t time.Duration) {
	atomic.AddInt64(&c.time, int64(t))
}

// NewManualClock returns an instance of ManualClock
func NewManualClock() *ManualClock {
	return &ManualClock{}
}
