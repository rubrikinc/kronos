package tm

import "time"

// MonotonicClock is an implementation of Clock which is immune to clock jumps
type MonotonicClock struct {
	startTime time.Time
	offset    int64
}

// Now takes adds the elapsed time from a fixed point of reference to generate
// a new time
func (c *MonotonicClock) Now() int64 {
	return c.startTime.UnixNano() + time.Since(c.startTime).Nanoseconds() + c.offset
}

// NewMonotonicClock returns an instance of MonotonicClock
func NewMonotonicClock() Clock {
	return NewMonotonicClockWithOffset(0)
}

// NewMonotonicClockWithOffset returns an instance of MonotonicClock
// offset is added to startTime when computing Now()
func NewMonotonicClockWithOffset(offset int64) Clock {
	return &MonotonicClock{
		startTime: time.Now(),
		offset:    offset,
	}
}
