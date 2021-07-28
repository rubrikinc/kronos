package tm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOffset(t *testing.T) {
	a := assert.New(t)
	c1 := NewMonotonicClockWithOffset(int64(1*time.Hour) /* offset */, int64(2*time.Hour) /* uptime_offset */)
	c2 := NewMonotonicClockWithOffset(int64(10*time.Hour) /* offset */, int64(9*time.Hour) /* uptime offset */)
	a.InDelta(int64(9*time.Hour), c2.Now()-c1.Now(), float64(10*time.Second))
	a.InDelta(int64(7*time.Hour), c2.Uptime()-c1.Uptime(), float64(10*time.Second))
}
