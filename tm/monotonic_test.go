package tm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOffset(t *testing.T) {
	a := assert.New(t)
	c1 := NewMonotonicClockWithOffset(int64(1 * time.Hour) /* offset */)
	c2 := NewMonotonicClockWithOffset(int64(10 * time.Hour) /* offset */)
	a.InDelta(int64(9*time.Hour), c2.Now()-c1.Now(), float64(10*time.Second))
}
