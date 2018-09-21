package tm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/pb"
)

func TestDriftingClockCheckInterval(t *testing.T) {

	testCases := []struct {
		driftFactor   float64
		offset        time.Duration
		sleepDuration time.Duration
	}{
		{
			driftFactor:   1.2,
			offset:        time.Second,
			sleepDuration: 10 * time.Millisecond,
		},
		{
			driftFactor:   0.8,
			offset:        10 * time.Second,
			sleepDuration: 10 * time.Millisecond,
		},
		{
			driftFactor:   1.1,
			offset:        time.Hour,
			sleepDuration: 100 * time.Millisecond,
		},
		{
			driftFactor:   0.9,
			offset:        time.Minute,
			sleepDuration: 100 * time.Millisecond,
		},
		{
			driftFactor:   1.03,
			offset:        0,
			sleepDuration: 10 * time.Millisecond,
		},
		{
			driftFactor:   1,
			offset:        0,
			sleepDuration: 10 * time.Millisecond,
		},
	}

	for _, tc := range testCases {
		dc := NewDriftingClock(tc.driftFactor, 0)
		mc := NewMonotonicClock()
		mt1 := mc.Now()
		dt1 := dc.Now()
		time.Sleep(tc.sleepDuration)
		dt2 := dc.Now()
		mt2 := mc.Now()
		assert.InDelta(t, (float64(mt2-mt1))*tc.driftFactor, float64(dt2-dt1), float64(tc.sleepDuration/100))
	}
}

func TestDriftingClockChangeConfigs(t *testing.T) {
	configs := []*kronospb.DriftTimeConfig{
		{DriftFactor: 1.2, Offset: 0},
		{DriftFactor: 0.8, Offset: int64(time.Hour)},
		{DriftFactor: 1.1, Offset: 0},
		{DriftFactor: 0.9, Offset: int64(time.Minute)},
	}

	dc := NewDriftingClock(configs[0].DriftFactor, time.Duration(configs[0].Offset))
	mc := NewMonotonicClock()
	lastMonotonicTime := mc.Now()
	lastDriftedTime := dc.Now()
	for i := 0; i < len(configs); i++ {
		sleepDuration := 10 * time.Millisecond
		time.Sleep(sleepDuration)
		mt1 := mc.Now()
		dt1 := dc.Now()
		assert.InDelta(
			t,
			(float64(mt1-lastMonotonicTime))*configs[i].DriftFactor,
			float64(dt1-lastDriftedTime),
			float64(sleepDuration/100),
		)
		if i == len(configs)-1 {
			break
		}
		dc.UpdateDriftConfig(configs[i+1])
		time.Sleep(sleepDuration)
		mt2 := mc.Now()
		dt2 := dc.Now()
		assert.InDelta(
			t,
			(float64(mt2-mt1))*configs[i+1].DriftFactor+float64(configs[i+1].Offset),
			float64(dt2-dt1),
			float64(sleepDuration/100),
		)
		lastMonotonicTime = mt2
		lastDriftedTime = dt2
	}
}
