package kronosutil

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateTimeInConsensus(t *testing.T) {
	testCases := []struct {
		name           string
		timeOnNodes    map[string]int64
		maxDiffAllowed time.Duration
		errRegexp      *regexp.Regexp
	}{
		{
			name:           "empty map",
			timeOnNodes:    map[string]int64{},
			maxDiffAllowed: 5,
		},
		{
			name: "one element",
			timeOnNodes: map[string]int64{
				"1": 1,
			},
			maxDiffAllowed: 5,
		},
		{
			name: "two elements less than threshold",
			timeOnNodes: map[string]int64{
				"1": 1,
				"2": 2,
			},
			maxDiffAllowed: 5,
		},
		{
			name: "many elements less than threshold",
			timeOnNodes: map[string]int64{
				"1": 1,
				"2": 2,
				"4": 4,
				"5": 5,
			},
			maxDiffAllowed: 5,
		},
		{
			name: "many elements more than threshold",
			timeOnNodes: map[string]int64{
				"1": 1,
				"4": 4,
				"9": 9,
			},
			maxDiffAllowed: 5,
			errRegexp:      regexp.MustCompile("time seen across nodes varies by 8"),
		},
	}
	ctx := context.TODO()
	for _, testCase := range testCases {
		tc := testCase
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateTimeInConsensus(ctx, tc.maxDiffAllowed, tc.timeOnNodes); err != nil {
				assert.NotNil(t, tc.errRegexp)
				assert.Regexp(t, tc.errRegexp, err.Error())
				return
			}
			assert.Nil(t, tc.errRegexp)
		})
	}
}
