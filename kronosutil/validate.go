package kronosutil

import (
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
)

// ValidateTimeInConsensus validates that the kronos time across the given nodes
// (difference between maxTime and minTime) is within maxDiffAllowed.
func ValidateTimeInConsensus(
	ctx context.Context, maxDiffAllowed time.Duration, timeOnNodes map[string]int64,
) error {
	var maxTimeAcrossNodes int64 = math.MinInt64
	var minTimeAcrossNodes int64 = math.MaxInt64
	for _, timeOnNode := range timeOnNodes {
		if timeOnNode > maxTimeAcrossNodes {
			maxTimeAcrossNodes = timeOnNode
		}
		if timeOnNode < minTimeAcrossNodes {
			minTimeAcrossNodes = timeOnNode
		}
	}

	diff := maxTimeAcrossNodes - minTimeAcrossNodes
	if diff > int64(maxDiffAllowed) {
		return errors.Errorf(
			"time seen across nodes varies by %v, more than allowed %v, timeOnNodes: %v",
			time.Duration(diff),
			maxDiffAllowed,
			timeOnNodes,
		)
	}
	return nil
}
