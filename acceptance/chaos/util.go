package chaos

import (
	"context"
	"math/rand"

	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
)

func randomSubset(numNodes, numNodesToSelect int) []int {
	allIndices := allIndices(numNodes)
	rand.Shuffle(
		numNodes,
		func(i, j int) {
			tmp := allIndices[i]
			allIndices[i] = allIndices[j]
			allIndices[j] = tmp
		},
	)
	return allIndices[:numNodesToSelect]
}

func allIndices(numNodes int) []int {
	allIndices := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		allIndices[i] = i
	}
	return allIndices
}

func driftNodes(ctx context.Context, tc *cluster.TestCluster, nodeIndices ...int) error {
	for _, nodeIdx := range nodeIndices {
		randFloat := rand.Float64()
		df := 1 + (randFloat-0.5)*driftRange
		log.Infof(ctx, "Drifting node %d with drift factor %v", nodeIdx, df)
		if err := tc.UpdateClockConfig(
			ctx,
			nodeIdx,
			&kronospb.DriftTimeConfig{DriftFactor: df},
		); err != nil {
			return err
		}
	}
	return nil
}
