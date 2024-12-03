//go:build acceptance

package randomized_tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestRandomFailureInjection(t *testing.T) {

	// This test has two phases:
	// 1. Correctness Phase - It runs a series of randomized failure injections and asserts that the following invariants hold:
	//    - Each node either doesn't serve time or serves time within the allowed time difference.
	// 2. Liveness Phase - In this phase, a quorum of nodes is identified and all the failure injections are removed from
	//   the quorum. The test asserts that the time difference between the nodes in the quorum is within the allowed time difference.
	// The test is run for a fixed duration and the test fails if the invariants are violated.
	a := require.New(t)
	fs := afero.NewOsFs()
	ctx := context.Background()
	numNodes := 8
	manageOracleTickInterval := 100 * time.Millisecond

	tc, err := cluster.NewClusterWithFailureInjection(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            5,
		},
		cluster.DefaultFailureInjectionConfig,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)

	ticker := time.NewTicker(100 * time.Millisecond)

	defaultValidationThreshold := time.Duration(4 * int64(cluster.DefaultFailureInjectionConfig.DefaultNetworkDelayMicro) * int64(time.Microsecond))

	// Correctness Phase
	// Any number of faults can be injected in this phase
	// Assert that the time difference between the nodes serving time is within the allowed time difference
	startTime := time.Now()
	for time.Since(startTime) < time.Minute {
		select {
		case <-ticker.C:
			a.NoError(tc.Tick(), "failed to tick")
			tc.AssertTimeInvariants(ctx,
				a,
				defaultValidationThreshold,
				make(map[int]struct{}),
			)
		}
	}

	// Liveness Phase
	// Identify a quorum of nodes
	// Remove all the failure injections from the quorum
	// Assert that the time difference between the nodes in the quorum is within the allowed time difference and quorum is able to serve time
	quorumNodes := numNodes/2 + 1
	quorum := make([]int, numNodes)
	for i := 0; i < numNodes; i++ {
		quorum[i] = i
	}
	chosenNodes := make(map[int]struct{})
	rand.Shuffle(numNodes, func(i, j int) {
		quorum[i], quorum[j] = quorum[j], quorum[i]
	})
	quorum = quorum[:quorumNodes]
	log.Infof(ctx, "Removing all faults from quorum %v", quorum)
	tc.RemoveAllFaults(quorum)
	time.Sleep(time.Minute)
	startTime = time.Now()
	for time.Since(startTime) < time.Minute {
		select {
		case <-ticker.C:
			a.NoError(tc.Tick(), "failed to tick")
			tc.AssertTimeInvariants(
				ctx,
				a,
				defaultValidationThreshold,
				chosenNodes,
			)
		}
	}
}
