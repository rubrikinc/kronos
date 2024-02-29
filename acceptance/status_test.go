//go:build acceptance
// +build acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"math/rand"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/acceptance/chaos"
	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/cli"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
)

func nodeInfosFromBytes(b []byte, a *assert.Assertions) []cli.NodeInfo {
	var nodeInfos []cli.NodeInfo
	err := json.Unmarshal(b, &nodeInfos)
	a.NoError(err)
	return nodeInfos
}

func TestKronosStatus(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	numNodes := 4
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			ManageOracleTickInterval: manageOracleTickInterval,
			NumNodes:                 numNodes,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	a := assert.New(t)

	// give some time to elect the oracle
	time.Sleep(kronosStabilizationBufferTime)
	a.NoError(runPolicy(ctx, tc, newTimeValidator(defaultValidationThreshold), &chaos.DriftAllPolicy{}))
	// give some time so that nodes have non-zero delta
	time.Sleep(kronosStabilizationBufferTime)
	// PreCheck - validate time across cluster is in similar range
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}
	a.NoError(tc.RemoveNode(ctx, len(tc.Nodes)-1, -1, ""))
	data, err := tc.Status(rand.Intn(len(tc.Nodes)-1), false /*local*/)
	a.NoError(err)
	nodeInfos := nodeInfosFromBytes(data, a)
	// removed node should not be shown in status
	a.Equal(len(tc.Nodes)-1, len(nodeInfos))
	numDifferentOracles := 0
	numNonZeroDeltas := 0
	// Verify that all the values are present and sensible.
	for _, nodeInfo := range nodeInfos {
		a.Nil(nodeInfo.Err)
		if proto.Equal(nodeInfo.OracleState.Oracle, nodeInfo.GRPCAddr) {
			numDifferentOracles++
		}
		if nodeInfo.Delta != int64(0) {
			numNonZeroDeltas++
		}
		a.True(nodeInfo.Time <= nodeInfo.OracleState.TimeCap)
		// NodeIds are always > 0
		a.NotEqual(nodeInfo.OracleState.Id, uint64(0))
		a.NotEqual(nodeInfo.Time, int64(0))
		a.NotNil(nodeInfo.RaftAddr)
		a.NotNil(nodeInfo.GRPCAddr)
		a.NotNil(nodeInfo.OracleState.Oracle)
		a.Equal(kronospb.ServerStatus_INITIALIZED, nodeInfo.ServerStatus)
	}
	// Verify that all nodes show same oracle. There should be no oracle change as
	// only drift policy is running.
	a.Equal(numDifferentOracles, 1)
	// Verify that atleast one node has non-zero delta. This is slightly flaky.
	a.NotEqual(0, numNonZeroDeltas)

	// verify that --local flag is respected
	data, err = tc.Status(rand.Intn(len(tc.Nodes)-1), true /*local*/)
	a.NoError(err)
	nodeInfos = nodeInfosFromBytes(data, a)

	a.Equal(1, len(nodeInfos))
	// Verify that all the values are present and sensible.
	for _, nodeInfo := range nodeInfos {
		a.Nil(nodeInfo.Err)
		a.True(nodeInfo.Time <= nodeInfo.OracleState.TimeCap)
		// NodeIds are always > 0
		a.NotEqual(nodeInfo.OracleState.Id, uint64(0))
		a.NotEqual(nodeInfo.Time, int64(0))
		a.NotNil(nodeInfo.RaftAddr)
		a.NotNil(nodeInfo.GRPCAddr)
		a.NotNil(nodeInfo.OracleState.Oracle)
		a.Equal(kronospb.ServerStatus_INITIALIZED, nodeInfo.ServerStatus)
	}

	// verify that errors are reported
	a.NoError(tc.RunOperation(ctx, cluster.Stop, 2))
	data, err = tc.Status(1, false /*local*/)
	a.NoError(err)
	var partials []map[string]interface{}
	a.NoError(json.Unmarshal(data, &partials))
	// One node was removed and it should not be shown
	a.Equal(len(tc.Nodes)-1, len(partials))
	numErrs := 0
	for _, partial := range partials {
		if partial["error"] != nil {
			numErrs++
		}
	}
	// There should be exactly one error as only one node was stopped.
	a.Equal(1, numErrs)
}
