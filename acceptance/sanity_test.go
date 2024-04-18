//go:build acceptance
// +build acceptance

package acceptance

import (
	"bytes"
	"context"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/checksumfile"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
)

const (
	kronosStabilizationBufferTime = 15 * time.Second
	manageOracleTickInterval      = time.Second
	validationThreshold           = 50 * time.Millisecond
	testTimeout                   = 5 * time.Minute
)

func TestKronosSanity(t *testing.T) {
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

	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// PreCheck - validate time across cluster is in similar range
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /* checkOnlyRunningNodes */
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tc.Oracle(ctx, false /* checkOnlyRunningNodes */)
	if err != nil {
		t.Fatal(err)
	}

	// restart all the nodes
	var allIndices []int
	for i := 0; i < numNodes; i++ {
		allIndices = append(allIndices, i)
	}
	err = tc.RunOperation(ctx, cluster.Restart, allIndices...)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(kronosStabilizationBufferTime)
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /* checkOnlyRunningNodes */
	)
	if err != nil {
		t.Fatal(err)
	}

	driftRange := 0.1
	// Introduce drift on all the nodes
	for i := 0; i < numNodes; i++ {
		randFloat := rand.Float64()
		df := 0.95 + randFloat*driftRange
		if err := tc.UpdateClockConfig(
			ctx,
			i,
			&kronospb.DriftTimeConfig{
				DriftFactor: df,
				Offset:      0,
			},
		); err != nil {
			log.Fatal(ctx, err)
		}
	}
	// Check the nodes time across the cluster for 30*0.1s = 3s while drift is
	// present
	for i := 0; i < 30; i++ {
		// The delta range is kept lenient as the nodes have variable drift, making
		// them far apart for the period the node is not syncing with orcale.
		_, _, err = tc.ValidateTimeInConsensus(
			ctx,
			time.Duration(driftRange*float64(manageOracleTickInterval)),
			false, /* checkOnlyRunningNodes */
		)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Restart the oracle
	oracle, err := tc.Oracle(ctx, false /*checkOnlyRunningNodes*/)
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.RunOperation(ctx, cluster.Restart, oracle); err != nil {
		log.Fatal(ctx, err)
	}
	time.Sleep(kronosStabilizationBufferTime)
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		time.Duration(driftRange*float64(manageOracleTickInterval)),
		false, /* checkOnlyRunningNodes */
	)
	if err != nil {
		t.Fatal(err)
	}
	newOracle, err := tc.Oracle(ctx, false /*checkOnlyRunningNodes*/)
	t.Logf("oracle - %d, newOracle - %d", oracle, newOracle)
	if oracle == newOracle {
		t.Fatalf("New oracle and oracle should be different, found same %d", oracle)
	}
	if err != nil {
		t.Fatal(err)
	}

	// check the flow of time matches the oracle time flow.
	oldTimes := make([]int64, numNodes)
	for i := 0; i < numNodes; i++ {
		oldTimes[i], err = tc.Time(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
	}
	sleepTime := 5 * manageOracleTickInterval
	time.Sleep(sleepTime)
	newTimes := make([]int64, numNodes)
	for i := 0; i < numNodes; i++ {
		newTimes[i], err = tc.Time(ctx, i)
		if err != nil {
			t.Fatal(err)
		}
	}
	timeElapsedOnOracle := newTimes[newOracle] - oldTimes[newOracle]
	t.Logf("Time elapsed in oracle - %v", time.Duration(timeElapsedOnOracle))
	a := assert.New(t)
	for i := 0; i < numNodes; i++ {
		timeDiffOnIthNode := newTimes[i] - oldTimes[i]
		t.Logf("Time elapsed in node %d - %v", i, time.Duration(timeDiffOnIthNode))
		a.InDelta(
			timeDiffOnIthNode,
			timeElapsedOnOracle,
			float64(manageOracleTickInterval)/20,
		)
	}
	driftConfigOnOracle := tc.GetClockConfig(ctx, newOracle)
	a.InDelta(
		driftConfigOnOracle.DriftFactor*float64(sleepTime),
		timeElapsedOnOracle,
		float64(manageOracleTickInterval)/20,
	)
}

func TestKronosInsecureCluster(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	numNodes := 4
	tc, err := cluster.NewInsecureCluster(
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

	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// PreCheck - validate time across cluster is in similar range
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /* checkOnlyRunningNodes */
	)
	if err != nil {
		t.Fatal(err)
	}

}

func TestKronosSanityReIP(t *testing.T) {
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

	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// PreCheck - validate time across cluster is in similar range
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}

	if err = tc.ReIP(ctx); err != nil {
		t.Fatal(err)
	}

	// wait for sometime after restart.
	time.Sleep(kronosStabilizationBufferTime)
	_, err = tc.Oracle(ctx, false /*checkOnlyRunningNodes*/)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}

}

func TestKronosSanityBackupRestore(t *testing.T) {
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

	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// PreCheck - validate time across cluster is in similar range
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}
	fileName := filepath.Join(tc.Nodes[0].DataDir(), "cluster_info")

	// backup command should fail as node is running.
	if err := tc.Backup(ctx, 0); err == nil {
		t.Fatal("backup should not succeed when node is running")
	}

	if err := tc.RunOperation(ctx, cluster.Stop, 0); err != nil {
		t.Fatal(err)
	}

	oldData, err := checksumfile.Read(fileName)
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.Backup(ctx, 0); err != nil {
		t.Fatal(err)
	}

	c, err := metadata.NewCluster(tc.Nodes[0].DataDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Persist(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	if err := tc.Restore(ctx, 0); err != nil {
		t.Fatal(err)
	}

	newData, err := checksumfile.Read(fileName)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(oldData, newData) {
		t.Fatal("Data after backup and restore doesn't match")
	}

	if err := tc.RunOperation(ctx, cluster.Start, 0); err != nil {
		t.Fatal(err)
	}

	// wait for sometime after restart.
	time.Sleep(kronosStabilizationBufferTime)

	// restore command should fail as node is running.
	if err := tc.Restore(ctx, 0); err == nil {
		t.Fatal("restore should not succeed when node is running")
	}

	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}

}

func TestKronosSanityAddRemove(t *testing.T) {
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
	// give some time to elect the oracle
	time.Sleep(bootstrapTime)

	// remove node 2
	nodeToRemove := 2
	if err := tc.RemoveNode(ctx, nodeToRemove, -1, ""); err != nil {
		t.Fatal(err)
	}
	_, err = tc.Time(ctx, nodeToRemove)
	if err == nil {
		t.Fatalf("unexpected nil err")
	}

	// give some time to let the cluster stabilize in case node 2 was oracle or
	// raft leader.
	time.Sleep(kronosStabilizationBufferTime)
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(kronosStabilizationBufferTime)
	if _, err := tc.AddNode(ctx, 2); err != nil {
		t.Fatal(err)
	}

	// wait for 2 to get initialized
	time.Sleep(kronosStabilizationBufferTime)
	_, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		false, /*checkOnlyRunningNodes*/
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKronosSanityDeadNode(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	a := assert.New(t)
	numNodes := 7
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	// utility functions
	removeNode := func(idx int) {
		if err := tc.RemoveNode(ctx, idx, -1, ""); err != nil {
			t.Fatal(err)
		}
	}
	addNode := func(idx int) string {
		id, err := tc.AddNode(ctx, idx)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	stopNode := func(idx int) {
		if err := tc.RunOperation(ctx, cluster.Stop, idx); err != nil {
			t.Fatal(err)
		}
	}
	startNode := func(idx int) {
		if err := tc.RunOperation(ctx, cluster.Start, idx); err != nil {
			t.Fatal(err)
		}
	}
	// Need more time to initialize more 7 nodes cluster.
	time.Sleep(bootstrapTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	// Give some time to elect the oracle
	time.Sleep(kronosStabilizationBufferTime)
	removeNode(3)
	removeNode(4)
	removeNode(5)
	removeNode(6)

	// Stop the node to be dead for a long time.
	stopNode(2)
	time.Sleep(kronosStabilizationBufferTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}

	// Add 3 nodes which dead node isn't aware about.
	addNode(3)
	addNode(4)
	addNode(5)
	nodeIDForNewNode := addNode(6)
	startTime := time.Now()
	for time.Since(startTime) < time.Minute {
		if _, _, err = tc.ValidateTimeInConsensus(
			ctx,
			validationThreshold,
			true, /*checkOnlyRunningNodes*/
		); err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}

	// Now the cluster size is again 6.
	// Stop and start 0 to make sure it is neither raft leader nor oracle.
	stopNode(0)
	time.Sleep(kronosStabilizationBufferTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}
	startNode(0)
	time.Sleep(kronosStabilizationBufferTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}

	// Remove node 1, about which dead node isn't aware.
	removeNode(1)
	// Remove node 6. Dead node isn't aware of it's removal and addition.
	removeNode(6)
	time.Sleep(kronosStabilizationBufferTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}

	// Resurrect the long dead node
	startNode(2)
	// Wait longer so that the node gets and publish a snapshot
	time.Sleep(2 * kronosStabilizationBufferTime)
	if _, _, err = tc.ValidateTimeInConsensus(
		ctx,
		validationThreshold,
		true, /*checkOnlyRunningNodes*/
	); err != nil {
		t.Fatal(err)
	}
	a.NoError(err)
	// Check id node 6 is removed according to node 2(long dead node).
	isRemoved, err := tc.Nodes[2].IsNodeRemoved(nodeIDForNewNode)
	a.NoError(err)
	a.True(isRemoved)
}

func TestKronosSanityDuplicateNodes(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	a := assert.New(t)
	numNodes := 4
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	// give some time to elect the oracle
	time.Sleep(bootstrapTime)

	WipeAndAdd := func(idx int) {
		dataDir := tc.Nodes[idx].DataDir()
		oldNodeID, err := tc.NodeID(idx)
		a.NoError(err)
		tc.RunOperation(ctx, cluster.Stop, idx)
		fs.RemoveAll(dataDir)
		_, err = tc.AddNode(ctx, idx)
		time.Sleep(kronosStabilizationBufferTime)
		// Node idx should not come up since a node with
		// same raft-addr already exists in the cluster
		_, err = tc.Time(ctx, idx)
		a.Error(err)
		// Add the node back by removing it first
		time.Sleep(kronosStabilizationBufferTime)
		a.NoError(tc.RemoveNode(ctx, -1, idx^1, oldNodeID))
		time.Sleep(kronosStabilizationBufferTime)
		a.NoError(tc.RunOperation(ctx, cluster.Start, idx))
		time.Sleep(kronosStabilizationBufferTime)
		timeMap, uptimeMap, err := tc.ValidateTimeInConsensus(ctx,
			50*time.Millisecond, false)
		a.NoError(err)
		a.Contains(timeMap, idx)
		a.Contains(uptimeMap, idx)
	}
	// Non-seed node
	WipeAndAdd(2)
	// first seed node wipe and add
	// is not supported yet. It will form a cluster
	// of size 1 independent of the other nodes.
	//WipeAndAdd(0)
}

func TestKronosStrayMessages(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	a := assert.New(t)
	numNodes := 5
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)

	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// make sure the seed nodes are not the raft leader
	a.NoError(tc.RunOperation(ctx, cluster.Stop, 0, 1))
	a.NoError(tc.RunOperation(ctx, cluster.Start, 0, 1))
	time.Sleep(kronosStabilizationBufferTime)
	// Wipe and add nodes 0 and 1
	a.NoError(tc.RunOperation(ctx, cluster.Stop, 0, 1))
	WipeAndAdd := func(idx int) {
		dataDir := tc.Nodes[idx].DataDir()
		a.NoError(err)
		a.NoError(fs.RemoveAll(dataDir))
		_, err := tc.AddNode(ctx, idx)
		a.NoError(err)
	}
	WipeAndAdd(0)
	WipeAndAdd(1)
	time.Sleep(2 * kronosStabilizationBufferTime)
	// node 0 will crash because there is a node with the same raft-addr
	for idx := 0; idx < 2; idx++ {
		_, err = tc.Time(ctx, idx)
		a.Error(err)
	}
	for idx := 2; idx < 5; idx++ {
		_, err = tc.Time(ctx, idx)
		a.NoError(err)
	}
}

func TestExchangeStores(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	a := assert.New(t)
	numNodes := 5
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	// give some time to elect the oracle
	time.Sleep(bootstrapTime)
	// validate time across cluster is in similar range

	_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond,
		false)
	a.NoError(err)

	oldId1, err := tc.NodeID(0)
	a.NoError(err)

	oldId2, err := tc.NodeID(1)
	a.NoError(err)

	a.NoError(tc.ExchangeDataDir(ctx, 0, 1))

	time.Sleep(kronosStabilizationBufferTime)

	newId1, err := tc.NodeID(0)
	a.NoError(err)

	newId2, err := tc.NodeID(1)
	a.NoError(err)

	a.Equal(oldId1, newId2, "Node 0 should have the data of node 1")
	a.Equal(oldId2, newId1, "Node 1 should have the data of node 0")

	_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond,
		false)
	a.NoError(err)
}

func TestFirstSeedDoesntFormNewCluster(t *testing.T) {
	ctx, cancelFunc := context.WithTimeout(context.TODO(), testTimeout)
	defer cancelFunc()
	fs := afero.NewOsFs()
	a := assert.New(t)
	numNodes := 5
	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            2,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	time.Sleep(bootstrapTime)

	_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
	a.NoError(err)

	RemoveAndAdd := func(idx int) {
		// remove the first seed node
		a.NoError(tc.RemoveNode(ctx, idx, -1, ""))

		// add the first seed node back
		_, err = tc.AddNode(ctx, idx)

		// wait for the node to come up
		time.Sleep(kronosStabilizationBufferTime)
		// validate the first node didn't form a new cluster
		_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
		a.NoError(err)

		data, err := tc.Status(idx, false /*local*/)
		a.NoError(err)
		nodeInfos := nodeInfosFromBytes(data, a)
		a.Equal(len(tc.Nodes), len(nodeInfos))
	}
	RemoveAndAdd(0)
	RemoveAndAdd(1)
}
