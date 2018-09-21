package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

const (
	// driftRange is the total range across which drift will vary.
	// Drift will vary from [1-driftRange/2,1+driftRange/2].
	driftRange = 0.05
	// Number of consecutive oracle sync errors upon which we attempt to
	// overthrow the oracle.
	numConsecutiveErrsForOverthrow = 3
)

// Policy describes the chaos policies that we inject while we are testing the
// cluster.
type Policy interface {
	Inject(ctx context.Context, tc *cluster.TestCluster) (time.Duration, error)
	Recover(ctx context.Context, tc *cluster.TestCluster) error
	String() string
}

// StopAnySubsetPolicy stops any subset of nodes in the cluster. Only
// MaxNodeFailuresAllowed are allowed to be stopped together.
type StopAnySubsetPolicy struct {
	MaxNodeFailuresAllowed int
	nodesToStop            []int
}

func (s *StopAnySubsetPolicy) String() string {
	return fmt.Sprintf(
		"StopAnySubsetPolicy(MaxNodeFailuresAllowed=%d)",
		s.MaxNodeFailuresAllowed,
	)
}

// Inject injects the failure policy.
func (s *StopAnySubsetPolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	numNodesToStop := rand.Intn(s.MaxNodeFailuresAllowed) + 1
	s.nodesToStop = randomSubset(len(tc.Nodes), numNodesToStop)
	log.Infof(ctx, "Stopping nodes: %v", s.nodesToStop)
	if err := tc.RunOperation(
		ctx,
		cluster.Stop,
		s.nodesToStop...,
	); err != nil {
		return 0, err
	}
	// drift and stop together can increase the time differences of the
	// default validation threshold as the drifted nodes might not sync with the
	// oracle for (numConsecutiveErrsForOverthrow + 1) * ManageOracleTickInterval
	// (numConsecutiveErrsForOverthrow ticks are required to change the oracle).
	return time.Duration(
		(numConsecutiveErrsForOverthrow + 1) * driftRange * float64(tc.ManageOracleTickInterval),
	), nil
}

// Recover recovers from the failure policy.
func (s *StopAnySubsetPolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	if err := tc.RunOperation(
		ctx,
		cluster.Start,
		s.nodesToStop...,
	); err != nil {
		return err
	}
	time.Sleep(3 * tc.ManageOracleTickInterval)
	return nil
}

// StopOraclePolicy stops the oracle node.
type StopOraclePolicy struct {
	oldOracle int
}

func (s *StopOraclePolicy) String() string {
	return "StopOraclePolicy"
}

// Inject injects the failure policy.
func (s *StopOraclePolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	var err error
	s.oldOracle, err = tc.Oracle(ctx, false /*checkOnlyForRunningNodes*/)
	if err != nil {
		return 0, err
	}
	if err := tc.RunOperation(ctx, cluster.Stop, s.oldOracle); err != nil {
		return 0, err
	}
	// drift and stop together can increase the time differences
	return time.Duration(
		(numConsecutiveErrsForOverthrow + 1) * driftRange * float64(tc.ManageOracleTickInterval),
	), nil
}

// Recover recovers from the failure policy.
func (s *StopOraclePolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	if s.oldOracle >= 0 && s.oldOracle < len(tc.Nodes) {
		if err := tc.RunOperation(ctx, cluster.Start, s.oldOracle); err != nil {
			return err
		}
		time.Sleep(3 * tc.ManageOracleTickInterval)
	}
	return nil
}

// RestartOraclePolicy restarts the oracle.
type RestartOraclePolicy struct{}

func (r *RestartOraclePolicy) String() string {
	return "RestartOraclePolicy"
}

// Inject injects the failure policy.
func (r *RestartOraclePolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	oracle, err := tc.Oracle(ctx, false /*checkOnlyForRunningNodes*/)
	if err != nil {
		return 0, err
	}
	if err := tc.RunOperation(ctx, cluster.Restart, oracle); err != nil {
		return 0, err
	}
	// sleep to let the cluster come in stable state where the oracle has been
	// chosen
	time.Sleep(10 * tc.ManageOracleTickInterval)
	return 0, nil
}

// Recover recovers from the failure policy.
func (r *RestartOraclePolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	return nil
}

// RestartAllPolicy restart all the nodes part of kronos cluster.
type RestartAllPolicy struct{}

func (r *RestartAllPolicy) String() string {
	return "RestartAllPolicy"
}

// Inject injects the failure policy.
func (r *RestartAllPolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	allIndices := allIndices(len(tc.Nodes))
	if err := tc.RunOperation(
		ctx,
		cluster.Restart,
		allIndices...,
	); err != nil {
		return 0, err
	}
	time.Sleep(10 * tc.ManageOracleTickInterval)
	return 0, nil
}

// Recover recovers from the failure policy.
func (r *RestartAllPolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	return nil
}

// DriftOraclePolicy introduces drift in the time of the oracle.
type DriftOraclePolicy struct{}

func (d *DriftOraclePolicy) String() string {
	return "DriftOraclePolicy"
}

// Inject injects the failure policy.
func (d *DriftOraclePolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	oracle, err := tc.Oracle(ctx, false /*checkOnlyForRunningNodes*/)
	if err != nil {
		return 0, err
	}
	if err := driftNodes(ctx, tc, oracle); err != nil {
		return 0, err
	}
	time.Sleep(10 * tc.ManageOracleTickInterval)
	return 0, nil
}

// Recover recovers from the failure policy.
func (d *DriftOraclePolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	// not recovering from drift failure policy to make the test harder
	return nil
}

// DriftAllPolicy introduces drift on all the nodes part of kronos cluster.
type DriftAllPolicy struct{}

func (d *DriftAllPolicy) String() string {
	return "DriftAllPolicy"
}

// Inject injects the failure policy.
func (d *DriftAllPolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	allIndices := allIndices(len(tc.Nodes))
	if err := driftNodes(ctx, tc, allIndices...); err != nil {
		return 0, nil
	}
	time.Sleep(10 * tc.ManageOracleTickInterval)
	return 0, nil
}

// Recover recovers from the failure policy.
func (d *DriftAllPolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	// not recovering from drift failure policy to make the test harder
	return nil
}

// AddRemovePolicy removes a node from the cluster, wipes it's data directory
// and it back to the cluster as a new node.
type AddRemovePolicy struct {
	nodeToRemove int
}

func (d *AddRemovePolicy) String() string {
	return "AddRemovePolicy"
}

// Inject injects the failure  policy.
func (d *AddRemovePolicy) Inject(
	ctx context.Context, tc *cluster.TestCluster,
) (time.Duration, error) {
	// Currently, remove node is naive and just removes the node from the cluster.
	// To remove a seed host, we need to elect a new seed host and change the
	// procfile to reflect the same. Therefore, this test only supports removing
	// non-seedhosts from the cluster.
	// Getting a number from [2, num_nodes-1) to avoid removing seedHosts.
	d.nodeToRemove = rand.Intn(len(tc.Nodes)-2) + 2
	log.Infof(ctx, "Removing node %d", d.nodeToRemove)
	if err := tc.RemoveNode(ctx, d.nodeToRemove); err != nil {
		return 0, err
	}
	time.Sleep(10 * tc.ManageOracleTickInterval)
	return 0, nil
}

// Recover recovers from the failure policy.
func (d *AddRemovePolicy) Recover(ctx context.Context, tc *cluster.TestCluster) error {
	if _, err := tc.AddNode(ctx, d.nodeToRemove); err != nil {
		return nil
	}
	time.Sleep(6 * tc.ManageOracleTickInterval)
	return nil
}
