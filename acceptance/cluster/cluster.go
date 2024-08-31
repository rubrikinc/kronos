package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/rubrikinc/kronos/cli"
	"github.com/stretchr/testify/assert"
	"testing"

	"fmt"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rubrikinc/failure-test-utils/failuregen"
	tcpLog "github.com/rubrikinc/failure-test-utils/log"
	"github.com/rubrikinc/failure-test-utils/tcpproxy"
	"github.com/rubrikinc/kronos/checksumfile"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/rubrikinc/kronos/syncutil"

	"github.com/rubrikinc/kronos/acceptance/testutil"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

// We are using goreman to control the different kronos processes. goreman uses
// a procfile to get the list of processIds and processes to be run and control.
// It supports various operation like start, stop and restart a single process.

const (
	procfile  = "Procfile"
	localhost = "127.0.0.1"
)

// ErrMultipleOracles is returned in case different nodes consider different
// servers as oracle. It contains the list of oracles according to each node.
type ErrMultipleOracles struct {
	addresses []int
}

func (e *ErrMultipleOracles) Error() string {
	return fmt.Sprintf("expected same oracle on all nodes, got %#v", e.addresses)
}

// Operation denotes the operation supported by goreman on clusters
type Operation uint16

const (
	// Start a node
	Start Operation = iota
	// Stop a node
	Stop
	// Restart a node
	Restart
)

func (op Operation) String() string {
	switch op {
	case Start:
		return "start"
	case Stop:
		return "stop"
	case Restart:
		return "restart"
	default:
		panic(errors.Errorf("unknown operation %d", op))
	}
}

type TestNode struct {
	Id            string
	DataDirectory string
	LogDir        string
	ListenHost    string
	AdvertiseHost string
	RaftPort      string
	GrpcPort      string
	DriftPort     string
	PprofAddr     string
	KronosBinary  string
	IsRunning     bool
	Mu            *syncutil.RWMutex
	driftConfig   *kronospb.DriftTimeConfig
}

func (t *TestNode) DataDir() string {
	return t.DataDirectory
}

func (t *TestNode) kronosStartCmd(
	kronosBinary string,
	seedHosts []string,
	gossipSeeds []string,
	manageOracleTickInterval time.Duration,
	certsDir string,
	raftSnapCount uint64,
	leaderNotOracle bool,
) string {
	kronosCmd := []string{
		"KRONOS_NODE_ID=" + t.Id,
		"GODEBUG=netdns=cgo",
		fmt.Sprintf("LD_PRELOAD=%v", os.Getenv("PROXY_AWARE_RESOLVER")),
		kronosBinary,
		"start",
		"--advertise-host", t.AdvertiseHost,
		"--listen-addr", t.ListenHost,
		"--raft-port", t.RaftPort,
		"--grpc-port", t.GrpcPort,
		"--gossip-seed-hosts", strings.Join(gossipSeeds, ","),
		"--use-drift-clock",
		"--drift-port", t.DriftPort,
		"--data-dir", t.DataDirectory,
		"--pprof-addr", t.PprofAddr,
		"--seed-hosts", strings.Join(seedHosts, ","),
		"--manage-oracle-tick-interval", manageOracleTickInterval.String(),
		"--raft-snap-count", fmt.Sprint(raftSnapCount),
		"--test-mode", fmt.Sprint(true),
	}

	if len(certsDir) > 0 {
		kronosCmd = append(kronosCmd, "--certs-dir", certsDir)
	}

	if leaderNotOracle {
		kronosCmd = append([]string{"LEADER_NOT_ORACLE=true"}, kronosCmd...)
	}

	return strings.Join(kronosCmd, " ")
}

// ClusterTime is the time on all the nodes.
type ClusterTime map[int]int64

// Relative gives the relative time of all the nodes in the cluster, wrt the
// minimum time on the nodes. eg.[1,2,3] - [0,1,2]. It is used for pretty
// printing of ClusterTime.
func (ct ClusterTime) Relative() map[int]time.Duration {
	var minTime int64 = math.MaxInt64
	for _, t := range ct {
		if t < minTime {
			minTime = t
		}
	}
	durations := make(map[int]time.Duration)
	for i, t := range ct {
		durations[i] = time.Duration(t - minTime)
	}
	return durations
}

// Since gives the time passed on all the nodes since pastTime was captured.
func (ct ClusterTime) Since(pastTime ClusterTime) map[int]time.Duration {
	nodeIdxToDuration := make(map[int]time.Duration)
	for nodeIdx, lastTime := range pastTime {
		currTime, currTimeExists := ct[nodeIdx]
		if currTimeExists {
			nodeIdxToDuration[nodeIdx] = time.Duration(currTime - lastTime)
		}
	}
	return nodeIdxToDuration
}

// TestCluster is a kronos test cluster, that can be used to test kronos in
// various scenarios.
type TestCluster struct {
	Nodes                    []*TestNode
	ManageOracleTickInterval time.Duration
	CertsDir                 string
	RaftSnapCount            uint64
	ErrCh                    chan error
	LeaderNotOracle          bool
	Fs                       afero.Fs
	kronosBinary             string
	Procfile                 string
	seedHosts                []string
	gossipSeedHosts          []string
	goremanCmd               *exec.Cmd
	goremanPort              string
	grpcProxy                [][]tcpproxy.TCPProxy
	raftProxy                [][]tcpproxy.TCPProxy
	testDir                  string
}

func (tc *TestCluster) runGoremanWithArgs(args ...string) (string, error) {
	goremanBinary, err := absoluteBinaryPath("goreman")
	if err != nil {
		return "", err
	}
	goremanCmd := exec.Command(goremanBinary, args...)
	// goreman does not respect -p (port) for running cluster operations.
	// Hence environment variable is used
	goremanCmd.Env = os.Environ()
	goremanCmd.Env = append(goremanCmd.Env, fmt.Sprintf("GOREMAN_RPC_PORT=%v", tc.goremanPort))
	output, err := goremanCmd.CombinedOutput()
	return string(output), err
}

// Stop stops the testCluster by terminating the goreman command.
func (tc *TestCluster) Stop(ctx context.Context) error {
	if err := tc.goremanCmd.Process.Signal(syscall.SIGTERM); err != nil {
		log.Errorf(ctx, "Error while terminating goreman: %v", err)
		if err = tc.goremanCmd.Process.Kill(); err != nil {
			return err
		}
	}
	return nil
}

// RunOperation can be used to start, stop, restart nodes of test cluster tc.
func (tc *TestCluster) RunOperation(ctx context.Context, op Operation, indices ...int) error {
	if indices == nil {
		return nil
	}
	for _, index := range indices {
		log.Infof(ctx, "Running %s on node %d", op, index)
		tc.Nodes[index].Mu.Lock()
		defer tc.Nodes[index].Mu.Unlock()
		output, err := tc.runGoremanWithArgs(
			"run",
			op.String(),
			tc.Nodes[index].Id,
		)
		if err != nil {
			return errors.Wrapf(err, "output: %s", output)
		}
		switch op {
		case Start, Restart:
			tc.Nodes[index].IsRunning = true
		case Stop:
			tc.Nodes[index].IsRunning = false
		default:
			return errors.Errorf("unsupported value of op %v", op)
		}
	}
	return nil
}

// IsRunning is used to check if nodeIdx node is running or has been stopped.
func (tc *TestCluster) IsRunning(nodeIdx int) bool {
	tc.Nodes[nodeIdx].Mu.RLock()
	defer tc.Nodes[nodeIdx].Mu.RUnlock()
	return tc.Nodes[nodeIdx].IsRunning
}

// UpdateClockConfig is used to update the drifting clock config for nodeidx
// node.
func (tc *TestCluster) UpdateClockConfig(
	ctx context.Context, nodeIdx int, config *kronospb.DriftTimeConfig,
) error {
	dialOpts := grpc.WithInsecure()
	conn, err := grpc.Dial(
		net.JoinHostPort(tc.Nodes[nodeIdx].ListenHost, tc.Nodes[nodeIdx].DriftPort),
		dialOpts,
	)
	if err != nil {
		return err
	}
	defer kronosutil.CloseWithErrorLog(ctx, conn)
	driftClient := kronospb.NewUpdateDriftTimeServiceClient(conn)
	_, err = driftClient.UpdateDriftConfig(ctx, config)
	tc.Nodes[nodeIdx].driftConfig = config
	return err
}

// GetClockConfig is used to get the drifting clock config for nodeidx
// node.
func (tc *TestCluster) GetClockConfig(ctx context.Context, nodeIdx int) *kronospb.DriftTimeConfig {
	return tc.Nodes[nodeIdx].driftConfig
}

// Time is used to get kronos time of node nodeIdx.
func (tc *TestCluster) Time(ctx context.Context, nodeIdx int) (int64, error) {
	timeClient := server.NewGRPCClient(tc.CertsDir)
	defer timeClient.Close()
	tr, err := timeClient.KronosTime(
		ctx,
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].ListenHost, Port: tc.Nodes[nodeIdx].GrpcPort},
	)
	if err != nil {
		return -1, err
	}
	return tr.Time, nil
}

// Bootstrap is used to bootstrap the kronos cluster
func (tc *TestCluster) Bootstrap(ctx context.Context, nodeIdx int) error {
	bootstrapClient := server.NewGRPCClient(tc.CertsDir)
	defer bootstrapClient.Close()
	_, err := bootstrapClient.Bootstrap(ctx,
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].ListenHost, Port: tc.Nodes[nodeIdx].
			GrpcPort}, &kronospb.BootstrapRequest{ExpectedNodeCount: int32(len(tc.Nodes))})
	return err
}

// ValidateTimeInConsensus validates time across the cluster(difference between
// maxTime and minTime) is within maxDiffAllowed for the running nodes. It
// returns ClusterTime and uptime which is a map of NodeID to time / uptime.
func (tc *TestCluster) ValidateTimeInConsensus(
	ctx context.Context, maxDiffAllowed time.Duration, checkOnlyRunningNodes bool,
) (ClusterTime, ClusterTime, error) {
	var nodesToValidate []string
	for nodeIdx, node := range tc.Nodes {
		if checkOnlyRunningNodes && !tc.IsRunning(nodeIdx) {
			continue
		}
		nodesToValidate = append(
			nodesToValidate,
			net.JoinHostPort(node.ListenHost, node.GrpcPort),
		)
	}
	addressToTime, addressToUptime, err := tc.validateTimeInConsensus(
		ctx,
		maxDiffAllowed,
		nodesToValidate,
		tc.CertsDir,
	)

	return addressToTime, addressToUptime, err
}

func (tc *TestCluster) index(address string) int {
	for idx, node := range tc.Nodes {
		nodeAddress := net.JoinHostPort(node.ListenHost, node.GrpcPort)
		if nodeAddress == address {
			return idx
		}
	}
	return -1
}

// OracleForNode is used to get the current oracle according to node nodeIdx.
func (tc *TestCluster) OracleForNode(ctx context.Context, nodeIdx int) (int, error) {
	timeClient := server.NewGRPCClient(tc.CertsDir)
	defer timeClient.Close()
	status, err := timeClient.Status(
		ctx,
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].ListenHost, Port: tc.Nodes[nodeIdx].GrpcPort},
	)
	if err != nil {
		return -1, err
	}
	addr := net.JoinHostPort(
		status.OracleState.Oracle.Host,
		status.OracleState.Oracle.Port,
	)
	idx := -1
	for i, node := range tc.Nodes {
		if addr == net.JoinHostPort(node.AdvertiseHost, node.GrpcPort) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return -1, errors.Errorf("node address %s not found in cluster", addr)
	}
	return idx, nil
}

// NodeID returns the raft Id of the idx node. It reads cluster
// metadata of idx node to get the same.
func (tc *TestCluster) NodeID(idx int) (string, error) {
	id, err := metadata.FetchNodeID(tc.Nodes[idx].DataDirectory)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// RemoveNode removes a node from testCluster and wipes it's data directory
func (tc *TestCluster) RemoveNode(ctx context.Context, idx int, nodeToRunRemoveFrom int, nodeID string) error {
	if nodeToRunRemoveFrom == -1 {
		r, _ := checksumfile.NewPseudoRand()
		if r.Float32() < 0.5 {
			// Test remove node from the node being removed
			nodeToRunRemoveFrom = idx
		} else {
			// Test remove node from a node not being removed (idx of 0 or 1)
			if idx == 0 {
				nodeToRunRemoveFrom = 1
			} else {
				nodeToRunRemoveFrom = 0
			}
		}
	}

	var err error
	if idx != -1 {
		nodeID, err = tc.NodeID(idx)
	}
	log.Infof(ctx, "Removing node %v from node %d", nodeID, nodeToRunRemoveFrom)
	if err != nil {
		return err
	}
	removeArgs := []string{
		"cluster",
		"remove",
		nodeID,
		"--host",
		fmt.Sprintf("%s:%s", tc.Nodes[nodeToRunRemoveFrom].ListenHost, tc.Nodes[nodeToRunRemoveFrom].RaftPort),
		"--certs-dir", tc.CertsDir,
	}
	if output, err := exec.Command(
		tc.kronosBinary, removeArgs...,
	).CombinedOutput(); err != nil {
		return errors.Wrapf(err, "Output: %s", string(output))
	}
	if idx != -1 {
		if err := tc.RunOperation(ctx, Stop, idx); err != nil {
			return err
		}
		// deleting the data directory, as we don't support recommission of node to
		// kronos cluster yet.
		return tc.Fs.RemoveAll(tc.Nodes[idx].DataDirectory)
	} else {
		return nil
	}
}

// AddNode adds a new node to testCluster and returns the newly assigned NodeID.
func (tc *TestCluster) AddNode(ctx context.Context, idx int) (string, error) {
	log.Infof(ctx, "Adding node %d to the cluster", idx)
	if _, err := os.Stat(tc.Nodes[idx].DataDirectory); err == nil || os.IsExist(err) {
		return "", errors.New("data directory for node being added already exists")
	}
	if err := tc.Fs.Mkdir(tc.Nodes[idx].DataDirectory, 0755); err != nil {
		return "", err
	}
	if err := tc.RunOperation(ctx, Start, idx); err != nil {
		return "", err
	}
	// Wait for sometime to let the node-info file be created for the node.
	time.Sleep(time.Second)
	id, err := metadata.FetchNodeID(tc.Nodes[idx].DataDirectory)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// IsNodeRemoved checks if node idx is removed according to the metadata of
// node sourceNode.
func (t *TestNode) IsNodeRemoved(id string) (bool, error) {
	c, err := metadata.LoadCluster(t.DataDirectory, true /* readOnly */)
	if err != nil {
		return true, err
	}
	for nodeID, node := range c.NodesIncludingRemoved() {
		if nodeID == id {
			return node.IsRemoved, nil
		}
	}
	return true, errors.Errorf(
		"node %s not found in cluster metadata",
		id,
	)
}

// Oracle validates that all the nodes in the cluster think that a
// single node is the oracle and returns that oracle. If two or more nodes
// consider different nodes as oracle, then this will return an error with
// what every node thinks is the oracle.
func (tc *TestCluster) Oracle(
	ctx context.Context, checkOnlyRunningNodes bool,
) (oracleIdx int, err error) {
	if len(tc.Nodes) == 0 {
		return -1, nil
	}
	var oracles []int
	multipleOracle := false
	for i, node := range tc.Nodes {
		if checkOnlyRunningNodes && !node.IsRunning {
			continue
		}
		var oracleForI int
		oracleForI, err = tc.OracleForNode(ctx, i)
		if err != nil {
			return -1, err
		}
		if i == 0 {
			oracleIdx = oracleForI
		} else if oracleForI != oracleIdx {
			multipleOracle = true
		}
		oracles = append(oracles, oracleForI)
	}
	if multipleOracle {
		return -1, &ErrMultipleOracles{addresses: oracles}
	}
	return oracleIdx, nil
}

// Backup backs up cluster metadata of node idx
func (tc *TestCluster) Backup(ctx context.Context, idx int) error {
	backupArgs := []string{
		"cluster",
		"backup",
		"--data-dir", tc.Nodes[idx].DataDir(),
	}
	backupCmd := exec.Command(tc.kronosBinary, backupArgs...)
	if err := backupCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Error(ctx, backupArgs, exitErr.Stderr)
			return errors.Wrap(err, string(exitErr.Stderr))
		}
		return err
	}
	return nil
}

// Restore restores cluster metadata of node idx backed up a previous backup
func (tc *TestCluster) Restore(ctx context.Context, idx int) error {
	restoreArgs := []string{
		"cluster",
		"restore",
		"--data-dir", tc.Nodes[idx].DataDir(),
	}
	restoreCmd := exec.Command(tc.kronosBinary, restoreArgs...)
	if err := restoreCmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Error(ctx, restoreArgs, exitErr.Stderr)
			return errors.Wrap(err, string(exitErr.Stderr))
		}
		return err
	}
	return nil
}

func (tc *TestCluster) destroyProxies() {
	for i := 0; i < len(tc.Nodes); i++ {
		for j := 0; j < len(tc.Nodes); j++ {
			if i == j {
				continue
			}
			if tc.grpcProxy != nil && tc.grpcProxy[i][j] != nil {
				tc.grpcProxy[i][j].Stop()
			}
			if tc.raftProxy != nil && tc.raftProxy[i][j] != nil {
				tc.raftProxy[i][j].Stop()
			}
		}
	}
}

// ReIP simulates re-ip in testcluster by changing raft ports of all the nodes
// in the cluster.
func (tc *TestCluster) ReIP(ctx context.Context) error {
	if err := tc.Stop(ctx); err != nil {
		return err
	}
	tc.destroyProxies()
	freePorts, err := testutil.GetFreePorts(ctx, len(tc.Nodes))
	if err != nil {
		return err
	}
	oldToNewAddr := make(map[string]string)
	addr := func(host, port string) string { return host + ":" + port }
	for i, node := range tc.Nodes {
		newPort := fmt.Sprint(freePorts[i])
		name := "kronos" + strconv.Itoa(i+1)
		oldToNewAddr[addr(name, node.RaftPort)] = addr(name, newPort)
		node.RaftPort = newPort
	}
	var newSeedHosts []string
	for _, seed := range tc.seedHosts {
		newSeedHosts = append(newSeedHosts, oldToNewAddr[seed])
	}
	tc.seedHosts = newSeedHosts

	if err = tc.generateProcfile(ctx, nil); err != nil {
		return err
	}

	if err = tc.createProxies(ctx); err != nil {
		return err
	}

	if err = tc.Start(ctx); err != nil {
		return err
	}
	log.Info(ctx, "Cluster ReIP successful.")
	return nil
}

// Status returns kronos status fetched via hostIdx
func (tc *TestCluster) Status(hostIdx int, local bool) ([]byte, error) {
	statusArgs := []string{
		"status",
		"--format", "json",
		"--raft-addr", fmt.Sprintf("%s:%s", tc.Nodes[hostIdx].AdvertiseHost, tc.Nodes[hostIdx].RaftPort),
		fmt.Sprintf("--local=%t", local),
	}
	if len(tc.CertsDir) > 0 {
		statusArgs = append(statusArgs, "--certs-dir", tc.CertsDir)
	}
	statusCmd := exec.Command(tc.kronosBinary, statusArgs...)
	var stdout, stderr bytes.Buffer
	statusCmd.Stdout = &stdout
	statusCmd.Stderr = &stderr
	envs := os.Environ()
	envs = append(envs, "GODEBUG=netdns=cgo")
	envs = append(envs, "LD_PRELOAD="+os.Getenv("PROXY_AWARE_RESOLVER"))
	envs = append(envs, "KRONOS_NODE_ID=test")
	statusCmd.Env = envs
	err := statusCmd.Run()
	if err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return nil, err
		}
		if stderr.Len() != 0 {
			return nil, err
		}
	}
	return stdout.Bytes(), nil
}

func (tc *TestCluster) generateProcfile(ctx context.Context, addEnvs map[int]string) error {
	pf, err := tc.Fs.OpenFile(tc.Procfile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer pf.Close()
	for i := 0; i < len(tc.Nodes); i++ {
		envVars := ""
		if addEnvs != nil {
			if env, ok := addEnvs[i]; ok {
				envVars = env
			}
		}
		_, err = pf.WriteString(
			fmt.Sprintf(
				"%s: %s %s 2>>%s 1>>%s\n",
				tc.Nodes[i].Id,
				envVars,
				tc.Nodes[i].kronosStartCmd(
					tc.Nodes[i].KronosBinary,
					tc.seedHosts,
					tc.gossipSeedHosts,
					tc.ManageOracleTickInterval,
					tc.CertsDir,
					tc.RaftSnapCount,
					tc.LeaderNotOracle,
				),
				filepath.Join(tc.Nodes[i].LogDir, "kronos-stderr.log"),
				filepath.Join(tc.Nodes[i].LogDir, "kronos-stdout.log"),
			),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tc *TestCluster) Start(ctx context.Context) error {
	goremanBinary, err := absoluteBinaryPath("goreman")
	if err != nil {
		return err
	}
	if _, err = tc.Fs.Stat(goremanBinary); err != nil {
		if os.IsNotExist(err) {
			log.Info(ctx, "goreman binary is missing. Make sure $GOPATH/bin is in $PATH")
			return err
		} else {
			return err
		}
	}
	if tc.goremanCmd != nil {
		tc.goremanCmd.Wait()
	}
	if tc.goremanPort == "" {
		ports, err := testutil.GetFreePorts(ctx, 1)
		if err != nil {
			return err
		}
		tc.goremanPort = strconv.Itoa(ports[0])
	}

	tc.goremanCmd = exec.Command(
		goremanBinary,
		"-p",
		tc.goremanPort,
		"-f",
		tc.Procfile,
		"-exit-on-stop=false",
		"start",
	)
	if err := tc.goremanCmd.Start(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return errors.Wrap(err, string(exitErr.Stderr))
		}
		return err
	}

	return nil
}

// Close closes the testcluster.
func (tc *TestCluster) Close() error {
	if err := tc.Stop(context.Background()); err != nil {
		return err
	}
	for _, node := range tc.Nodes {
		if err := tc.Fs.RemoveAll(node.DataDirectory); err != nil {
			return err
		}
	}
	tc.destroyProxies()
	return nil
}

// ClusterConfig describes the config to start testcluster with.
type ClusterConfig struct {
	Fs                       afero.Fs
	NumNodes                 int
	ManageOracleTickInterval time.Duration
	RaftSnapCount            uint64
	LeaderNotOracle          bool
}

// NewInsecureCluster returns an instance of a test kronos cluster. It returns
// - The test_cluster
// - Error if any while creating the testcluster
// TestCluster should be closed using Close method once test is finished.
// This returns a cluster which runs in insecure mode.
func NewInsecureCluster(ctx context.Context, cc ClusterConfig) (*TestCluster, error) {
	return newCluster(ctx, cc, true)
}

// NewCluster returns an instance of a test kronos cluster. It returns
// - The test_cluster
// - Error if any while creating the testcluster
// TestCluster should be closed using Close method once test is finished.
// This returns a cluster which runs in secure mode.
func NewCluster(ctx context.Context, cc ClusterConfig) (*TestCluster, error) {
	//return newCluster(ctx, cc, false /* insecure */)
	return newCluster(ctx, cc, true /* insecure */)
}

func (tc *TestCluster) createProxy(ctx context.Context, i, j int) error {
	proxy, err := tcpproxy.NewTCPProxy(
		ctx,
		fmt.Sprintf("127.0.%d.%d:%s", i+1, j+1, tc.Nodes[j].RaftPort),
		fmt.Sprintf("127.0.0.%d:%s", j+1, tc.Nodes[j].RaftPort),
		failuregen.NewFailureGenerator(),
		failuregen.NewFailureGenerator(),
	)
	tc.raftProxy[i][j] = proxy
	if err != nil {
		return err
	}
	proxy, err = tcpproxy.NewTCPProxy(
		ctx,
		fmt.Sprintf("127.0.%d.%d:%s", i+1, j+1, tc.Nodes[j].GrpcPort),
		fmt.Sprintf("127.0.0.%d:%s", j+1, tc.Nodes[j].GrpcPort),
		failuregen.NewFailureGenerator(),
		failuregen.NewFailureGenerator(),
	)
	tc.grpcProxy[i][j] = proxy
	if err != nil {
		return err
	}
	return nil
}

func (tc *TestCluster) createProxies(ctx context.Context) error {
	for i := 0; i < len(tc.Nodes); i++ {
		for j := i + 1; j < len(tc.Nodes); j++ {
			if err := tc.createProxy(ctx, i, j); err != nil {
				return err
			}
			if err := tc.createProxy(ctx, j, i); err != nil {
				return err
			}
		}
	}
	return nil
}

// newCluster returns an instance of a test kronos cluster. It returns
// - The test_cluster
// - Error if any while creating the testcluster
// TestCluster should be closed using Close method once test is finished.
func newCluster(ctx context.Context, cc ClusterConfig, insecure bool) (*TestCluster, error) {
	testDir, err := afero.TempDir(cc.Fs, "", "kronos_test_dir_")
	if err != nil {
		return nil, err
	}
	var certsDir string
	if !insecure {
		//certsDir = filepath.Join(testDir, "certs_dir")
		//// The certs related constants are taken from
		//// pkg/acceptance/cluster/dockercluster.go
		//const keyLen = 1024
		//err = security.CreateCAPair(
		//	certsDir,
		//	filepath.Join(certsDir, security.EmbeddedCAKey),
		//	keyLen,
		//	96*time.Hour,
		//	false, /* allowKeyReuse */
		//	false, /* overwrite */
		//)
		//if err != nil {
		//	return nil, err
		//}
		//err = security.CreateNodePair(
		//	certsDir,
		//	filepath.Join(certsDir, security.EmbeddedCAKey),
		//	keyLen,
		//	48*time.Hour,
		//	false, /* overwrite */
		//	[]string{localhost},
		//)
		//if err != nil {
		//	return nil, err
		//}
		return nil, errors.New("Can't get secure cluster")
	}

	tc := &TestCluster{
		CertsDir:                 certsDir,
		Fs:                       cc.Fs,
		ErrCh:                    make(chan error),
		ManageOracleTickInterval: cc.ManageOracleTickInterval,
		Nodes:                    make([]*TestNode, cc.NumNodes),
		Procfile:                 filepath.Join(testDir, procfile),
		RaftSnapCount:            cc.RaftSnapCount,
		testDir:                  testDir,
		LeaderNotOracle:          cc.LeaderNotOracle,
	}
	log.Infof(ctx, "Procfile: %v", tc.Procfile)

	// 0 -> goreman port
	// 1 -> raft port,
	// 2 -> grpc port,
	// 3 -> DriftPort
	// 4 -> pprofPort
	freePorts, err := testutil.GetFreePorts(ctx, 5)
	if err != nil {
		return nil, err
	}
	tc.goremanPort = strconv.Itoa(freePorts[0])

	const numSeedHosts = 2
	for i := 0; i < numSeedHosts; i++ {
		tc.seedHosts = append(
			tc.seedHosts,
			fmt.Sprintf("kronos%d:%s", i+1, strconv.Itoa(freePorts[1])),
		)
		tc.gossipSeedHosts = append(tc.gossipSeedHosts,
			fmt.Sprintf("kronos%d:%s", i+1, strconv.Itoa(freePorts[2])))
	}

	for i := 0; i < cc.NumNodes; i++ {
		kronosBinary, err := absoluteBinaryPath("kronos")
		if err != nil {
			return nil, err
		}
		log.Infof(ctx, "Kronos binary path: %v", kronosBinary)
		tc.kronosBinary = kronosBinary
		tc.Nodes[i] = &TestNode{
			Id:            fmt.Sprintf("%d", i+1),
			DataDirectory: filepath.Join(testDir, fmt.Sprintf("data_dir_%d", i)),
			LogDir:        filepath.Join(testDir, fmt.Sprintf("log_dir_%d", i)),
			AdvertiseHost: fmt.Sprintf("kronos%d", i+1),
			ListenHost:    fmt.Sprintf("127.0.0.%d", i+1),
			RaftPort:      strconv.Itoa(freePorts[1]),
			GrpcPort:      strconv.Itoa(freePorts[2]),
			DriftPort:     strconv.Itoa(freePorts[3]),
			PprofAddr:     strconv.Itoa(freePorts[4]),
			IsRunning:     true,
			Mu:            &syncutil.RWMutex{},
			KronosBinary:  kronosBinary,
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].DataDirectory, 0755); err != nil {
			return nil, err
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].LogDir, 0755); err != nil {
			return nil, err
		}
	}

	tcpLog.SetLogger(log.NoOplogger)

	tc.grpcProxy = make([][]tcpproxy.TCPProxy, cc.NumNodes)
	tc.raftProxy = make([][]tcpproxy.TCPProxy, cc.NumNodes)

	for i := 0; i < cc.NumNodes; i++ {
		tc.grpcProxy[i] = make([]tcpproxy.TCPProxy, cc.NumNodes)
		tc.raftProxy[i] = make([]tcpproxy.TCPProxy, cc.NumNodes)
	}

	if err := tc.createProxies(ctx); err != nil {
		return nil, err
	}

	if err = tc.generateProcfile(ctx, nil); err != nil {
		return nil, err
	}

	if err = tc.Start(ctx); err != nil {
		return nil, err
	}

	for err = tc.Bootstrap(ctx, 0); err != nil; err = tc.Bootstrap(ctx, 0) {
		log.Infof(ctx, "Waiting for the cluster to bootstrap err : %v", err)
		time.Sleep(time.Second)
	}

	return tc, nil
}

// validateTimeInConsensus validates that the kronos time across the given nodes
// (difference between maxTime and minTime) is within maxDiffAllowed.
// Returns a map of node Id to time, node Id to uptime, and error (if any)
func (tc *TestCluster) validateTimeInConsensus(
	ctx context.Context, maxDiffAllowed time.Duration, nodeAddresses []string, certsDir string,
) (ClusterTime, ClusterTime, error) {
	var mu syncutil.RWMutex
	timeOnNodes := make(ClusterTime)
	uptimeOnNodes := make(ClusterTime)
	eg, ctx := errgroup.WithContext(ctx)
	var wg sync.WaitGroup
	wg.Add(len(nodeAddresses))
	for _, address := range nodeAddresses {
		// making a copy to avoid
		// https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		currAddress := address
		host, port, err := net.SplitHostPort(currAddress)
		if err != nil {
			return nil, nil, err
		}
		nodeAddr := &kronospb.NodeAddr{
			Host: host,
			Port: port,
		}
		eg.Go(
			func() error {
				timeClient := server.NewGRPCClient(certsDir)
				defer timeClient.Close()
				// Make a first call to KronosTime and wait for all clients to return
				// using wg. This is so that we warm up the connection for the next
				// call to KronosTime to different nodes to be very close to each other
				// in wall clock time.
				_, err := timeClient.KronosTime(ctx, nodeAddr)
				if err != nil {
					log.Errorf(
						ctx, "Failed to get KronosTime from address %s, error: %v",
						currAddress, err,
					)
					wg.Done()
					return err
				}
				wg.Done()
				wg.Wait()
				tr, err := timeClient.KronosTime(ctx, nodeAddr)
				if err != nil {
					log.Errorf(
						ctx, "Failed to get KronosTime from address %s, error: %v",
						currAddress, err,
					)
					return err
				}
				utr, err := timeClient.KronosUptime(ctx, nodeAddr)
				if err != nil {
					log.Errorf(
						ctx, "Failed to get KronosUptime from address %s, error: %v",
						currAddress, err,
					)
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				timeOnNodes[tc.index(currAddress)] = tr.Time
				uptimeOnNodes[tc.index(currAddress)] = utr.Uptime
				return err
			},
		)
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	// Validate time on nodes.
	if err := kronosutil.ValidateTimeInConsensus(ctx, maxDiffAllowed, timeOnNodes); err != nil {
		log.Errorf(ctx, "Time validation failed: %v", timeOnNodes.Relative())
		return nil, nil, err
	}
	// Validate uptime on nodes.
	if err := kronosutil.ValidateTimeInConsensus(ctx, maxDiffAllowed, uptimeOnNodes); err != nil {
		log.Errorf(ctx, "Uptime validation failed: %v", uptimeOnNodes.Relative())
		return nil, nil, err
	}
	return timeOnNodes, uptimeOnNodes, nil
}

func (tc *TestCluster) ExchangeDataDir(ctx context.Context, id1 int,
	id2 int) error {
	err := tc.RunOperation(ctx, Stop, id1, id2)
	if err != nil {
		return err
	}

	// Exchange data dir
	tmpDataDir := tc.Nodes[id1].DataDirectory + "_tmp"
	err = tc.Fs.Rename(tc.Nodes[id1].DataDirectory, tmpDataDir)
	if err != nil {
		return err
	}
	err = tc.Fs.Rename(tc.Nodes[id2].DataDirectory, tc.Nodes[id1].DataDirectory)
	if err != nil {
		return err
	}
	err = tc.Fs.Rename(tmpDataDir, tc.Nodes[id2].DataDirectory)
	if err != nil {
		return err
	}

	err = tc.RunOperation(ctx, Start, id1, id2)
	if err != nil {
		return err
	}

	return nil
}

func (tc *TestCluster) Disconnect(oracle int, i int) {
	tc.raftProxy[oracle][i].BlockAllTraffic()
	tc.grpcProxy[oracle][i].BlockAllTraffic()
	tc.raftProxy[i][oracle].BlockAllTraffic()
	tc.grpcProxy[i][oracle].BlockAllTraffic()
}

func (tc *TestCluster) Connect(oracle int, i int) {
	tc.raftProxy[oracle][i].UnblockAllTraffic()
	tc.grpcProxy[oracle][i].UnblockAllTraffic()
	tc.raftProxy[i][oracle].UnblockAllTraffic()
	tc.grpcProxy[i][oracle].UnblockAllTraffic()
}

func (tc *TestCluster) ProcessNodePairs(filter func(int, int) bool, effect func(int, int)) {
	for i := 0; i < len(tc.Nodes); i++ {
		for j := i + 1; j < len(tc.Nodes); j++ {
			if filter(i, j) {
				effect(i, j)
			}
		}
	}
}

func (tc *TestCluster) findNodeWithRaftId(t *testing.T, id string) int {
	for idx := range tc.Nodes {
		raftId, err := tc.NodeID(idx)
		if err != nil {
			t.Fatal(err)
		}
		if raftId == id {
			return idx
		}
	}
	t.Fatalf("node with Id %v not found", id)
	return -1
}

func (tc *TestCluster) FindLeader(t *testing.T, a *assert.Assertions) int {
	data, err := tc.Status(0, false /*local*/)
	a.NoError(err)
	var nodeInfos []cli.NodeInfo
	err = json.Unmarshal(data, &nodeInfos)
	a.NoError(err)
	leader := -1
	for _, nodeInfo := range nodeInfos {
		if nodeInfo.RaftLeader {
			leader = tc.findNodeWithRaftId(t, nodeInfo.ID)
		}
	}
	a.NotEqual(-1, leader)
	return leader
}

func (tc *TestCluster) AddEnv(ctx context.Context, node int, s string) {
	mp := make(map[int]string)
	mp[node] = s
	tc.generateProcfile(ctx, mp)
}
