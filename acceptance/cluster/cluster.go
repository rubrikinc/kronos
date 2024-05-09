package cluster

import (
	"bytes"
	"context"
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

type testNode struct {
	id            string
	dataDir       string
	logDir        string
	listenHost    string
	advertiseHost string
	raftPort      string
	grpcPort      string
	driftPort     string
	pprofAddr     string
	isRunning     bool
	mu            *syncutil.RWMutex
	driftConfig   *kronospb.DriftTimeConfig
}

func (t *testNode) DataDir() string {
	return t.dataDir
}

func (t *testNode) kronosStartCmd(
	kronosBinary string,
	seedHosts []string,
	gossipSeeds []string,
	manageOracleTickInterval time.Duration,
	certsDir string,
	raftSnapCount uint64,
) string {
	kronosCmd := []string{
		"KRONOS_NODE_ID=" + t.id,
		"GODEBUG=netdns=cgo",
		fmt.Sprintf("LD_PRELOAD=%v", os.Getenv("PROXY_AWARE_RESOLVER")),
		kronosBinary,
		"start",
		"--advertise-host", t.advertiseHost,
		"--listen-addr", t.listenHost,
		"--raft-port", t.raftPort,
		"--grpc-port", t.grpcPort,
		"--gossip-seed-hosts", strings.Join(gossipSeeds, ","),
		"--use-drift-clock",
		"--drift-port", t.driftPort,
		"--data-dir", t.dataDir,
		"--pprof-addr", t.pprofAddr,
		"--seed-hosts", strings.Join(seedHosts, ","),
		"--manage-oracle-tick-interval", manageOracleTickInterval.String(),
		"--raft-snap-count", fmt.Sprint(raftSnapCount),
	}

	if len(certsDir) > 0 {
		kronosCmd = append(kronosCmd, "--certs-dir", certsDir)
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
	Nodes                    []*testNode
	ManageOracleTickInterval time.Duration
	CertsDir                 string
	RaftSnapCount            uint64
	ErrCh                    chan error
	fs                       afero.Fs
	kronosBinary             string
	procfile                 string
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

// stop stops the testCluster by terminating the goreman command.
func (tc *TestCluster) stop(ctx context.Context) error {
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
		tc.Nodes[index].mu.Lock()
		output, err := tc.runGoremanWithArgs(
			"run",
			op.String(),
			tc.Nodes[index].id,
		)
		if err != nil {
			return errors.Wrapf(err, "output: %s", output)
		}
		switch op {
		case Start, Restart:
			tc.Nodes[index].isRunning = true
		case Stop:
			tc.Nodes[index].isRunning = false
		default:
			return errors.Errorf("unsupported value of op %v", op)
		}
		tc.Nodes[index].mu.Unlock()
	}
	return nil
}

// IsRunning is used to check if nodeIdx node is running or has been stopped.
func (tc *TestCluster) IsRunning(nodeIdx int) bool {
	tc.Nodes[nodeIdx].mu.RLock()
	defer tc.Nodes[nodeIdx].mu.RUnlock()
	return tc.Nodes[nodeIdx].isRunning
}

// UpdateClockConfig is used to update the drifting clock config for nodeidx
// node.
func (tc *TestCluster) UpdateClockConfig(
	ctx context.Context, nodeIdx int, config *kronospb.DriftTimeConfig,
) error {
	dialOpts := grpc.WithInsecure()
	conn, err := grpc.Dial(
		net.JoinHostPort(tc.Nodes[nodeIdx].listenHost, tc.Nodes[nodeIdx].driftPort),
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
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].listenHost, Port: tc.Nodes[nodeIdx].grpcPort},
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
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].listenHost, Port: tc.Nodes[nodeIdx].
			grpcPort}, &kronospb.BootstrapRequest{ExpectedNodeCount: int32(len(tc.Nodes))})
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
			net.JoinHostPort(node.listenHost, node.grpcPort),
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
		nodeAddress := net.JoinHostPort(node.listenHost, node.grpcPort)
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
		&kronospb.NodeAddr{Host: tc.Nodes[nodeIdx].listenHost, Port: tc.Nodes[nodeIdx].grpcPort},
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
		if addr == net.JoinHostPort(node.advertiseHost, node.grpcPort) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return -1, errors.Errorf("node address %s not found in cluster", addr)
	}
	return idx, nil
}

// NodeID returns the raft id of the idx node. It reads cluster
// metadata of idx node to get the same.
func (tc *TestCluster) NodeID(idx int) (string, error) {
	id, err := metadata.FetchNodeID(tc.Nodes[idx].dataDir)
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
		fmt.Sprintf("%s:%s", tc.Nodes[nodeToRunRemoveFrom].listenHost, tc.Nodes[nodeToRunRemoveFrom].raftPort),
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
		return tc.fs.RemoveAll(tc.Nodes[idx].dataDir)
	} else {
		return nil
	}
}

// AddNode adds a new node to testCluster and returns the newly assigned NodeID.
func (tc *TestCluster) AddNode(ctx context.Context, idx int) (string, error) {
	log.Infof(ctx, "Adding node %d to the cluster", idx)
	if _, err := os.Stat(tc.Nodes[idx].dataDir); err == nil || os.IsExist(err) {
		return "", errors.New("data directory for node being added already exists")
	}
	if err := tc.fs.Mkdir(tc.Nodes[idx].dataDir, 0755); err != nil {
		return "", err
	}
	if err := tc.RunOperation(ctx, Start, idx); err != nil {
		return "", err
	}
	// Wait for sometime to let the node-info file be created for the node.
	time.Sleep(time.Second)
	id, err := metadata.FetchNodeID(tc.Nodes[idx].dataDir)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// IsNodeRemoved checks if node idx is removed according to the metadata of
// node sourceNode.
func (t *testNode) IsNodeRemoved(id string) (bool, error) {
	c, err := metadata.LoadCluster(t.dataDir, true /* readOnly */)
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
		if checkOnlyRunningNodes && !node.isRunning {
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
			tc.grpcProxy[i][j].Stop()
			tc.raftProxy[i][j].Stop()
		}
	}
}

// ReIP simulates re-ip in testcluster by changing raft ports of all the nodes
// in the cluster.
func (tc *TestCluster) ReIP(ctx context.Context) error {
	if err := tc.stop(ctx); err != nil {
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
		oldToNewAddr[addr(name, node.raftPort)] = addr(name, newPort)
		node.raftPort = newPort
	}
	var newSeedHosts []string
	for _, seed := range tc.seedHosts {
		newSeedHosts = append(newSeedHosts, oldToNewAddr[seed])
	}
	tc.seedHosts = newSeedHosts

	if err = tc.generateProcfile(ctx); err != nil {
		return err
	}

	if err = tc.createProxies(ctx); err != nil {
		return err
	}

	if err = tc.start(ctx); err != nil {
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
		"--raft-addr", fmt.Sprintf("%s:%s", tc.Nodes[hostIdx].advertiseHost, tc.Nodes[hostIdx].raftPort),
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

func (tc *TestCluster) generateProcfile(ctx context.Context) error {
	kronosBinary, err := absoluteBinaryPath("kronos")
	if err != nil {
		return err
	}
	log.Infof(ctx, "Kronos binary path: %v", kronosBinary)
	tc.kronosBinary = kronosBinary
	pf, err := tc.fs.OpenFile(tc.procfile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer pf.Close()
	for i := 0; i < len(tc.Nodes); i++ {
		_, err = pf.WriteString(
			fmt.Sprintf(
				"%s: %s 2>>%s 1>>%s\n",
				tc.Nodes[i].id,
				tc.Nodes[i].kronosStartCmd(
					tc.kronosBinary,
					tc.seedHosts,
					tc.gossipSeedHosts,
					tc.ManageOracleTickInterval,
					tc.CertsDir,
					tc.RaftSnapCount,
				),
				filepath.Join(tc.Nodes[i].logDir, "kronos-stderr.log"),
				filepath.Join(tc.Nodes[i].logDir, "kronos-stdout.log"),
			),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tc *TestCluster) start(ctx context.Context) error {
	goremanBinary, err := absoluteBinaryPath("goreman")
	if err != nil {
		return err
	}
	if _, err = tc.fs.Stat(goremanBinary); err != nil {
		if os.IsNotExist(err) {
			log.Info(ctx, "goreman binary is missing. Make sure $GOPATH/bin is in $PATH")
		} else {
			return err
		}
	}
	if tc.goremanCmd != nil {
		tc.goremanCmd.Wait()
	}

	tc.goremanCmd = exec.Command(
		goremanBinary,
		"-p",
		tc.goremanPort,
		"-f",
		tc.procfile,
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
	if err := tc.stop(context.Background()); err != nil {
		return err
	}
	for _, node := range tc.Nodes {
		if err := tc.fs.RemoveAll(node.dataDir); err != nil {
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
}

// NewInsecureCluster returns an instance of a test kronos cluster. It returns
// - The test_cluster
// - Error if any while creating the testcluster
// TestCluster should be closed using Close method once test is finished.
// This returns a cluster which runs in insecure mode.
func NewInsecureCluster(ctx context.Context, cc ClusterConfig) (*TestCluster, error) {
	return newCluster(ctx, cc, true /* insecure */)
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
		fmt.Sprintf("127.0.%d.%d:%s", i+1, j+1, tc.Nodes[j].raftPort),
		fmt.Sprintf("127.0.0.%d:%s", j+1, tc.Nodes[j].raftPort),
		failuregen.NewFailureGenerator(),
		failuregen.NewFailureGenerator(),
	)
	tc.raftProxy[i][j] = proxy
	if err != nil {
		return err
	}
	proxy, err = tcpproxy.NewTCPProxy(
		ctx,
		fmt.Sprintf("127.0.%d.%d:%s", i+1, j+1, tc.Nodes[j].grpcPort),
		fmt.Sprintf("127.0.0.%d:%s", j+1, tc.Nodes[j].grpcPort),
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
		fs:                       cc.Fs,
		ErrCh:                    make(chan error),
		ManageOracleTickInterval: cc.ManageOracleTickInterval,
		Nodes:                    make([]*testNode, cc.NumNodes),
		procfile:                 filepath.Join(testDir, procfile),
		RaftSnapCount:            cc.RaftSnapCount,
		testDir:                  testDir,
	}
	log.Infof(ctx, "Procfile: %v", tc.procfile)

	// 0 -> goreman port
	// 1 -> raft port,
	// 2 -> grpc port,
	// 3 -> driftPort
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
		tc.Nodes[i] = &testNode{
			id:            fmt.Sprintf("%d", i+1),
			dataDir:       filepath.Join(testDir, fmt.Sprintf("data_dir_%d", i)),
			logDir:        filepath.Join(testDir, fmt.Sprintf("log_dir_%d", i)),
			advertiseHost: fmt.Sprintf("kronos%d", i+1),
			listenHost:    fmt.Sprintf("127.0.0.%d", i+1),
			raftPort:      strconv.Itoa(freePorts[1]),
			grpcPort:      strconv.Itoa(freePorts[2]),
			driftPort:     strconv.Itoa(freePorts[3]),
			pprofAddr:     strconv.Itoa(freePorts[4]),
			isRunning:     true,
			mu:            &syncutil.RWMutex{},
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].dataDir, 0755); err != nil {
			return nil, err
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].logDir, 0755); err != nil {
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

	if err = tc.generateProcfile(ctx); err != nil {
		return nil, err
	}

	if err = tc.start(ctx); err != nil {
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
// Returns a map of node id to time, node id to uptime, and error (if any)
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
	tmpDataDir := tc.Nodes[id1].dataDir + "_tmp"
	err = tc.fs.Rename(tc.Nodes[id1].dataDir, tmpDataDir)
	if err != nil {
		return err
	}
	err = tc.fs.Rename(tc.Nodes[id2].dataDir, tc.Nodes[id1].dataDir)
	if err != nil {
		return err
	}
	err = tc.fs.Rename(tmpDataDir, tc.Nodes[id2].dataDir)
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
	tc.raftProxy[oracle][i].BlockIncomingConns()
	tc.grpcProxy[oracle][i].BlockIncomingConns()
	tc.raftProxy[i][oracle].BlockIncomingConns()
	tc.grpcProxy[i][oracle].BlockIncomingConns()
}

func (tc *TestCluster) Connect(oracle int, i int) {
	tc.raftProxy[oracle][i].UnblockIncomingConns()
	tc.grpcProxy[oracle][i].UnblockIncomingConns()
	tc.raftProxy[i][oracle].UnblockIncomingConns()
	tc.grpcProxy[i][oracle].UnblockIncomingConns()
}
