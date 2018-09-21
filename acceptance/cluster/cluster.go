package cluster

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

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
	id          string
	dataDir     string
	logDir      string
	raftPort    string
	grpcPort    string
	driftPort   string
	pprofAddr   string
	isRunning   bool
	mu          *syncutil.RWMutex
	driftConfig *kronospb.DriftTimeConfig
}

func (t *testNode) DataDir() string {
	return t.dataDir
}

func (t *testNode) kronosStartCmd(
	kronosBinary string,
	seedHosts []string,
	manageOracleTickInterval time.Duration,
	certsDir string,
	raftSnapCount uint64,
) string {
	kronosCmd := []string{
		kronosBinary,
		"--log-dir", t.logDir,
		"start",
		"--advertise-host", localhost,
		"--raft-port", t.raftPort,
		"--grpc-port", t.grpcPort,
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
	goremanCmd               *exec.Cmd
	goremanPort              string
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
		net.JoinHostPort(localhost, tc.Nodes[nodeIdx].driftPort),
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
		&kronospb.NodeAddr{Host: localhost, Port: tc.Nodes[nodeIdx].grpcPort},
	)
	if err != nil {
		return -1, err
	}
	return tr.Time, nil
}

// ValidateTimeInConsensus validates time across the cluster(difference between
// maxTime and minTime) is within maxDiffAllowed for the running nodes. It
// returns ClusterTime which is a map of NodeID to time.
func (tc *TestCluster) ValidateTimeInConsensus(
	ctx context.Context, maxDiffAllowed time.Duration, checkOnlyRunningNodes bool,
) (ClusterTime, error) {
	var nodesToValidate []string
	for nodeIdx, node := range tc.Nodes {
		if checkOnlyRunningNodes && !tc.IsRunning(nodeIdx) {
			continue
		}
		nodesToValidate = append(
			nodesToValidate,
			net.JoinHostPort(localhost, node.grpcPort),
		)
	}
	addressToTime, err := validateTimeInConsensus(
		ctx,
		maxDiffAllowed,
		nodesToValidate,
		tc.CertsDir,
	)
	nodeIdxToTime := make(ClusterTime)
	for address, time := range addressToTime {
		nodeIdxToTime[tc.index(address)] = time
	}

	return nodeIdxToTime, err
}

func (tc *TestCluster) index(address string) int {
	for idx, node := range tc.Nodes {
		nodeAddress := net.JoinHostPort(localhost, node.grpcPort)
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
		&kronospb.NodeAddr{Host: localhost, Port: tc.Nodes[nodeIdx].grpcPort},
	)
	if err != nil {
		return -1, err
	}
	addr := net.JoinHostPort(
		status.OracleState.Oracle.Host,
		status.OracleState.Oracle.Port,
	)
	idx := tc.index(addr)
	if idx == -1 {
		return -1, errors.Errorf("node address %s not found in cluster", addr)
	}
	return idx, nil
}

// nodeID returns the raft id of the idx node. It reads cluster
// metadata of idx node to get the same.
func (tc *TestCluster) nodeID(idx int) (string, error) {
	c, err := metadata.LoadCluster(tc.Nodes[idx].dataDir, true /*readOnly*/)
	if err != nil {
		return "", err
	}
	for id, node := range c.ActiveNodes() {
		if node.RaftAddr.Port == tc.Nodes[idx].raftPort {
			return id, nil
		}
	}
	return "", errors.Errorf("nodeIdx %d not found in cluster metadata", idx)
}

// RemoveNode removes a node from testCluster and wipes it's data directory
func (tc *TestCluster) RemoveNode(ctx context.Context, idx int) error {
	var nodeToRunRemoveFrom int
	r, _ := randutil.NewPseudoRand()
	if r.Float32() < 0.5 {
		// Test remove node from the node being removed
		nodeToRunRemoveFrom = idx
	} else {
		// Test remove node from a node not being removed (idx of 0 or 1)
		if idx == 0 {
			nodeToRunRemoveFrom = 1
		}
	}

	log.Infof(ctx, "Removing node %d from node %d", idx, nodeToRunRemoveFrom)

	nodeID, err := tc.nodeID(idx)
	if err != nil {
		return err
	}
	removeArgs := []string{
		"cluster",
		"remove",
		nodeID,
		"--host",
		fmt.Sprintf("%s:%s", localhost, tc.Nodes[nodeToRunRemoveFrom].raftPort),
		"--certs-dir", tc.CertsDir,
	}
	if output, err := exec.Command(
		tc.kronosBinary, removeArgs...,
	).CombinedOutput(); err != nil {
		return errors.Wrapf(err, "Output: %s", string(output))
	}
	if err := tc.RunOperation(ctx, Stop, idx); err != nil {
		return err
	}
	// deleting the data directory, as we don't support recommission of node to
	// kronos cluster yet.
	return tc.fs.RemoveAll(tc.Nodes[idx].dataDir)
}

// AddNode adds a new node to testCluster and returns the newly assigned nodeID.
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

// ReIP simulates re-ip in testcluster by changing raft ports of all the nodes
// in the cluster.
func (tc *TestCluster) ReIP(ctx context.Context) error {
	if err := tc.stop(ctx); err != nil {
		return err
	}
	freePorts, err := testutil.GetFreePorts(ctx, len(tc.Nodes))
	if err != nil {
		return err
	}
	oldToNewAddr := make(map[string]string)
	addr := func(port string) string { return localhost + ":" + port }
	for i, node := range tc.Nodes {
		newPort := fmt.Sprint(freePorts[i])
		oldToNewAddr[addr(node.raftPort)] = addr(newPort)
		node.raftPort = newPort
	}
	var newSeedHosts []string
	for _, seed := range tc.seedHosts {
		newSeedHosts = append(newSeedHosts, oldToNewAddr[seed])
	}
	tc.seedHosts = newSeedHosts
	tempFile, err := afero.TempFile(tc.fs, "", "mappingFile")
	if err != nil {
		return err
	}
	defer func() { _ = tc.fs.Remove(tempFile.Name()) }()
	data, err := json.Marshal(oldToNewAddr)
	if err != nil {
		return err
	}
	for written := 0; written < len(data); {
		n, err := tempFile.Write(data[written:])
		written += n
		if err != nil {
			return err
		}
	}
	for _, node := range tc.Nodes {
		reIPLogDir, err := afero.TempDir(tc.fs, node.logDir, "reIp")
		if err != nil {
			return err
		}
		reIPArgs := []string{
			"--log-dir", reIPLogDir,
			"cluster",
			"re_ip",
			"--mapping-file", tempFile.Name(),
			"--data-dir", node.dataDir,
		}
		reIPCmd := exec.Command(tc.kronosBinary, reIPArgs...)
		if err := reIPCmd.Run(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				log.Error(ctx, reIPArgs, exitErr.Stderr)
				return errors.Wrap(err, string(exitErr.Stderr))
			}
			return err
		}
	}
	if err = tc.generateProcfile(ctx); err != nil {
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
		"--raft-addr", fmt.Sprintf("%s:%s", localhost, tc.Nodes[hostIdx].raftPort),
		fmt.Sprintf("--local=%t", local),
	}
	if len(tc.CertsDir) > 0 {
		statusArgs = append(statusArgs, "--certs-dir", tc.CertsDir)
	}
	statusCmd := exec.Command(tc.kronosBinary, statusArgs...)
	var stdout, stderr bytes.Buffer
	statusCmd.Stdout = &stdout
	statusCmd.Stderr = &stderr
	if err := statusCmd.Run(); err != nil {
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
				"%s: %s\n",
				tc.Nodes[i].id,
				tc.Nodes[i].kronosStartCmd(
					tc.kronosBinary,
					tc.seedHosts,
					tc.ManageOracleTickInterval,
					tc.CertsDir,
					tc.RaftSnapCount,
				),
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
			log.Info(ctx, "goreman binary is missing")
		} else {
			return err
		}
	}
	tc.goremanCmd = exec.Command(
		goremanBinary,
		"-p",
		tc.goremanPort,
		"-f",
		tc.procfile,
		"start",
	)
	if err := tc.goremanCmd.Start(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return errors.Wrap(err, string(exitErr.Stderr))
		}
		return err
	}

	go func() {
		if err := tc.goremanCmd.Wait(); err != nil {
			tc.ErrCh <- err
		}
	}()

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
	return newCluster(ctx, cc, false /* insecure */)
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
		certsDir = filepath.Join(testDir, "certs_dir")
		// The certs related constants are taken from
		// pkg/acceptance/cluster/dockercluster.go
		const keyLen = 1024
		err = security.CreateCAPair(
			certsDir,
			filepath.Join(certsDir, security.EmbeddedCAKey),
			keyLen,
			96*time.Hour,
			false, /* allowKeyReuse */
			false, /* overwrite */
		)
		if err != nil {
			return nil, err
		}
		err = security.CreateNodePair(
			certsDir,
			filepath.Join(certsDir, security.EmbeddedCAKey),
			keyLen,
			48*time.Hour,
			false, /* overwrite */
			[]string{localhost},
		)
		if err != nil {
			return nil, err
		}
	}

	tc := &TestCluster{
		CertsDir: certsDir,
		fs:       cc.Fs,
		ErrCh:    make(chan error),
		ManageOracleTickInterval: cc.ManageOracleTickInterval,
		Nodes:         make([]*testNode, cc.NumNodes),
		procfile:      filepath.Join(testDir, procfile),
		RaftSnapCount: cc.RaftSnapCount,
	}
	log.Infof(ctx, "Procfile: %v", tc.procfile)

	// 0 -> goreman port
	// [1, numNodes] -> raft port,
	// [numNodes + 1, 2*numNodes] -> grpc port,
	// [2*numNodes + 1, 3*numNodes] -> driftPort
	// [3*numNodes + 1, 4*numNodes] -> pprofPort
	freePorts, err := testutil.GetFreePorts(ctx, 4*cc.NumNodes+1)
	if err != nil {
		return nil, err
	}
	tc.goremanPort = strconv.Itoa(freePorts[0])
	kronosPorts := freePorts[1:]

	const numSeedHosts = 2
	for i := 0; i < numSeedHosts; i++ {
		tc.seedHosts = append(
			tc.seedHosts,
			fmt.Sprintf("%s:%d", localhost, kronosPorts[i]),
		)
	}

	for i := 0; i < cc.NumNodes; i++ {
		tc.Nodes[i] = &testNode{
			id:        fmt.Sprintf("kronos%d", i),
			dataDir:   filepath.Join(testDir, fmt.Sprintf("data_dir_%d", i)),
			logDir:    filepath.Join(testDir, fmt.Sprintf("log_dir_%d", i)),
			raftPort:  fmt.Sprint(kronosPorts[i]),
			grpcPort:  fmt.Sprint(kronosPorts[cc.NumNodes+i]),
			driftPort: fmt.Sprint(kronosPorts[2*cc.NumNodes+i]),
			pprofAddr: fmt.Sprintf(":%d", kronosPorts[3*cc.NumNodes+i]),
			isRunning: true,
			mu:        &syncutil.RWMutex{},
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].dataDir, 0755); err != nil {
			return nil, err
		}
		if err = cc.Fs.Mkdir(tc.Nodes[i].logDir, 0755); err != nil {
			return nil, err
		}
	}

	if err = tc.generateProcfile(ctx); err != nil {
		return nil, err
	}

	if err = tc.start(ctx); err != nil {
		return nil, err
	}

	return tc, nil
}

// validateTimeInConsensus validates that the kronos time across the given nodes
// (difference between maxTime and minTime) is within maxDiffAllowed.
func validateTimeInConsensus(
	ctx context.Context, maxDiffAllowed time.Duration, nodeAddresses []string, certsDir string,
) (map[string]int64, error) {
	var mu syncutil.RWMutex
	timeOnNodes := make(map[string]int64)
	eg, ctx := errgroup.WithContext(ctx)
	var wg sync.WaitGroup
	wg.Add(len(nodeAddresses))
	for _, address := range nodeAddresses {
		// making a copy to avoid
		// https://github.com/golang/go/wiki/CommonMistakes#using-goroutines-on-loop-iterator-variables
		currAddress := address
		host, port, err := net.SplitHostPort(currAddress)
		if err != nil {
			return nil, err
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
				mu.Lock()
				defer mu.Unlock()
				timeOnNodes[currAddress] = tr.Time
				return err
			},
		)
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	if err := kronosutil.ValidateTimeInConsensus(ctx, maxDiffAllowed, timeOnNodes); err != nil {
		return nil, err
	}
	return timeOnNodes, nil
}
