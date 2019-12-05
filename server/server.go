package server

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosstats"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/oracle"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/tm"
)

const (
	// DefaultOracleTimeCapDelta is the delta of the upper bound of KronosTime
	// which is persisted in the oracle state machine
	DefaultOracleTimeCapDelta = time.Minute
	// KronosDefaultRaftPort is the default port used by the raft HTTP transport
	KronosDefaultRaftPort = "5766"
	// KronosDefaultGRPCPort is the default port used by the kronos GRPC server
	KronosDefaultGRPCPort = "5767"
	// KronosDefaultDriftServerPort is the default port used by the kronos
	// drift server used in acceptance tests
	KronosDefaultDriftServerPort = "5768"
	// KronosDefaultPProfPort is the default port used by the pprof
	KronosDefaultPProfPort = "5777"
	// Number of consecutive oracle sync errors upon which we attempt to
	// overthrow. On every sync with the oracle (success or failure), we record a
	// new datapoint in a ring buffer, and overthrow if the buffer ends up with
	// all errors.
	numConsecutiveErrsForOverthrow = int64(3)
)

// Server is a time server application which can return nearly the same
// time in a cluster of nodes.
// It is fault tolerant and nearly monotonic across restarts.
type Server struct {
	// OracleSM is the state machine that is used to select an oracle
	OracleSM oracle.StateMachine
	// Client is used to query other kronos servers for time
	Client Client
	// GRPCAddr is the address of the grpc server
	GRPCAddr *kronospb.NodeAddr
	// raftAddr is the address of the raft HTTP server
	raftAddr *kronospb.NodeAddr
	// OracleDelta is the delta of this server with respect to KronosTime
	OracleDelta atomic.Int64
	// Clock is used to get current time
	Clock tm.Clock
	// StopC is used to trigger cleanup functions
	StopC chan struct{}
	// OracleTimeCapDelta is the delta of the upper bound of KronosTime which
	// is persisted in the oracle state machine
	OracleTimeCapDelta time.Duration
	// Metrics records kronos metrics
	Metrics *kronosstats.KronosMetrics

	mu struct {
		syncutil.RWMutex
		// lastKronosTime is the last served KronosTime. This is used to
		// ensure KronosTime does not have backward jumps
		lastKronosTime int64
	}

	// status of the server
	// It is of type kronospb.ServerStatus
	status atomic.Value
	// certsDir is the directory containing node and CA certificates
	certsDir string
	// dataDir is the directory where kronos stores its data like WAL, cluster
	// metadata and NodeID
	dataDir string
	// server is the GRPC server which responds to time requests
	server *grpc.Server
	// manageOracleTickInterval is the time after which an action is taken based
	// on the current state of state machine. Action can be to sync with oracle
	// or extend the oracle lease.
	manageOracleTickInterval time.Duration

	// oracleSyncPos % numConsecutiveErrsForOverthrow should be the index in
	// oracleSyncErrs where the next datapoint should be filled.
	oracleSyncPos int64
	// oracleSyncErrs is a ring buffer of last few oracle sync datapoints
	// including both successful and failed sync attempts (err is nil for
	// successful syncs).
	oracleSyncErrs [numConsecutiveErrsForOverthrow]struct {
		oracle *kronospb.NodeAddr
		err    error
	}
}

var _ kronospb.TimeServiceServer = &Server{}

// Status returns the current status of the server
func (k *Server) Status(
	ctx context.Context, request *kronospb.StatusRequest,
) (*kronospb.StatusResponse, error) {
	return &kronospb.StatusResponse{
		ServerStatus: k.ServerStatus(),
		OracleState:  k.OracleSM.State(ctx),
		Delta:        k.OracleDelta.Load(),
	}, nil
}

// OracleTime returns the current KronosTime if the this server is the elected
// oracle.
// If an error is returned, this oracle may be overthrown by another kronos
// instance.
func (k *Server) OracleTime(
	ctx context.Context, request *kronospb.OracleTimeRequest,
) (*kronospb.OracleTimeResponse, error) {
	oracleData := k.OracleSM.State(ctx)
	if oracleData == nil || !proto.Equal(oracleData.Oracle, k.GRPCAddr) {
		return nil, errors.Errorf(
			"server (%s) is not oracle, current oracle state: %s",
			k.GRPCAddr, oracleData,
		)
	}

	kt, err := k.KronosTimeNow(ctx)
	if err != nil {
		return nil, err
	}

	return &kronospb.OracleTimeResponse{Time: kt.Time}, nil
}

// KronosTime is the same as KronosTimeNow except that it takes a
// KronosTimeRequest as an argument so that Server implements the kronos GRPC
// service.
func (k *Server) KronosTime(
	ctx context.Context, request *kronospb.KronosTimeRequest,
) (*kronospb.KronosTimeResponse, error) {
	return k.KronosTimeNow(ctx)
}

// KronosTimeNow returns the current KronosTime according to the server.
// If an error is returned, then server might be unitialized or it might have
// stale data.
func (k *Server) KronosTimeNow(ctx context.Context) (*kronospb.KronosTimeResponse, error) {
	oracleData := k.OracleSM.State(ctx)

	k.mu.Lock()
	defer k.mu.Unlock()
	t := k.adjustedTime()
	currentStatus := k.ServerStatus()
	initialized := currentStatus == kronospb.ServerStatus_INITIALIZED
	var errorMsg string
	if !initialized {
		errorMsg = "kronos server not yet initialized"
	} else if oracleData.TimeCap == 0 {
		errorMsg = "kronos time cap not yet initialized"
	} else if oracleData.TimeCap <= t {
		errorMsg = "kronos time is beyond current time cap, time cap is too stale"
	}
	if errorMsg != "" {
		return nil, errors.Errorf(
			"%s: kronos time: %v, status: %v, time cap: %v",
			errorMsg, t, currentStatus, oracleData.TimeCap,
		)
	}

	// ensure that KronosTime does not have backward jumps
	if k.mu.lastKronosTime > t {
		t = k.mu.lastKronosTime
	}

	k.mu.lastKronosTime = t
	return &kronospb.KronosTimeResponse{Time: t, TimeCap: oracleData.TimeCap}, nil
}

func (k *Server) shouldOverthrowOracle(ctx context.Context) bool {
	// We overthrow the oracle if the last numConsecutiveErrsForOverthrow errors
	// are non-nil errors on the same oracle.
	N := int64(len(k.oracleSyncErrs))
	mod := func(p int64) int64 {
		// We return the positive remainder for negative inputs as well.
		return ((p % N) + N) % N
	}
	var oracle *kronospb.NodeAddr
	var errs []string
	for i := int64(1); i <= N; i++ {
		syncErr := k.oracleSyncErrs[mod(k.oracleSyncPos-i)]
		if syncErr.err == nil {
			return false
		}
		if oracle == nil {
			oracle = syncErr.oracle
		} else if !proto.Equal(syncErr.oracle, oracle) {
			return false
		}
		errs = append(errs, syncErr.err.Error())
	}
	log.Infof(
		ctx,
		"Eligible to overthrow oracle due to %d consecutive errors on the same oracle %s,"+
			" errs: [%v]",
		numConsecutiveErrsForOverthrow, oracle, strings.Join(errs, "; "),
	)
	return true
}

func (k *Server) syncOrOverthrowOracle(
	ctx context.Context, oracleState *kronospb.OracleState,
) (synced bool) {
	err := k.trySyncWithOracle(ctx, oracleState.Oracle)
	if err == nil {
		k.Metrics.SyncSuccessCount.Inc(1)
	} else {
		k.Metrics.SyncFailureCount.Inc(1)
	}

	pos := k.oracleSyncPos % int64(len(k.oracleSyncErrs))
	k.oracleSyncErrs[pos].oracle = oracleState.Oracle
	k.oracleSyncErrs[pos].err = err
	k.oracleSyncPos++
	if err != nil {
		log.Errorf(ctx, "Failed to sync with oracle %s, err: %v", oracleState.Oracle, err)
	}
	if k.shouldOverthrowOracle(ctx) {
		log.Warningf(ctx, "Overthrowing the oracle (%s) due to too many errors", oracleState)
		k.proposeSelf(ctx, oracleState)
		k.Metrics.OverthrowAttemptCount.Inc(1)
	}
	return err == nil
}

// trySyncWithOracle queries the oracle for KronosTime and adjusts
// the delta of this server.
// This returns whether the current oracle is healthy
func (k *Server) trySyncWithOracle(ctx context.Context, oracle *kronospb.NodeAddr) error {
	// We wish to hear from the oracle every manageOracleTickInterval, so put a
	// deadline for OracleTime rpc to return based on that. Note that this
	// deadline includes the time to make a connection as well.
	ctx, cancelFunc := context.WithTimeout(ctx, k.manageOracleTickInterval/3)
	defer cancelFunc()
	response, err := k.Client.OracleTime(ctx, oracle)
	if err != nil {
		return err
	}
	k.Metrics.RTT.RecordValue(response.Rtt)

	// Tolerate clock skews between nodes of up to MaxOffset of 400 ms.
	// We wish for the maximum skew across any two nodes to be lower than that.
	// If the OracleTime rpc has a long RTT, the error introduced by the RTT
	// itself could be high enough for us to not be able to make that guarantee,
	// therefore consider any RTT of more than 200 ms as an error.
	const oracleUnhealthyRTTThreshold = 200 * time.Millisecond
	if response.Rtt > int64(oracleUnhealthyRTTThreshold) {
		return errors.Errorf(
			"rtt too high (more than %v): %v",
			oracleUnhealthyRTTThreshold,
			time.Duration(response.Rtt),
		)
	}

	// Assuming reponse.Time reading could have been taken at any point of time
	// within the RTT, adjust our own delta in the most conservative manner to
	// avoid too much back and forth on the delta. Find the range of possible
	// values of oracle time taking the RTT error into account, and adjust our
	// delta by the least possible value to keep us in that range.

	oracleTimeRangeLHS := response.Time
	oracleTimeRangeRHS := response.Time + response.Rtt

	adjustedTime := k.adjustedTime()

	var deltaAdjustment int64
	if adjustedTime < oracleTimeRangeLHS {
		deltaAdjustment = oracleTimeRangeLHS - adjustedTime
	} else if adjustedTime > oracleTimeRangeRHS {
		deltaAdjustment = oracleTimeRangeRHS - adjustedTime
	} else {
		deltaAdjustment = 0
	}

	k.OracleDelta.Add(deltaAdjustment)
	log.Infof(
		ctx,
		"Synced with oracle: %s. Oracle time: %v, rtt: %v, delta adjustment: %v, new delta: %v",
		oracle,
		response.Time,
		time.Duration(response.Rtt),
		time.Duration(deltaAdjustment),
		time.Duration(k.OracleDelta.Load()),
	)
	return nil
}

// initialize initializes the Kronos server
// This server is considered initialized if it is the oracle or if has synced
// time with the oracle.
// A server proposes self as the oracle if either one of these conditions is
// met:
// 1. It is already the oracle according to the state machine.
// 2. There is no current oracle.
// 3. It detects the current oracle is down.
// This server waits for a few ticks before proposing itself as the oracle since
// it is preferred that initialized servers become the oracle as they have an
// appropriate delta.
// After initialization, KronosTime of this server would be valid.
func (k *Server) initialize(ctx context.Context, tickCh <-chan time.Time, tickCallback func()) {
	ctx = log.WithLogTag(ctx, "initialize", nil)
	// becomeOracleTickThreshold is the number of ticks after which this node
	// is eligible to become an oracle.
	// It is preferred if an initialized node of the cluster becomes the oracle to
	// have continuous time as live nodes have the appropriate delta for this
	// cluster.
	// If a non initialized server becomes the oracle it will continue time from
	// time cap if its clock is behind because it is unaware of the appropriate
	// delta. This can potentially cause a time jump.
	// becomeOracleTickThreshold is carefully chosen to be one more than
	// numConsecutiveErrsForOverthrow. So if this oracle restarted, yielding for
	// tick number [0 .. numConsecutiveErrsForOverthrow] will ensure that if there
	// was an initialized server, it would have attempted to overthrow this
	// oracle.
	const becomeOracleTickThreshold = numConsecutiveErrsForOverthrow + 1

	tickNum := int64(0)
	for k.ServerStatus() != kronospb.ServerStatus_INITIALIZED {
		select {
		case <-k.StopC:
			return
		case <-tickCh:
			oracleState := k.OracleSM.State(ctx)
			k.Metrics.TimeCap.Update(oracleState.TimeCap)
			log.Infof(ctx, "Oracle state: %v", oracleState)
			switch {
			case oracleState.Oracle == nil:
				// No oracle exists.
				// This can happen the first time a kronos cluster starts up.
				log.Info(ctx, "Oracle does not exist, volunteering")
				// This server is trying to become the first ever oracle of the cluster.
				k.proposeSelf(ctx, oracleState)
				// Do not wait to claim oracle-ship if we attempted to become the first
				// ever oracle. If we wait, another server might try to overthrow us.
				tickNum = becomeOracleTickThreshold

			case proto.Equal(oracleState.Oracle, k.GRPCAddr):
				k.Metrics.IsOracle.Update(1)
				// Local IP is oracle
				// It is possible that this server restarted while it was the last
				// oracle, if this server has waited enough time for another server to
				// become the oracle and no other server has stepped up, continue as the
				// oracle.
				if tickNum < becomeOracleTickThreshold {
					log.Info(
						ctx,
						"Local server is the oracle, but yielding for another initialized server to step up.",
					)
					break
				}
				if timeCap := k.timeCap(); timeCap < oracleState.TimeCap {
					// When using monotonic clock backward jump can only occur across
					// restarts. If a backward jump is detected on startup, adjust delta
					// to match TimeCap.
					//
					// Comment from vijay.karthik:
					// If there are no clock jumps and delta ~ 0, k.adjustedTime <
					// oracleState.TimeCap < k.timeCap since oracleState.Timecap would be
					// a timecap for an earlier adjustedTime.  In this scenario, we
					// continue time as is as the time is approximately correct. This can
					// happen in case we restart all servers and all of them have a delta
					// ~0 (which is the common case). Continuing time from time cap in
					// this scenario would result in a forward jump and a delta of up to
					// oracleTimeCapDelta. Also, if this happens in a crash loop delta can
					// keep increasing. So in this scenario we continue with a delta ~ 0.
					// This can potentially result in a small backward jump, but as long
					// as delta ~ 0 this backward jump would not be noticed.
					//
					// If there is a backward jump, we will hit the scenario
					// k.adjustedTime < k.timeCap  < oracleState.TimeCap. In this case, we
					// need to continue time from time cap. This is the condition which
					// currently exists.
					//
					// If there was a forward jump, we will hit the scenario
					// oracleState.TimeCap  < k.adjustedTime < k.timeCap and we do no
					// adjustments.
					deltaAdj := oracleState.TimeCap - k.adjustedTime() + 1
					log.Infof(
						ctx,
						"Backward time jump was detected during initialization."+
							" Local timeCap: %v is less than Oracle timeCap: %v."+
							" Adjusting delta by %v to ensure monotonicity",
						timeCap, oracleState.TimeCap, deltaAdj,
					)
					k.OracleDelta.Add(deltaAdj)
				}

				log.Infof(
					ctx,
					"Initializing self as oracle. Extending oracle lease. Delta: %v",
					time.Duration(k.OracleDelta.Load()),
				)
				k.proposeSelf(ctx, oracleState)
				// This server is the oracle. It is now initialized.
				k.status.Store(kronospb.ServerStatus_INITIALIZED)

			default:
				k.Metrics.IsOracle.Update(0)
				// Synchronize time with the oracle
				if k.syncOrOverthrowOracle(ctx, oracleState) {
					// Time synced with oracle. This server is now initialized.
					k.status.Store(kronospb.ServerStatus_INITIALIZED)
				}
				// If we did not find ourselves to be the first oracle, we do not yield
				// any further in case we decide to overthrow.
				tickNum = becomeOracleTickThreshold
			}

			// Update metric before tickCallback so that this can be deterministically
			// tested
			if k.ServerStatus() == kronospb.ServerStatus_INITIALIZED {
				k.Metrics.Delta.Update(k.OracleDelta.Load())
			}
			tickCallback()
			tickNum++
		}
	}
}

// ManageOracle initializes the server and takes an action on each tick of
// tickCh
// At every tick of tickCh the current state of the oracle state machine is
// read and the action taken could be
// 1. Extend oracle lease
// 2. Overthrow the oracle if it is down
// 3. Sync time with the oracle
// tickCallback is called after processing each tick
func (k *Server) ManageOracle(tickCh <-chan time.Time, tickCallback func()) {
	ctx := log.WithLogTag(context.Background(), "manage-oracle", nil)
	if tickCallback == nil {
		tickCallback = func() {}
	}

	// Initialize the server. KronosTime is valid only for initialized servers
	k.initialize(ctx, tickCh, tickCallback)

	// Server is now initialized.
	// Continue as oracle if this server is the oracle else sync time with the
	// oracle.
	for {
		select {
		case <-k.StopC:
			return
		case <-tickCh:
			oracleState := k.OracleSM.State(ctx)
			k.Metrics.TimeCap.Update(oracleState.TimeCap)
			log.Infof(ctx, "Oracle state: %s", oracleState)
			switch {
			case proto.Equal(oracleState.Oracle, k.GRPCAddr):
				k.Metrics.IsOracle.Update(1)
				// Local IP is oracle
				log.Infof(
					ctx,
					"Extending self oracle lease. Delta: %v",
					time.Duration(k.OracleDelta.Load()),
				)
				k.proposeSelf(ctx, oracleState)

			default:
				// Synchronize time with the oracle
				k.Metrics.IsOracle.Update(0)
				k.syncOrOverthrowOracle(ctx, oracleState)
			}
			k.Metrics.Delta.Update(k.OracleDelta.Load())
			tickCallback()
		}
	}
}

// Stop the Kronos server
func (k *Server) Stop() {
	close(k.StopC)
	if k.server != nil {
		k.server.Stop()
	}
	k.OracleSM.Close()
}

// proposeSelf proposes self as the next oracle
func (k *Server) proposeSelf(ctx context.Context, curOracle *kronospb.OracleState) {
	timeCap := k.timeCap()
	if curOracle.TimeCap >= timeCap {
		// Always bump the time cap to ensure monotonicity. If the whole cluster is
		// restarted and all nodes forget their delta, we start the time at the
		// previous time cap.
		timeCap = curOracle.TimeCap + 1
	}

	k.OracleSM.SubmitProposal(ctx, &kronospb.OracleProposal{
		ProposedState: &kronospb.OracleState{
			Oracle:  k.GRPCAddr,
			TimeCap: timeCap,
			// Hope to be next in the sequence of state machine update proposals. If
			// someone else's proposal makes it before us, our proposal will be
			// rejected, which is good because we do not want multiple claimants to
			// thrash the oracle state.
			Id: curOracle.Id + 1,
		},
	})

	// Clear out the history of errors if we proposed ourselves as oracle. We
	// might resume following another oracle later and we do not want to take very
	// old history error values into account making decisions.
	if k.oracleSyncPos > 0 {
		for i := 0; i < len(k.oracleSyncErrs); i++ {
			k.oracleSyncErrs[i].oracle = nil
			k.oracleSyncErrs[i].err = nil
		}
		k.oracleSyncPos = 0
	}
}

// ServerStatus returns the current status of the server
func (k *Server) ServerStatus() kronospb.ServerStatus {
	status := k.status.Load()
	if status == nil {
		return kronospb.ServerStatus_NOT_INITIALIZED
	}
	return status.(kronospb.ServerStatus)
}

// adjustedTime returns the time adjusted by delta
func (k *Server) adjustedTime() int64 {
	return k.Clock.Now() + k.OracleDelta.Load()
}

// timeCap returns an upper bound to KronosTime
func (k *Server) timeCap() int64 {
	return k.adjustedTime() + int64(k.OracleTimeCapDelta)
}

// NewClusterClient returns a ClusterClient which can be used to perform
// cluster operations on the kronos server. Close should be called after
// completing all the requests to avoid connection leaks. Close should be called
// after completing all the requests to avoid connection leaks.
func (k *Server) NewClusterClient() (*kronoshttp.ClusterClient, error) {
	tlsInfo := kronosutil.TLSInfo(k.certsDir)
	return kronoshttp.NewClusterClient(k.raftAddr, tlsInfo)
}

// RunServer starts a Kronos GRPC server
func (k *Server) RunServer(ctx context.Context) error {
	if k.server != nil {
		return errors.New("server already running")
	}

	// Listen on all interfaces
	lis, err := net.Listen(
		"tcp",
		net.JoinHostPort("", k.GRPCAddr.Port),
	)
	if err != nil {
		log.Fatalf(ctx, "failed to listen: %v", err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, lis)
	var opts []grpc.ServerOption
	if k.certsDir != "" {
		creds, err := kronosutil.SSLCreds(k.certsDir)
		if err != nil {
			return errors.Errorf("could not load TLS keys: %s", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	server := grpc.NewServer(opts...)
	kronospb.RegisterTimeServiceServer(server, k)

	k.server = server
	t := time.NewTicker(k.manageOracleTickInterval)
	defer t.Stop()
	go k.ManageOracle(t.C, nil)

	return server.Serve(lis)
}

// ID returns the persisted ID of the kronos server. If the kronos server is not
// initialized and does not have an assigned ID, an error is returned
func (k *Server) ID() (string, error) {
	id, err := metadata.FetchNodeID(k.dataDir)
	if err != nil {
		return "", err
	}

	return id.String(), nil
}

// Config is used to initialize a kronos server based on the given parameters
type Config struct {
	*oracle.RaftConfig
	// OracleTimeCapDelta is the delta of the upper bound of KronosTime which
	// is persisted in the oracle state machine
	OracleTimeCapDelta time.Duration
	// manageOracleTickInterval is the time after which an action is taken based
	// on the current state of state machine. Action can be to sync with oracle
	// or extend the oracle lease.
	ManageOracleTickInterval time.Duration
	// Clock is the interface which is used to access system time inside server.
	Clock tm.Clock
}

// NewKronosServer returns an instance of Server based on given
// configurations
func NewKronosServer(ctx context.Context, config Config) (*Server, error) {
	oracleSM := oracle.NewRaftStateMachine(ctx, config.RaftConfig)

	return &Server{
		Clock:                    config.Clock,
		OracleSM:                 oracleSM,
		Client:                   NewGRPCClient(config.CertsDir),
		GRPCAddr:                 config.GRPCHostPort,
		raftAddr:                 config.RaftHostPort,
		certsDir:                 config.CertsDir,
		dataDir:                  config.DataDir,
		StopC:                    make(chan struct{}),
		OracleTimeCapDelta:       config.OracleTimeCapDelta,
		manageOracleTickInterval: config.ManageOracleTickInterval,
		Metrics:                  kronosstats.NewMetrics(),
	}, nil
}
