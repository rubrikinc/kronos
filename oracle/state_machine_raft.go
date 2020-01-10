package oracle

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/scaledata/etcd/snap"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
)

// RaftConfig is used to initialize a raft based on the given parameters
type RaftConfig struct {
	// RaftHostPort is the host:port of raft HTTP server
	RaftHostPort *kronospb.NodeAddr
	// GRPCHostPort is the host:port of raft HTTP server
	GRPCHostPort *kronospb.NodeAddr
	// SeedHosts is a comma separated list of kronos seed hosts in the cluster
	SeedHosts []string
	// CertsDir is the directory having node and CA certificates / pems
	CertsDir string
	// DataDir is the directory where a Kronos server will store its snapshots
	// and raft WAL
	DataDir string
	// SnapCount is the number of raft entries after which a raft snapshot is
	// triggered.
	SnapCount uint64
}

// RaftStateMachine is a distributed state machine managed by raft.
// Each node maintains an in memory state machine where the actions fed in
// are the actions (messages) committed through raft
type RaftStateMachine struct {
	// proposeC is the channel for proposing updates
	proposeC chan<- string
	// snapshotter is used to restore from a raft snapshot
	snapshotter *snap.Snapshotter
	// stateMachine is an in memory state machine managed through raft
	stateMachine *inMemStateMachine
	// channel to signal blocking/unblocking a snapshot
	getSnapshotC chan struct{}
	// closer is a cleanup function
	closer func()
}

// convertToNodeAddrs converts a slice of host:port to a slice of
// kronospb.NodeAddr
func convertToNodeAddrs(addrs []string) ([]*kronospb.NodeAddr, error) {
	var seedHostsAddrs []*kronospb.NodeAddr
	for _, addr := range addrs {
		nodeAddr, err := kronosutil.NodeAddr(addr)
		if err != nil {
			return nil, err
		}
		seedHostsAddrs = append(seedHostsAddrs, nodeAddr)
	}
	return seedHostsAddrs, nil
}

var _ StateMachine = &RaftStateMachine{}

// NewRaftStateMachine returns an instance of a distributed oracle state
// machine managed by raft
func NewRaftStateMachine(ctx context.Context, rc *RaftConfig) StateMachine {
	var raftStateMachine *RaftStateMachine
	proposeC := make(chan string)
	getSnapshot := func() ([]byte, error) { return raftStateMachine.GetSnapshot(ctx) }
	nodeID := metadata.FetchOrAssignNodeID(ctx, rc.DataDir).String()
	commitC, errorC, snapshotterReady := newRaftNode(rc, getSnapshot, proposeC, nodeID)

	raftStateMachine = &RaftStateMachine{
		proposeC:     proposeC,
		snapshotter:  <-snapshotterReady,
		stateMachine: NewMemStateMachine().(*inMemStateMachine),
		getSnapshotC: make(chan struct{}),
	}
	raftStateMachine.closer = func() {
		close(proposeC)
	}

	// replay existing commits synchronously so that that the state machine is at
	// the last known state before initializing.
	raftStateMachine.readCommits(ctx, commitC, errorC)
	// read commits from raft into RaftStateMachine
	go raftStateMachine.readCommits(context.Background(), commitC, errorC)
	return raftStateMachine
}

// Close cleans up RaftStateMachine
func (s *RaftStateMachine) Close() {
	s.closer()
}

// State returns a snapshot of the current state of the state machine
func (s *RaftStateMachine) State(ctx context.Context) *kronospb.OracleState {
	return s.stateMachine.State(ctx)
}

// SubmitProposal submits a new proposal to the StateMachine. The state machine
// accepts the proposal if PrevID matches the ID of the StateMachine.
// This function does not return anything as the proposal is async.
func (s *RaftStateMachine) SubmitProposal(ctx context.Context, proposal *kronospb.OracleProposal) {
	encodedProposal, err := protoutil.Marshal(proposal)
	if err != nil {
		log.Fatalf(ctx, "Failed to marshal proposal: %v, err: %v", proposal, err)
	}
	s.proposeC <- string(encodedProposal)
}

// readCommits reads committed messages in raft and applies the messages to
// the in memory state machine
func (s *RaftStateMachine) readCommits(
	ctx context.Context, commitC <-chan string, errorC <-chan error,
) {
	for data := range commitC {
		switch data {
		case replayedWALMsg:
			// WAL is replayed synchronously when a server restarts
			log.Info(ctx, "Done replaying WAL entries on state machine.")
			return
		case loadSnapshotMsg:
			// signaled to load snapshot
			if err := s.recoverFromSnapshot(ctx); err != nil {
				log.Fatalf(ctx, "Failed to recover from snapshot, err: %v", err)
			}
		case unblockSnapshotMsg:
			// signaled to unblock a snapshot
			s.getSnapshotC <- struct{}{}
		default:
			proposal := &kronospb.OracleProposal{}
			if err := protoutil.Unmarshal([]byte(data), proposal); err != nil {
				log.Fatalf(ctx, "Failed to unmarshal message %+.100q, err: %v", data, err)
			}
			s.stateMachine.SubmitProposal(ctx, proposal)
		}
	}

	if err, ok := <-errorC; ok {
		log.Fatalf(ctx, "Received error from raft, err: %v", err)
	}
}

// GetSnapshot returns a snapshot of the in memory state machine
func (s *RaftStateMachine) GetSnapshot(ctx context.Context) ([]byte, error) {
	// Block until we get a signal on getSnapshotC
	<-s.getSnapshotC
	return protoutil.Marshal(s.State(ctx))
}

// recoverFromSnapshot restores the state of the in memory state machine from a
// snapshot
func (s *RaftStateMachine) recoverFromSnapshot(ctx context.Context) error {
	snapshot, err := s.snapshotter.Load()
	if err != nil {
		// There must always be snapshot to load whenever we are signaled to
		// consume it.
		return err
	}
	log.Infof(
		ctx,
		"Applying snapshot to state machine, index: %d, term: %d, bytes: %d",
		snapshot.Metadata.Index,
		snapshot.Metadata.Term,
		len(snapshot.Data),
	)
	var data kronospb.OracleState
	if err := protoutil.Unmarshal(snapshot.Data, &data); err != nil {
		return err
	}
	s.stateMachine.restoreState(data)
	return nil
}
