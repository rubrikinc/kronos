package oracle

import (
	"context"
	"crypto/tls"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coreos/pkg/capnslog"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/transport"
	"go.etcd.io/etcd/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"

	"github.com/rubrikinc/kronos/gossip"
	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
)

// This code in this file is based on
// https://github.com/coreos/etcd/blob/master/contrib/raftexample/raft.go

const (
	// Maximum size after which a new WAL file is created. We override the default
	// (64 MB) with a lower value because our application only writes a new entry
	// every few seconds, and we do not want to have wasted space in pre-allocated
	// log files.
	walSegmentSizeBytes = 16 * 1000 * 1000 // 16 MB
	// maxWALFiles is the number of WAL files to keep around. Older WAL files will
	// be periodically garbage collected.
	// We support restarting a node from last 10 snapshots (maxSnapFiles), with
	// each snapshot being created roughly after 1000 entries
	// (numEntriesPerSnap).
	// This means that we need to have more than 10000 most recent entries in the
	// WAL at all times.  Empirically in our application, each WAL entry is less
	// than 100 bytes, which means that with 4 files * 16 MB, we can retain about
	// 640,000 entries which is much more than enough.
	maxWALFiles   = 4
	walFileSuffix = "wal"
	// numEntriesPerSnap is the number of raft entries after which a snapshot is
	// (synchronously) triggered.
	numEntriesPerSnap = uint64(1000)
	// maxSnapFiles is the number of raft snapshot files to retain. Each file
	// contains one snapshot, older snapshots will periodically garbage collected.
	// On a restart, we attempt to start the raft index from the newest snapshot
	// that we can successfully load, and replay the entries since that index from
	// WAL files.
	maxSnapFiles = 10
	// minDurationBetweenConfChangeSnapshots is the minimum duration between two
	// consecutive snapshots triggered due to a confChange entry.
	minDurationBetweenConfChangeSnapshots = 5 * time.Minute
	snapFileSuffix                        = "snap"
	// Run file garbage collection periodically at this interval.
	fileGCInterval = time.Minute
	// clusterRequestTimeout denote the timeout used for http calls to /cluster
	// endpoints. We wait for 10 seconds and have retries to handle situations
	// when the server doesn't reply for 10 seconds.
	clusterRequestTimeout = 10 * time.Second
)

type proposalFilter func(ctx context.Context, proposal interface{}) error

var propFilters struct {
	filters []proposalFilter
	mu      sync.RWMutex
}

func init() {
	// Override the default wal SegmentSizeBytes
	wal.SegmentSizeBytes = walSegmentSizeBytes
	capnslog.SetFormatter(&logFormatter{})
	propFilters.filters = make([]proposalFilter, 0)
}

func AddProposalFilter(f proposalFilter) {
	propFilters.mu.Lock()
	defer propFilters.mu.Unlock()
	propFilters.filters = append(propFilters.filters, f)
}

// logFormatter is used to convert capnslog log format to kronos log format.
type logFormatter struct{}

// Format logs capnslog line as a kronos log line
func (r *logFormatter) Format(
	pkg string, level capnslog.LogLevel, depth int, entries ...interface{},
) {
	ctx := context.TODO()
	logDepth := depth + 1
	switch level {
	case capnslog.CRITICAL, capnslog.ERROR:
		log.ErrorfDepth(ctx, logDepth, "", entries...)
	case capnslog.WARNING:
		log.WarningfDepth(ctx, logDepth, "", entries...)
	case capnslog.INFO, capnslog.NOTICE:
		log.InfofDepth(ctx, logDepth, "", entries...)
	case capnslog.DEBUG:
		if log.V(2) {
			log.InfofDepth(ctx, logDepth, "", entries...)
		}
	default:
		if log.V(3) {
			log.InfofDepth(ctx, logDepth, "", entries...)
		}
	}
}

// Flush flushes kronos logs
func (r *logFormatter) Flush() {
	log.Flush()
}

// We use special hardcoded messages to indicate replay done, and signal loading
// a snapshot. We know that these hardcoded values will not conflict with other
// serialized entries that we send and receive from raft in our application.
var replayedCommitsFromWALMsg = "__kronos_replay_done"
var loadSnapshotMsg = "__kronos_load_snapshot"
var unblockSnapshotMsg = "__kronos_unblock_snapshot"

// snapTriggerConfig is used to check if a snapshot should be triggered or not.
// Lock is not required in this struct as it only accessed by a single event
// loop goroutine.
type snapTriggerConfig struct {
	// snapCount is the number of raft entries after which a snapshot is
	// (synchronously) triggered.
	snapCount    uint64
	lastSnapTime time.Time
	// confChangeSinceLastSnap states if a confChange was applied since a snapshot
	// was triggered last.
	confChangeSinceLastSnap bool
}

func newSnapTriggerConfig(snapCount uint64) *snapTriggerConfig {
	return &snapTriggerConfig{
		confChangeSinceLastSnap: false,
		lastSnapTime:            time.Now(),
		snapCount:               snapCount,
	}
}

// markSnapDone refreshes the snapTriggerConfig and should be called after a
// snapshot has been taken.
func (c *snapTriggerConfig) markSnapDone() {
	c.confChangeSinceLastSnap = false
	c.lastSnapTime = time.Now()
}

// shouldTrigger returns if a raft snapshot should be triggered. It takes into
// account the config values, appliedIndex and snapshotIndex.
func (c *snapTriggerConfig) shouldTrigger(appliedIndex, snapshotIndex uint64) bool {
	// Trigger a snapshot when any of the following happens:-
	// 1. The wal exceeds snapCount.
	// 2. There is a confChange. A snapshot is required here so that new nodes
	// don't receive stale addresses in confChange entries in case of Re-IP. In
	// this case don't trigger if the last snapshot was triggered in last
	// minDurationBetweenConfChangeSnapshots to avoid multiple snapshots being
	// triggered during bootstrap when there are multiple add nodes.
	// NOTE: time.Since is used here to measure time difference, which uses
	// monotonic  time difference internally. So, this time difference is immune
	// to clock jumps.
	return (appliedIndex-snapshotIndex > c.snapCount) ||
		(c.confChangeSinceLastSnap &&
			time.Since(c.lastSnapTime) > minDurationBetweenConfChangeSnapshots)
}

// raftNode is a node in the raft cluster which manages the oracle state
// machine
// This implements rafthttp.Raft
type raftNode struct {
	// proposeC is where proposed messages are read from
	proposeC <-chan string
	// confChangeC is where raft configuration changes are read from
	confChangeC <-chan raftpb.ConfChange
	// commitC is where entries committed in raft are pushed
	commitC chan<- string
	// errorC is where errors in the raft session are pushed
	errorC chan<- error

	bootstrapReqC chan kronospb.BootstrapRequest

	nodeID    string // client ID for raft session
	clusterID uint64
	localAddr *kronospb.NodeAddr
	seedHosts []*kronospb.NodeAddr
	waldir    string            // path to WAL directory
	snapdir   string            // path to snapshot directory
	datadir   string            // path to data directory
	cluster   *metadata.Cluster // cluster metadata that is also persisted to a file

	getSnapshot func() ([]byte, error)
	// last committed index of the replay log at startup, this is used to determine when we
	// are done replaying the existing committed log entries.
	lastCommittedIndex uint64

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	gossip *gossip.Server

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
	// snapTriggerConfig is used to check when to trigger a snapshot.
	snapTriggerConfig *snapTriggerConfig
	listenHost        string

	bootstrappedStatus struct {
		sync.Mutex
		isBootstrapped bool
	}
	testMode bool
	lg       *zap.Logger
}

// getNodesIncludingRemoved gets nodes in the cluster metadata from
// the remote node. It retries internally for 1 minute.
func (rc *raftNode) getNodesIncludingRemoved(
	ctx context.Context,
	remote *kronospb.NodeAddr,
	timeout time.Duration,
) (nodes []kronoshttp.Node, err error) {
	log.Infof(ctx, "Getting nodes from %v", remote)
	err = kronoshttp.ForDuration(
		timeout,
		func() error {
			c, err := kronoshttp.NewClusterClient(remote, rc.transport.TLSInfo)
			if err != nil {
				log.Errorf(ctx, "Failed to create cluster client, error: %v", err)
				return err
			}
			defer c.Close()
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, clusterRequestTimeout)
			defer cancelFunc()
			nodes, err = c.Nodes(ctxWithTimeout)
			if err != nil {
				log.Errorf(ctxWithTimeout, "Failed to get nodes, error: %v", err)
				return err
			}
			return nil
		},
	)
	return
}

// getRaftStatus gets nodes in the cluster metadata from
// the remote node. It retries internally for 1 minute.
func (rc *raftNode) getRaftStatus(ctx context.Context, remote *kronospb.NodeAddr, timeout time.Duration) (statusResp *kronoshttp.RaftStatusResp, err error) {
	log.Infof(ctx, "Getting raft status from %v", remote)
	err = kronoshttp.ForDuration(
		timeout,
		func() error {
			c, err := kronoshttp.NewClusterClient(remote, rc.transport.TLSInfo)
			if err != nil {
				log.Errorf(ctx, "Failed to create cluster client, error: %v", err)
				return err
			}
			defer c.Close()
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, clusterRequestTimeout)
			defer cancelFunc()
			statusResp, err = c.RaftStatus(ctxWithTimeout)
			if err != nil {
				log.Errorf(ctxWithTimeout, "Failed to get nodes, error: %v", err)
				return err
			}
			return nil
		},
	)
	return
}

// extraNodes returns 2 sets, one set containing the nodes present in set A, but
//
//	not in B. Other set is one present in set B, but not in A.
func extraNodes(
	setA map[string]*kronospb.Node, setB []uint64,
) (extraInA map[string]struct{}, extraInB map[string]struct{}) {
	extraInA = make(map[string]struct{})
	// Add all the nodes to extraInA and then later remove all that are not in B.
	for nodeID := range setA {
		extraInA[nodeID] = struct{}{}
	}
	extraInB = make(map[string]struct{})
	for _, nID := range setB {
		nodeID := types.ID(nID).String()
		if _, ok := setA[nodeID]; ok {
			delete(extraInA, nodeID)
		} else {
			extraInB[nodeID] = struct{}{}
		}
	}
	return
}

// removeNode removes the given nodeID from cluster metadata and raft peers.
func (rc *raftNode) removeNode(id string) {
	rc.cluster.RemoveNode(id)
	raftID := raftID(id)
	// check if peerExists before removing the node. Otherwise, when old
	// remove node conf-entries are replayed eg. while replaying wal,
	// then transport panics as the peer doesn't exists.
	// This can happen if AddNode conf change has been snapshotted and
	// merged into conf state but remove node conf change is still a part
	// of WAL which is replayed.
	if rc.transport.Get(raftID) != nil {
		rc.transport.RemovePeer(raftID)
	}
}

// addNode adds the given nodeID to cluster metadata and raft peers with the
// given raft address.
func (rc *raftNode) addNode(id string, addr *kronospb.NodeAddr) error {
	if err := rc.cluster.AddNode(id, addr); err != nil {
		return err
	}
	url := kronosutil.AddrToURL(addr, !rc.transport.TLSInfo.Empty())
	rc.transport.AddPeer(raftID(id), []string{url.String()})
	return nil
}

// updateClusterFromConfState updates the cluster metadata and raft peers with
// the nodes present in the confstate. It fetches the current cluster metadata
// from raft leader and other nodes to get the metadata for the nodes which could've
// been added and then removed since the last snapshot.
func (rc *raftNode) updateClusterFromConfState(ctx context.Context, snapshotIndex uint64) {
	log.Infof(ctx, "Raft confstate: %v", rc.confState.Voters)
	// remove nodes extra in activeNodes and add nodes extra in confstate.
	nodesToRemove, nodesToAdd := extraNodes(rc.cluster.ActiveNodes(),
		rc.confState.Voters)

	// remove all the IDs not in confstate from cluster metadata and transport
	// peers as raft only talks to the nodes present in confstate.
	for nodeID := range nodesToRemove {
		if nodeID == rc.nodeID {
			log.Info(ctx, "Shutting down as node is removed from the cluster")
			rc.stop()
			return
		}
		rc.removeNode(nodeID)
	}

	getNodeAddr := func(addr string) (*kronospb.NodeAddr, error) {
		parsedUrl, err := url.Parse(addr)
		if err != nil {
			log.Errorf(ctx, "Failed to parse url %s, err: %v", addr, err)
			return nil, err
		}
		nodeAddr, err := kronosutil.NodeAddr(parsedUrl.Host)
		if err != nil {
			log.Errorf(ctx, "Failed to convert %s to NodeAddr, err: %v", parsedUrl.Host, err)
			return nil, err
		}
		return nodeAddr, nil
	}

	rc.gossip.WaitForNRoundsofGossip(time.Minute, 2)
	added := 0
	for nodeID := range nodesToAdd {
		nodeDesc := rc.gossip.GetNodeDesc(nodeID)
		addr, err := getNodeAddr(nodeDesc.RaftAddr)
		if err != nil {
			log.Errorf(ctx, "Failed to get node addr for node %s, err: %v", nodeID, err)
			continue
		}
		if err := rc.addNode(nodeID, addr); err != nil {
			log.Errorf(ctx, "Failed to add node id: %s, addr: %s to cluster metadata, err: %v",
				nodeID, addr, err)
		} else {
			added++
		}
	}

	// The below code is to handle the cases where a node was added and then removed before the snapshot was taken.
	// In such cases, the node will not be present in the confState but we still need to add it to the cluster metadata.
	// To handle this, we get the metadata of all the nodes from raft leader and other nodes and remove the nodes which
	// are marked as removed.
	raftLeader := rc.node.Status().Lead
	err := func() error {
		if raftLeader == raft.None {
			return errors.New("raft leader not found")
		}
		nodeID := types.ID(raftLeader).String()
		if nodeID == rc.nodeID {
			// This should never happen as we are not the leader.
			return errors.New("leader is this node")
		}
		nodeDesc := rc.gossip.GetNodeDesc(nodeID)
		if nodeDesc == nil {
			log.Errorf(ctx, "Node %s not found in gossip list", nodeID)
			return errors.New("node not found in gossip list")
		}
		addr, err := getNodeAddr(nodeDesc.RaftAddr)
		if err != nil {
			log.Errorf(ctx, "Failed to get node addr for node %s, err: %v", nodeID, err)
			return err
		}
		nodes, err := rc.getNodesIncludingRemoved(ctx, addr, 10*time.Second)
		if err != nil {
			log.Errorf(ctx, "Failed to get nodes from %s, err: %v", addr, err)
			return err
		}
		for _, node := range nodes {
			if node.IsRemoved {
				rc.removeNode(node.NodeID)
			}
		}
		return nil
	}()

	if err != nil {
		func() {
			for {
				for _, nodeDesc := range rc.gossip.GetNodeList() {
					if nodeDesc.IsRemoved || !nodeDesc.IsBootstrapped || nodeDesc.NodeId == rc.nodeID {
						continue
					}
					parsedUrl, err := url.Parse(nodeDesc.RaftAddr)
					if err != nil {
						log.Errorf(ctx, "Failed to parse url %s, err: %v", nodeDesc.RaftAddr, err)
						continue
					}
					addr, err := kronosutil.NodeAddr(parsedUrl.Host)
					if err != nil {
						log.Errorf(ctx, "Failed to convert %s to NodeAddr, err: %v", parsedUrl.Host, err)
						continue
					}
					nodes, err := rc.getNodesIncludingRemoved(ctx, addr, 10*time.Second)
					if err != nil {
						log.Errorf(ctx, "Failed to get nodes from %s, err: %v", addr, err)
						continue
					}
					for _, node := range nodes {
						if node.IsRemoved {
							rc.removeNode(node.NodeID)
						}
					}
					raftStatus, err := rc.getRaftStatus(ctx, addr, 10*time.Second)
					if err != nil {
						log.Errorf(ctx, "Failed to get raftstatus from %s, err: %v", addr, err)
						continue
					}
					if raftStatus.AppliedIndex >= snapshotIndex {
						return
					}
				}
				time.Sleep(5 * time.Second)
			}
		}()
	}

	if len(nodesToAdd) > added {
		// Don't fatal over here as it's possible that this is an older snapshot
		// and later a remove node comes in publishEntries because of which we don't
		// find this node in the metadata of the seed hosts. Log as error here as
		// the above is a rare case and this might help in debugging other issues.
		log.Errorf(
			ctx,
			"Failed to get metadata for all following nodes in the confState: %v",
			nodesToAdd,
		)
	}

	if err := rc.cluster.Persist(); err != nil {
		log.Fatalf(ctx, "Failed to persist cluster, error: %v", err)
	}
}

var _ rafthttp.Raft = &raftNode{}

type RaftNodeInfo struct {
	CommitC          <-chan string
	ErrorC           <-chan error
	SnapshotterReady <-chan *snap.Snapshotter
	BootstrapReqC    chan kronospb.BootstrapRequest
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// proposal channel proposeC. All log entries are replayed over the
// commit channel, followed by the replayedCommitsFromWALMsg message (to indicate the
// channel is current), then new log entries. To shutdown, close proposeC and
// read errorC.
func newRaftNode(
	rc *RaftConfig, getSnapshot func() ([]byte, error),
	proposeC <-chan string, nodeID string, g *gossip.Server,
	testMode bool,
) *RaftNodeInfo {
	ctx := context.Background()
	seedHosts, err := convertToNodeAddrs(rc.SeedHosts)
	if err != nil {
		log.Fatalf(ctx, "Failed to convert seedHosts to NodeAddr, error: %v", err)
	}
	commitC := make(chan string)
	errorC := make(chan error)
	confChangeC := make(chan raftpb.ConfChange)
	if rc.SnapCount <= 0 {
		rc.SnapCount = numEntriesPerSnap
	}
	var core zapcore.Core = log.Getlogger()
	lg := zap.New(core).WithOptions(zap.AddCaller())
	rn := &raftNode{
		proposeC:          proposeC,
		confChangeC:       confChangeC,
		commitC:           commitC,
		errorC:            errorC,
		nodeID:            nodeID,
		seedHosts:         seedHosts,
		localAddr:         rc.RaftHostPort,
		datadir:           rc.DataDir,
		waldir:            filepath.Join(rc.DataDir, "kronosraft"),
		snapdir:           filepath.Join(rc.DataDir, "kronosraft-snap"),
		getSnapshot:       getSnapshot,
		stopc:             make(chan struct{}),
		httpstopc:         make(chan struct{}),
		httpdonec:         make(chan struct{}),
		snapTriggerConfig: newSnapTriggerConfig(rc.SnapCount),
		snapshotterReady:  make(chan *snap.Snapshotter, 1),
		gossip:            g,
		bootstrapReqC:     make(chan kronospb.BootstrapRequest),
		listenHost:        rc.ListenHost,
		testMode:          testMode,
		lg:                lg,
		// rest of structure populated after WAL replay
	}
	go rn.startRaft(ctx, confChangeC, rc.CertsDir, rc.GRPCHostPort)
	return &RaftNodeInfo{commitC, errorC, rn.snapshotterReady, rn.bootstrapReqC}
}

func (rc *raftNode) purgeFiles(ctx context.Context) {
	snaperrc := fileutil.PurgeFile(rc.lg, rc.snapdir, snapFileSuffix,
		maxSnapFiles, fileGCInterval, rc.stopc)
	walerrc := fileutil.PurgeFile(rc.lg, rc.waldir, walFileSuffix, maxWALFiles,
		fileGCInterval, rc.stopc)
	select {
	case e := <-snaperrc:
		log.Fatalf(ctx, "Failed to purge snap files, err: %v", e)
	case e := <-walerrc:
		log.Fatalf(ctx, "Failed to purge wal files, err: %v", e)
	case <-rc.stopc:
		return
	}
}

func (rc *raftNode) gcInvalidSnapshots(ctx context.Context) {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.lg, rc.waldir)
		if err == nil {
			fnames, err := fileutil.ReadDir(rc.snapdir)
			if err != nil {
				log.Errorf(ctx, "Failed to read snap dir, err: %v", err)
			} else {
				for _, fname := range fnames {
					log.Infof(ctx, "Checking snap file %s", fname)
					if strings.HasSuffix(fname, snapFileSuffix) {
						found := false
						for _, snap := range walSnaps {
							// This should match the file naming scheme in
							// vendor/go.etcd.io/etcd/server/v3/etcdserver/api/snap/snapshotter.go
							snapFname := fmt.Sprintf("%016x-%016x.%s", snap.Term, snap.Index, snapFileSuffix)
							if snapFname == fname {
								found = true
								break
							}
						}
						if !found {
							log.Infof(ctx, "Removing snap file %s since its not a valid snapshot entry", fname)
							if err := os.Remove(filepath.Join(rc.snapdir, fname)); err != nil {
								log.Errorf(ctx, "Failed to remove snap file %s, err: %v", fname, err)
							}
						}
					}
				}
			}
		}
	}
}

// saveSnap saves the given snapshot
func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

// entriesToApply returns the entries to apply to the raft node from the
// raft log based on committed entries
func (rc *raftNode) entriesToApply(
	ctx context.Context, committedEntries []raftpb.Entry,
) (nents []raftpb.Entry) {
	if len(committedEntries) == 0 {
		return
	}
	firstIdx := committedEntries[0].Index
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf(
			ctx,
			"First index of committed entry[%d] "+
				"should be <= progress.appliedIndex[%d]+1",
			firstIdx,
			rc.appliedIndex,
		)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(committedEntries)) {
		nents = committedEntries[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(
	ctx context.Context,
	ents []raftpb.Entry,
) bool {
	// This is the only function where rc.cluster is being modified while the
	// raft node is active. This is called synchronously by the caller. So there
	// is no need to provide thread-safety to cluster.
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(ents[i].Data, &cc); err != nil {
				log.Fatal(ctx, err)
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				log.Infof(ctx, "Processing add node conf change: %v", cc)
				if len(cc.Context) > 0 {
					nodeID := types.ID(cc.NodeID).String()
					if node, ok := rc.cluster.Node(nodeID); ok && node.IsRemoved {
						log.Warningf(ctx, "Removed node %v cannot be added back to the cluster", nodeID)
						break
					}

					// avoid adding self as peer
					if kronosutil.NodeAddrToString(rc.localAddr) != string(cc.Context) {
						rc.transport.AddPeer(
							types.ID(cc.NodeID),
							[]string{withProtocol(string(cc.Context), !rc.transport.TLSInfo.Empty())},
						)
						log.Infof(ctx, "Added peer id: %s", nodeID)
					}

					addr, err := kronosutil.NodeAddr(string(cc.Context))
					if err != nil {
						log.Errorf(ctx, "Failed to convert %s to NodeAddr, err: %v", string(cc.Context), err)
						return false
					}

					if _, ok := rc.cluster.Node(nodeID); ok {
						log.Infof(ctx, "Node to add %v already exists in cluster metadata", nodeID)
						break // this exits the switch case
					}

					// This is the single goroutine where write operations are performed
					// on cluster once the node starts.
					// Hence we do not require a lock between Node(), AddNode() and
					// Persist()
					if err := rc.cluster.AddNode(nodeID, addr); err != nil {
						log.Errorf(
							ctx, "Failed to add node id: %s, addr: %s to cluster metadata, err: %v",
							nodeID, addr, err,
						)
						return false
					}
					if err := rc.cluster.Persist(); err != nil {
						log.Errorf(
							ctx,
							"Failed to persist cluster metadata after adding the node, error: %v",
							err,
						)
						return false
					}
					log.Infof(ctx, "Successfully added node %v", nodeID)
				}
			case raftpb.ConfChangeRemoveNode:
				log.Infof(ctx, "Processing remove node conf change %v", cc)
				nodeID := types.ID(cc.NodeID).String()
				rc.removeNode(nodeID)
				rc.gossip.RemovePeer(nodeID)

				if err := rc.cluster.Persist(); err != nil {
					log.Errorf(
						ctx,
						"Failed to persist cluster metadata after remove node, error: %v",
						err,
					)
					return false
				}
				if nodeID == rc.nodeID {
					rc.gossip.SetRemoved(ctx, true)
					rc.gossip.WaitForNRoundsofGossip(time.Minute, 2)
					log.Info(ctx, "Shutting down as node is removed from the cluster")
					return false
				}
				log.Infof(ctx, "Successfully removed node %s", nodeID)
			}
			// received a confChange, mark the confChangeSinceLastSnap true to
			// actively trigger a snapshot to remove the confChange entries asap as
			// they contain stale addresses which can create edge cases when Re-IP and
			// node addition happens in short intervals.
			rc.snapTriggerConfig.confChangeSinceLastSnap = true
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

	}
	return true
}

// loadSnapshot returns the latest valid snapshot
func (rc *raftNode) loadSnapshot(ctx context.Context) *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf(ctx, "Error loading snapshot (%v)", err)
	}
	if wal.Exist(rc.waldir) && err != snap.ErrNoSnapshot {
		// ValidSnapshotEntries returns the snapshots that are valid (which have index <= last committed index in hard state)
		walSnaps, err := wal.ValidSnapshotEntries(rc.lg, rc.waldir)
		if err != nil {
			log.Errorf(ctx, "Failed to read wal snapshots, err: %v", err)
			return snapshot
		}
		snapshot, err = rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil {
			log.Errorf(ctx, "Failed to load newest available snapshot, err: %v", err)
			return snapshot
		}
	}
	return snapshot
}

// readWal reads WAL entries starting at snap. It also repairs WAL
// ErrUnexpectedEOF errors in WAL files which can arise if there were past
// records that were only partially flushed (and therefore never synced.)
func (rc *raftNode) readWAL(
	ctx context.Context, snap *raftpb.Snapshot,
) (w *wal.WAL, st raftpb.HardState, ents []raftpb.Entry) {
	var err error
	if !wal.Exist(rc.waldir) {
		if err = os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf(ctx, "Cannot create dir for wal, err: %v", err)
		}
		w, err = wal.Create(rc.lg, rc.waldir, nil)
		if err != nil {
			log.Fatalf(ctx, "Create wal err: %v", err)
		}
		if err := w.Close(); err != nil {
			log.Fatalf(ctx, "Close wal err: %v", err)
		}
		log.Infof(ctx, "Created wal dir %s", rc.waldir)
	}

	walsnap := walpb.Snapshot{}
	if snap != nil {
		walsnap.Index, walsnap.Term = snap.Metadata.Index, snap.Metadata.Term
	}
	log.Infof(
		ctx,
		"Opening WAL at term %d and index %d",
		walsnap.Term,
		walsnap.Index,
	)

	repaired := false
	for {
		if w, err = wal.Open(rc.lg, rc.waldir, walsnap); err != nil {
			log.Fatalf(ctx, "Failed to open wal, err: %v", err)
		}
		if _, st, ents, err = w.ReadAll(); err != nil {
			log.Errorf(ctx, "Failed to read entries from wal, read wal err: %v", err)
			// We can only repair ErrUnexpectedEOF
			if err != io.ErrUnexpectedEOF {
				log.Fatalf(ctx, "Cannot repair read wal err: %v, can only repair %v", err, io.ErrUnexpectedEOF)
			}
			// We do not repair more than once.
			if repaired {
				log.Fatalf(ctx, "Cannot repair wal more than once, last err: %v", err)
			}
			if err := w.Close(); err != nil {
				log.Errorf(ctx, "Ignoring close wal err: %v", err)
			}
			if !wal.Repair(rc.lg, rc.waldir) {
				log.Fatalf(ctx, "Failed to repair WAL")
			} else {
				log.Infof(ctx, "Successfully repaired WAL")
				repaired = true
			}
			continue
		}
		break
	}
	return w, st, ents
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL(ctx context.Context) *wal.WAL {
	log.Info(ctx, "Loading a previous successful snapshot, if any")
	snapshot := rc.loadSnapshot(ctx)
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		log.Infof(ctx, "Applying snapshot to memory storage, snapshot index: %d", snapshot.Metadata.Index)
		if err := rc.raftStorage.ApplySnapshot(*snapshot); err != nil {
			log.Fatal(ctx, err)
		}
		rc.publishSnapshot(ctx, *snapshot)
		// Currently we don't update cluster metadata for on-disk snapshots as this
		// can cause deadlocks when all the nodes restart. As even raft transport
		// of seedhosts won't be initialized, so it can't query for nodes metadata.
		// We should either update cluster metadata from on-disk snapshot and wal
		// entries or not from either to be logically sound (Correctness-wise it's
		// fine as operations are idempotent). One possible solution is not to play
		// entries till we send replayedWalMsg (do it for the last snapshot).
	} else {
		log.Info(ctx, "No snapshot found")
	}

	log.Info(ctx, "Reading WAL for entries after snapshot")
	w, st, ents := rc.readWAL(ctx, snapshot)

	if err := rc.raftStorage.SetHardState(st); err != nil {
		log.Fatal(ctx, err)
	}
	log.Info(ctx, "Replayed WAL entries to memory storage")

	// append to storage so raft starts at the right place in log
	if err := rc.raftStorage.Append(ents); err != nil {
		log.Fatal(ctx, err)
	}

	if len(ents) > 0 && st.Commit >= ents[0].Index {
		// if there are committed entries to be replayed, note the last index so that we can
		// tell when all past entries are published in the raft loop, and indicate
		// that to the client. replayedCommitsFromWALMsg is pushed to commitC by publishEntries
		// when all the entries found here are replayed.
		// Replaying upto the last committed index is sufficient (and necessary) as the
		// following invariant holds for any past runs of the process:
		// applied_index <= commit_index(on persisted WAL)
		// This is sufficient to ensure we don't go back in terms of the state visible
		// to the kronos time-server.
		rc.lastCommittedIndex = st.Commit
		log.Infof(ctx, "There are %d WAL entries to be replayed. lastCommittedIndex: %d",
			st.Commit-ents[0].Index+1, rc.lastCommittedIndex)
	} else {
		// if there is nothing to replay, indicate to the client that replay is
		// already done. also explicitly initialize lastCommittedIndex to 0, so that it never
		// matches an entry index later and the replayedCommitsFromWALMsg is not sent again
		// (first entry we ever receive from raft has Index 1.)
		rc.lastCommittedIndex = 0
		rc.commitC <- replayedCommitsFromWALMsg
	}

	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func withProtocol(addr string, secure bool) string {
	if secure {
		return "https://" + addr
	}
	return "http://" + addr
}

func isFirstSeedHost(seedHosts []*kronospb.NodeAddr, localAddr *kronospb.NodeAddr) bool {
	return proto.Equal(seedHosts[0], localAddr)
}

func raftID(nodeID string) types.ID {
	raftID, err := types.IDFromString(nodeID)
	if err != nil {
		panic(errors.Wrapf(err, "failed to convert nodeID %s to raftID", nodeID))
	}
	return raftID
}

func (rc *raftNode) tryIdempotentRpc(
	ctx context.Context,
	tlsInfo transport.TLSInfo,
	timeout time.Duration,
	nodeAddr *kronospb.NodeAddr,
	rpc func(ctx context.Context, client *kronoshttp.ClusterClient) error,
) error {
	var lastTryError error
	if proto.Equal(nodeAddr, rc.localAddr) {
		return errors.New("Cannot execute rpc on local node")
	}
	log.Infof(ctx, "Executing rpc on %v", *nodeAddr)
	lastTryError = kronoshttp.RetryUntil(
		ctx,
		timeout,
		func() error {
			c, err := kronoshttp.NewClusterClient(nodeAddr, tlsInfo)
			if err != nil {
				log.Errorf(ctx, "Failed to create cluster client, error: %v", err)
				return err
			}
			defer c.Close()
			ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, clusterRequestTimeout)
			defer cancelFunc()
			err = rpc(ctxWithTimeout, c)
			if err != nil {
				log.Errorf(ctx, "RPC to %v failed with error: %v",
					*nodeAddr, err)
				return errors.Wrapf(err, "RPC to %v failed", *nodeAddr)
			}
			return nil
		},
	)
	if lastTryError != nil {
		log.Errorf(ctx, "Failed to execute rpc on %v, error: %v", *nodeAddr,
			lastTryError)
	}
	return lastTryError
}

func (rc *raftNode) checkDuplicate(ctx context.Context,
	nodes []kronoshttp.Node) {
	for _, node := range nodes {
		if node.IsRemoved {
			continue
		}
		if proto.Equal(node.RaftAddr, rc.localAddr) && node.
			NodeID != rc.nodeID {
			// There is already a kronos node with a different nodeID
			// with the same raft addr
			log.Fatalf(ctx,
				"There is already a node %v with the same raft"+
					" address %v. Remove it from the cluster before"+
					" adding this node with a new ID.", node.NodeID,
				node.RaftAddr)
		}
	}
}

func (rc *raftNode) updateClusterFile(desc *kronospb.NodeDescriptor) error {
	parsedUrl, err := url.Parse(desc.RaftAddr)
	if err != nil {
		return errors.Wrapf(err, "failed to parse url %s", desc.RaftAddr)
	}
	addr, err := kronosutil.NodeAddr(parsedUrl.Host)
	if err != nil {
		return errors.Wrapf(err, "failed to convert %s to NodeAddr", desc.RaftAddr)
	}

	changed, err := rc.cluster.UpdateNode(desc.NodeId, addr)
	if err != nil {
		return errors.Wrapf(err, "failed to update node %s in cluster metadata", desc.NodeId)
	}
	if changed {
		err := rc.cluster.Persist()
		if err != nil {
			return errors.Wrapf(err, "failed to persist cluster metadata after updating node %s", desc.NodeId)
		}
	}
	return nil
}

func (rc *raftNode) maybeAddRemote(ctx context.Context,
	desc *kronospb.NodeDescriptor) {
	if desc == nil {
		return
	}
	if desc.IsRemoved {
		rc.removeNode(desc.NodeId)
		if log.V(1) {
			log.Infof(ctx, "Node %v is removed, skipping", desc.NodeId)
		}
		return
	}
	if !kronosutil.IsValidRaftAddr(desc.RaftAddr) {
		log.Errorf(ctx, "Invalid raft address for node %v : %v",
			desc.NodeId, desc.RaftAddr)
		return
	}
	typedNodeId, err := types.IDFromString(desc.NodeId)
	if err != nil {
		log.Errorf(ctx, "Failed to convert node id to raft id, error: %v", err)
		return
	}
	if rc.transport != nil {
		rc.transport.AddRemote(typedNodeId, []string{desc.RaftAddr})
		rc.transport.UpdatePeer(typedNodeId, []string{desc.RaftAddr})
	}
}

func (rc *raftNode) tryJoin(ctx context.Context, joinCh chan struct{}, tlsInfo transport.TLSInfo) {
	ticker := time.NewTicker(time.Second)
	for {
		nodes := rc.gossip.GetNodeList()
		for _, node := range nodes {
			select {
			case <-ctx.Done():
				return
			case _ = <-ticker.C:
				// try joining the cluster
			}
			if node.NodeId == rc.nodeID {
				continue
			}
			if !node.IsBootstrapped || !gossip.IsNodeLive(node) {
				continue
			}

			log.Infof(ctx, "Trying to join node %v", node)

			parsedUrl, _ := url.Parse(node.RaftAddr)
			nodeAddr := &kronospb.NodeAddr{Host: parsedUrl.
				Hostname(), Port: parsedUrl.Port()}

			err := rc.tryIdempotentRpc(ctx, tlsInfo, time.Minute, nodeAddr,
				func(ctx context.Context, client *kronoshttp.ClusterClient) error {
					nodes, err := client.Nodes(ctx)
					if err != nil {
						return err
					}
					rc.checkDuplicate(ctx, nodes)

					rc.bootstrappedStatus.Lock()
					defer rc.bootstrappedStatus.Unlock()

					if rc.bootstrappedStatus.isBootstrapped {
						return nil
					}

					clusterID, err := client.AddNode(ctx, &kronoshttp.AddNodeRequest{
						NodeID:  rc.nodeID,
						Address: kronosutil.NodeAddrToString(rc.localAddr),
					})
					if err != nil {
						return err
					}

					rc.clusterID = clusterID.ClusterID
					err = metadata.PersistClusterUUID(ctx, rc.datadir, types.ID(rc.clusterID))
					if err != nil {
						return err
					}

					rc.bootstrappedStatus.isBootstrapped = true
					rc.gossip.SetBootstrapped(ctx, true)

					return nil
				})
			if err == nil {
				log.Infof(ctx, "Successfully joined node %v", node)
				close(joinCh)
				return
			} else {
				log.Errorf(ctx, "Failed to join cluster, error: %v", err)
			}
		}
	}
}

func (rc *raftNode) tryBootstrap(
	ctx context.Context,
	bootStrapCh chan struct{},
	joinCh chan struct{},
) {
	select {
	case <-joinCh:
		return
	case _ = <-rc.bootstrapReqC:
	}
	func() {
		rc.bootstrappedStatus.Lock()
		defer func() {
			close(bootStrapCh)
			rc.bootstrappedStatus.Unlock()
		}()

		if rc.bootstrappedStatus.isBootstrapped {
			return
		}
		log.Infof(ctx,
			"Bootstrapping a new cluster as a bootstrap request has been received")
		rc.bootstrappedStatus.isBootstrapped = true
		rc.gossip.SetBootstrapped(ctx, true)
		rc.clusterID = uint64(metadata.FetchOrAssignClusterUUID(ctx, rc.datadir, false))
	}()
}

// startRaft replays the existing WAL and joins the raft cluster
func (rc *raftNode) startRaft(
	ctx context.Context,
	confChangeC chan<- raftpb.ConfChange,
	certsDir string,
	grpcAddr *kronospb.NodeAddr,
) {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf(ctx, "Cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(rc.lg, rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	rc.gcInvalidSnapshots(ctx)
	rc.wal = rc.replayWAL(ctx)

	selfID := raftID(rc.nodeID)
	c := &raft.Config{
		ID:              uint64(selfID),
		ElectionTick:    15,
		HeartbeatTick:   5,
		PreVote:         true,
		CheckQuorum:     true,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   16 * 1024,
		MaxInflightMsgs: 64,
		Logger:          NewRaftLogger(),
	}

	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		log.Fatalf(ctx, "Failed to get lastIndex from store, err: %v", err)
	}
	// If the log is empty, this is a new node (StartNode); otherwise it's
	// restoring an existing node (RestartNode).
	isNodeInitialized := lastIndex != 0

	tlsInfo := kronosutil.TLSInfo(certsDir)

	if isNodeInitialized {
		rc.bootstrappedStatus.Lock()
		rc.bootstrappedStatus.isBootstrapped = true
		rc.gossip.SetBootstrapped(ctx, true)
		rc.bootstrappedStatus.Unlock()
		// Cluster metadata will exist for initialized nodes
		rc.cluster, err = metadata.LoadCluster(rc.datadir, false /*readOnly*/)
		if err != nil {
			log.Fatalf(ctx, "Failed to create cluster, error: %v", err)
		}
		log.Infof(ctx, "Initialized cluster: %+v", rc.cluster)
		// Raft cluster had already been initialized
		rc.node = raft.RestartNode(c)
		rc.clusterID = uint64(metadata.FetchOrAssignClusterUUID(ctx, rc.datadir, true))
	} else {
		joinCtx, cancelJoin := context.WithCancel(ctx)
		defer cancelJoin()

		var startingPeers []raft.Peer
		joinCh := make(chan struct{})
		bootStrapCh := make(chan struct{})

		go rc.tryJoin(joinCtx, joinCh, tlsInfo)
		go rc.tryBootstrap(ctx, bootStrapCh, joinCh)

		rc.gossip.WaitForNRoundsofGossip(time.Minute, 2)
		select {
		case <-bootStrapCh:
			startingPeers = []raft.Peer{{ID: uint64(selfID), Context: []byte(kronosutil.NodeAddrToString(rc.localAddr))}}
			cancelJoin()
		case <-joinCh:
		case <-rc.stopc:
			return
		}
		// Create an empty cluster. This will be populated by confChange entries/
		// while loading remote snapshots and getting metadata from other nodes.
		rc.cluster, err = metadata.NewCluster(rc.datadir, nil /* cluster */)
		if err != nil {
			log.Fatalf(ctx, "Error creating cluster metadata using data from seed host: %v", err)
		}
		if len(startingPeers) > 0 {
			rc.node = raft.StartNode(c, startingPeers)
		} else {
			rc.node = raft.RestartNode(c)
		}
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.lg,
		ID:          selfID,
		ClusterID:   types.ID(rc.clusterID),
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(rc.lg, fmt.Sprintf("%v", rc.nodeID)),
		ErrorC:      make(chan error),
		DialTimeout: 5 * time.Second,
		TLSInfo:     tlsInfo,
	}

	if err := rc.transport.Start(); err != nil {
		log.Fatalf(ctx, "Failed to start raft transport, error: %v", err)
	}

	// Add remotes in case some of the nodes are in old version, they won't gossip their address
	// our best guess is to use the address in the cluster metadata.
	for id, node := range rc.cluster.ActiveNodes() {
		if id == rc.nodeID {
			continue
		}
		if node.IsRemoved {
			continue
		}
		typedID, err := types.IDFromString(id)
		if err != nil {
			log.Errorf(ctx, "Failed to convert node id %v to raft id, error: %v", id, err)
			continue
		}
		addrToURL := kronosutil.AddrToURL(node.RaftAddr, !tlsInfo.Empty())
		rc.transport.AddPeer(typedID, []string{addrToURL.String()})
	}

	rc.gossip.RegisterCallback(gossip.NodeDescriptorPrefix, func(
		g *gossip.Server, key gossip.GossipKey, info *kronospb.Info) {
		var desc kronospb.NodeDescriptor
		if err := proto.Unmarshal(info.Data, &desc); err != nil {
			log.Errorf(ctx, "Failed to unmarshal node descriptor, error: %v", err)
			return
		}
		err := rc.updateClusterFile(&desc)
		if err != nil {
			log.Errorf(ctx, "Failed to update cluster metadata, error: %v", err)
		}
		if desc.NodeId == rc.nodeID {
			return
		}
		rc.maybeAddRemote(ctx, &desc)
	}, true)

	log.Infof(ctx, "Raft Address: %v, raft Id: %v", rc.localAddr, rc.nodeID)

	// At this point both snapdir and waldir must have already been created, so we
	// can start the purge goroutines.
	go rc.purgeFiles(ctx)
	go rc.serveRaft(ctx, confChangeC, grpcAddr)
	go rc.serveChannels(ctx)
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

// publishSnapshot publishes the given snapshot and requests the state machine
// to load a snapshot
func (rc *raftNode) publishSnapshot(ctx context.Context, snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}

	log.Infof(
		ctx,
		"Publishing snapshot at index: %d, last snapshot index: %d",
		snap.Metadata.Index,
		rc.snapshotIndex,
	)

	if snap.Metadata.Index <= rc.appliedIndex {
		log.Fatalf(
			ctx,
			"Snapshot index [%d] should > progress.appliedIndex [%d] + 1",
			snap.Metadata.Index,
			rc.appliedIndex,
		)
	}

	rc.commitC <- loadSnapshotMsg // trigger kronos to load snapshot

	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index
	log.Infof(ctx, "Finished publishing snapshot, new snapshot index: %d", rc.snapshotIndex)
}

// maybeTriggerSnapshot triggers a snapshot if the number of applied entries
// since the last snapshot exceeds the snapCount
func (rc *raftNode) maybeTriggerSnapshot(ctx context.Context) {
	if !rc.snapTriggerConfig.shouldTrigger(rc.appliedIndex, rc.snapshotIndex) {
		return
	}

	log.Infof(
		ctx,
		"Starting snapshot [applied index: %d | last snapshot index: %d]",
		rc.appliedIndex,
		rc.snapshotIndex,
	)
	// Send the unblockSnapshotMsg on commitC right before we call getSnapshot.
	// The client unblocks and executes the next getSnapshot call only once it has
	// received the unblockSnapshotMsg, which makes sure that any previously
	// published commits on commitC have been fully applied before the snapshot is
	// taken. Note that it is necessary that we call getSnapshot exactly once
	// immediately after the unblockSnapshotMsg is sent otherwise the client may
	// block indefinitely waiting for the getSnapshot call to appear.
	rc.commitC <- unblockSnapshotMsg
	data, err := rc.getSnapshot()
	if err != nil {
		log.Fatal(ctx, err)
	}
	snapshot, err := rc.raftStorage.CreateSnapshot(
		rc.appliedIndex,
		&rc.confState,
		data,
	)
	if err != nil {
		log.Fatal(ctx, err)
	}
	if err := rc.saveSnap(snapshot); err != nil {
		log.Fatal(ctx, err)
	}

	// Retain history so that we can recover from last 3 snapshots
	// Raft logs prior to compactIndex are removed
	compactIndex := uint64(0)
	retentionEntryCount := maxSnapFiles*rc.snapTriggerConfig.snapCount + 1
	if rc.appliedIndex > retentionEntryCount {
		compactIndex = rc.appliedIndex - retentionEntryCount
	}

	if compactIndex > 0 {
		log.Infof(ctx, "Compacting log at index %d", compactIndex)
		if err := rc.raftStorage.Compact(compactIndex); err != nil {
			if err == raft.ErrCompacted {
				log.Info(ctx, "Compaction not required")
			} else {
				log.Fatalf(ctx, "Compaction failed, err: %v", err)
			}
		} else {
			log.Infof(ctx, "Compacted log at index %d", compactIndex)
		}
	}

	rc.snapshotIndex = rc.appliedIndex
	// Refresh the snapTriggerConfig as a snapshot was just completed.
	rc.snapTriggerConfig.markSnapDone()
	log.Infof(ctx, "Completed snapshot [snapshot index: %d]", rc.snapshotIndex)
}

func (rc *raftNode) serveChannels(ctx context.Context) {
	snapshot, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snapshot.Metadata.ConfState
	rc.snapshotIndex = snapshot.Metadata.Index
	rc.appliedIndex = snapshot.Metadata.Index

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer kronosutil.CloseWithErrorLog(ctx, rc.wal)

	// send proposals over raft
	go func() {
		var confChangeCount uint64

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					if kronosutil.IsOracleProposal([]byte(prop)) {
						if rc.isNodeEligibleForOracle() {
							err := rc.node.Propose(ctx, []byte(prop))
							if err != nil {
								log.Error(ctx, err)
							}
						} else {
							log.Infof(ctx, "Node is not eligible for oracle, skipping proposal")
						}
					} else {
						err := rc.node.Propose(ctx, []byte(prop))
						if err != nil {
							log.Error(ctx, err)
						}
					}
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					if err := rc.node.ProposeConfChange(ctx, cc); err != nil {
						log.Error(ctx, err)
					}
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rc.saveSnap(rd.Snapshot); err != nil {
					log.Fatal(ctx, err)
				}
				// gofail: var crashBeforeSaveHardState struct{}
			}
			if err := rc.wal.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatal(ctx, err)
			}
			appliedIndexBeforePublishing := rc.appliedIndex
			if !raft.IsEmptySnap(rd.Snapshot) {
				// gofail: var crashBeforeSaveSnapshot struct{}
				if err := rc.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatal(ctx, err)
				}
				rc.publishSnapshot(ctx, rd.Snapshot)
				log.Infof(ctx, "Updating cluster metadata")
				rc.updateClusterFromConfState(ctx, rd.Snapshot.Metadata.Index)
			}

			if err := rc.raftStorage.Append(rd.Entries); err != nil {
				log.Fatal(ctx, err)
			}

			rc.transport.Send(rc.sanitizeOutgoingMessages(rd.Messages))
			ents := rc.entriesToApply(ctx, rd.CommittedEntries)
			if ok := rc.publishEntries(ctx, ents); !ok {
				rc.stop()
				return
			}
			if appliedIndexBeforePublishing < rc.lastCommittedIndex {
				// Only log if we are catching up, to prevent spam.
				log.Infof(ctx, "Published %d entries", len(ents))
			}
			// If the publish moved us ahead or at par to the state at startup, send
			// the special replayedCommitsFromWALMsg on commit channel to signal replay has
			// finished.
			if appliedIndexBeforePublishing < rc.lastCommittedIndex && rc.appliedIndex >= rc.lastCommittedIndex {
				rc.commitC <- replayedCommitsFromWALMsg
			}

			rc.maybeTriggerSnapshot(ctx)
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

// serveRaft runs the raft HTTP server
func (rc *raftNode) serveRaft(
	ctx context.Context, confChangeC chan<- raftpb.ConfChange,
	grpcAddr *kronospb.NodeAddr,
) {
	// Listen on all interfaces
	host := net.JoinHostPort(rc.listenHost, rc.localAddr.Port)
	ln, err := kronosutil.NewStoppableListener(host, rc.httpstopc)
	if err != nil {
		log.Fatalf(ctx, "Failed to listen rafthttp (%v)", err)
	}

	handler := rc.transport.Handler().(*http.ServeMux)
	// Add status handler which can be called to check if the raft HTTP server
	// is up.
	handler.Handle("/status", kronoshttp.NewStatusHandler(rc.nodeID))
	// Add clusterOpsHandler which can be used to do cluster Operations.
	handler.Handle(
		fmt.Sprintf("/%s/", kronoshttp.ClusterPath),
		kronoshttp.NewClusterHandler(confChangeC, rc.datadir, grpcAddr, rc.node),
	)

	httpServer := &http.Server{Handler: handler}
	var tlsConfig *tls.Config
	if !rc.transport.TLSInfo.Empty() {
		tlsConfig, err = rc.transport.TLSInfo.ServerConfig()
		if err != nil {
			log.Fatalf(ctx, "Failed to get tls config: %v", err)
		}
		tlsConfig.MinVersion, tlsConfig.MaxVersion = kronosutil.GetTLSVersions()
		tlsConfig.CipherSuites = kronosutil.GetTls12CipherSuites()
		httpServer.TLSConfig = tlsConfig
		err = httpServer.ServeTLS(
			ln,
			// ServeTLS takes empty args because TLSConfig is provided.
			"", /* certFile */
			"", /* keyFile */
		)
	} else {
		err = httpServer.Serve(ln)
	}
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf(ctx, "Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func filterProposal(ctx context.Context, data []byte) error {
	propFilters.mu.RLock()
	defer propFilters.mu.RUnlock()
	for _, filter := range propFilters.filters {
		if err := filter(ctx, data); err != nil {
			return err
		}
	}
	return nil
}

// Process processes the given raft message coming from raft transport
func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	status := rc.node.Status()
	if status.RaftState == raft.StateLeader && m.Type == raftpb.MsgProp {
		newEntries := make([]raftpb.Entry, 0, len(m.Entries))
		for i := range m.Entries {
			err := filterProposal(ctx, m.Entries[i].Data)
			if err == nil {
				newEntries = append(newEntries, m.Entries[i])
			} else {
				log.Infof(ctx, "Dropping proposal, error: %v", err)
			}
		}
		m.Entries = newEntries
		if len(newEntries) > 0 {
			return rc.node.Step(ctx, m)
		} else {
			return nil
		}
	} else {
		return rc.node.Step(ctx, m)
	}
}

// IsIDRemoved returns whether the given raft node ID has been removed from this
// cluster.
// This is used by raft HTTP to drop messages from removed nodes
func (rc *raftNode) IsIDRemoved(id uint64) bool {
	typesID := types.ID(id).String()
	node, ok := rc.cluster.Node(typesID)
	if !ok {
		log.Errorf(context.TODO(), "Peer %v not found in cluster metadata", typesID)
		return false
	}

	return node.IsRemoved
}

// ReportUnreachable reports the given node is not reachable for the last send.
func (rc *raftNode) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}

// ReportSnapshot reports the status of the sent snapshot.
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

func (rc *raftNode) sanitizeOutgoingMessages(ms []raftpb.Message) []raftpb.Message {
	for i := 0; i < len(ms); i++ {
		if ms[i].Type == raftpb.MsgSnap {
			// When there is a `raftpb.EntryConfChange` after creating the snapshot,
			// then the confState included in the snapshot is out of date. so We need
			// to update the confState before sending a snapshot to a follower.
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
	}
	return ms
}

func (rc *raftNode) isNodeEligibleForOracle() bool {
	if !rc.testMode {
		return true
	}
	if os.Getenv("LEADER_NOT_ORACLE") != "" {
		return rc.node.Status().RaftState != raft.StateLeader && rc.node.Status().Lead != raft.None
	} else {
		return true
	}
}
