package oracle

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/coreos/pkg/capnslog"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/scaledata/etcd/etcdserver/stats"
	"github.com/scaledata/etcd/pkg/fileutil"
	"github.com/scaledata/etcd/pkg/types"
	"github.com/scaledata/etcd/raft"
	"github.com/scaledata/etcd/raft/sdraftpb"
	"github.com/scaledata/etcd/rafthttp"
	"github.com/scaledata/etcd/snap"
	"github.com/scaledata/etcd/wal"
	"github.com/scaledata/etcd/wal/sdwalpb"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
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

func init() {
	// Override the default wal SegmentSizeBytes
	wal.SegmentSizeBytes = walSegmentSizeBytes
	capnslog.SetFormatter(&logFormatter{})
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
var replayedWALMsg = "__kronos_replay_done"
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
	confChangeC <-chan sdraftpb.ConfChange
	// commitC is where entries committed in raft are pushed
	commitC chan<- string
	// errorC is where errors in the raft session are pushed
	errorC chan<- error

	nodeID    string // client ID for raft session
	localAddr *kronospb.NodeAddr
	seedHosts []*kronospb.NodeAddr
	waldir    string            // path to WAL directory
	snapdir   string            // path to snapshot directory
	datadir   string            // path to data directory
	cluster   *metadata.Cluster // cluster metadata that is also persisted to a file

	getSnapshot func() ([]byte, error)
	// last index of the replay log at startup, this is used to determine when we
	// are done replaying the existing log entries.
	lastIndex uint64

	confState     sdraftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
	// snapTriggerConfig is used to check when to trigger a snapshot.
	snapTriggerConfig *snapTriggerConfig
}

// getNodesIncludingRemoved gets nodes in the cluster metadata from
// the remote node. It retries internally for 1 minute.
func (rc *raftNode) getNodesIncludingRemoved(
	ctx context.Context, remote *kronospb.NodeAddr,
) (nodes []kronoshttp.Node, err error) {
	log.Infof(ctx, "Getting nodes from %v", remote)
	err = retry.ForDuration(
		time.Minute,
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

// extraNodes returns 2 sets, one set containing the nodes present in set A, but
//  not in B. Other set is one present in set B, but not in A.
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
	if rc.transport.PeerExists(raftID) {
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
// from seedHosts to get the addresses for the ID not present in the it's
// cluster metadata. It also removes the nodeIDs present in the cluster metadata
// but not in the confState.
func (rc *raftNode) updateClusterFromConfState(ctx context.Context) {
	log.Infof(ctx, "Raft confstate: %v", rc.confState.Nodes)
	// remove nodes extra in activeNodes and add nodes extra in confstate.
	nodesToRemove, nodesToAdd := extraNodes(rc.cluster.ActiveNodes(), rc.confState.Nodes)

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

	// NB(kaavee): need to think if we need to change it to use addresses from
	// all the nodes in confstate, or just the seed hosts. What to
	// do if we get error? Should we retry later sometime?
	for _, seed := range rc.seedHosts {
		// nodes from nodesToAdd are deleted in this function later when we add a
		// node to cluster metadata. Check if all the required nodes have been added
		// initially to avoid asking nodes from the subsequent seedhosts if we get
		// all the required data from the previous seed hosts.
		if len(nodesToAdd) == 0 {
			break
		}
		if proto.Equal(rc.localAddr, seed) {
			continue
		}
		nodesIncludingRemoved, err := rc.getNodesIncludingRemoved(ctx, seed)
		if err != nil {
			log.Errorf(ctx, "Failed to get nodes from %v, error: %v", seed, err)
			continue
		}
		// Add the nodes not present in the local node's metadata with address
		// according to the metadata fetched from the seed host.
		for _, node := range nodesIncludingRemoved {
			if node.IsRemoved {
				rc.removeNode(node.NodeID)
			} else {
				if _, ok := nodesToAdd[node.NodeID]; !ok {
					continue
				}
				if err := rc.addNode(node.NodeID, node.RaftAddr); err != nil {
					log.Fatalf(ctx, "Failed to add node to cluster, error: %v", err)
				}
				// delete as the metadata has been added for this node.
				delete(nodesToAdd, node.NodeID)
			}
		}
	}

	if len(nodesToAdd) > 0 {
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

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// proposal channel proposeC. All log entries are replayed over the
// commit channel, followed by the replayedWALMsg message (to indicate the
// channel is current), then new log entries. To shutdown, close proposeC and
// read errorC.
func newRaftNode(
	rc *RaftConfig, getSnapshot func() ([]byte, error), proposeC <-chan string, nodeID string,
) (<-chan string, <-chan error, <-chan *snap.Snapshotter) {
	ctx := context.Background()
	seedHosts, err := convertToNodeAddrs(rc.SeedHosts)
	if err != nil {
		log.Fatalf(ctx, "Failed to convert seedHosts to NodeAddr, error: %v", err)
	}
	commitC := make(chan string)
	errorC := make(chan error)
	confChangeC := make(chan sdraftpb.ConfChange)
	if rc.SnapCount <= 0 {
		rc.SnapCount = numEntriesPerSnap
	}
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
		// rest of structure populated after WAL replay
	}
	go rn.startRaft(ctx, confChangeC, rc.CertsDir, rc.GRPCHostPort)
	return commitC, errorC, rn.snapshotterReady
}

func (rc *raftNode) purgeFiles(ctx context.Context) {
	snaperrc := fileutil.PurgeFile(rc.snapdir, snapFileSuffix, maxSnapFiles, fileGCInterval, rc.stopc)
	walerrc := fileutil.PurgeFile(rc.waldir, walFileSuffix, maxWALFiles, fileGCInterval, rc.stopc)
	select {
	case e := <-snaperrc:
		log.Fatalf(ctx, "Failed to purge snap files, err: %v", e)
	case e := <-walerrc:
		log.Fatalf(ctx, "Failed to purge wal files, err: %v", e)
	case <-rc.stopc:
		return
	}
}

// saveSnap saves the given snapshot
func (rc *raftNode) saveSnap(snap sdraftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := sdwalpb.Snapshot{
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
	ctx context.Context, committedEntries []sdraftpb.Entry,
) (nents []sdraftpb.Entry) {
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
func (rc *raftNode) publishEntries(ctx context.Context, ents []sdraftpb.Entry) bool {
	// This is the only function where rc.cluster is being modified while the
	// raft node is active. This is called synchronously by the caller. So there
	// is no need to provide thread-safety to cluster.
	for i := range ents {
		switch ents[i].Type {
		case sdraftpb.EntryNormal:
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

		case sdraftpb.EntryConfChange:
			var cc sdraftpb.ConfChange
			if err := protoutil.Unmarshal(ents[i].Data, &cc); err != nil {
				log.Fatal(ctx, err)
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case sdraftpb.ConfChangeAddNode:
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
			case sdraftpb.ConfChangeRemoveNode:
				log.Infof(ctx, "Processing remove node conf change %v", cc)
				nodeID := types.ID(cc.NodeID).String()
				rc.removeNode(nodeID)
				if err := rc.cluster.Persist(); err != nil {
					log.Errorf(
						ctx,
						"Failed to persist cluster metadata after remove node, error: %v",
						err,
					)
					return false
				}
				if nodeID == rc.nodeID {
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
func (rc *raftNode) loadSnapshot(ctx context.Context) *sdraftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf(ctx, "Error loading snapshot (%v)", err)
	}
	return snapshot
}

// readWal reads WAL entries starting at snap. It also repairs WAL
// ErrUnexpectedEOF errors in WAL files which can arise if there were past
// records that were only partially flushed (and therefore never synced.)
func (rc *raftNode) readWAL(
	ctx context.Context, snap *sdraftpb.Snapshot,
) (w *wal.WAL, st sdraftpb.HardState, ents []sdraftpb.Entry) {
	var err error
	if !wal.Exist(rc.waldir) {
		if err = os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf(ctx, "Cannot create dir for wal, err: %v", err)
		}
		w, err = wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf(ctx, "Create wal err: %v", err)
		}
		if err := w.Close(); err != nil {
			log.Fatalf(ctx, "Close wal err: %v", err)
		}
		log.Infof(ctx, "Created wal dir %s", rc.waldir)
	}

	walsnap := sdwalpb.Snapshot{}
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
		if w, err = wal.Open(rc.waldir, walsnap); err != nil {
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
			if !wal.Repair(rc.waldir) {
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

	log.Infof(ctx, "There are %d entries to be replayed", len(ents))
	if len(ents) > 0 {
		// if there are entries to be replayed, note the last index so that we can
		// tell when all past entries are published in the raft loop, and indicate
		// that to the client. replayedWALMsg is pushed to commitC by publishEntries
		// when all the entries found here are replayed.
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		// if there is nothing to replay, indicate to the client that replay is
		// already done. also explicitly initialize lastIndex to 0, so that it never
		// matches an entry index later and the replayedWALMsg is not sent again
		// (first entry we ever receive from raft has Index 1.)
		rc.lastIndex = 0
		rc.commitC <- replayedWALMsg
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

// startRaft replays the existing WAL and joins the raft cluster
func (rc *raftNode) startRaft(
	ctx context.Context,
	confChangeC chan<- sdraftpb.ConfChange,
	certsDir string,
	grpcAddr *kronospb.NodeAddr,
) {
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf(ctx, "Cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	rc.wal = rc.replayWAL(ctx)

	selfID := raftID(rc.nodeID)
	c := &raft.Config{
		ID:              uint64(selfID),
		ElectionTick:    15,
		HeartbeatTick:   5,
		PreVote:         true,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   16 * 1024,
		MaxInflightMsgs: 64,
	}

	lastIndex, err := c.Storage.LastIndex()
	if err != nil {
		log.Fatalf(ctx, "Failed to get lastIndex from store, err: %v", err)
	}
	// If the log is empty, this is a new node (StartNode); otherwise it's
	// restoring an existing node (RestartNode).
	isNodeInitialized := lastIndex != 0

	tlsInfo := kronosutil.TLSInfo(certsDir)
	isFirstSH := isFirstSeedHost(rc.seedHosts, rc.localAddr)

	if isNodeInitialized {
		// Cluster metadata will exist for initialized nodes
		rc.cluster, err = metadata.LoadCluster(rc.datadir, false /*readOnly*/)
		if err != nil {
			log.Fatalf(ctx, "Failed to create cluster, error: %v", err)
		}
		log.Infof(ctx, "Initialized cluster: %v", rc.cluster)
		// Raft cluster had already been initialized
		rc.node = raft.RestartNode(c)
	} else {
		if !isFirstSH {
			// Add all hosts to cluster formed by the first seedHost.
			if err := retry.ForDuration(
				time.Minute,
				func() error {
					// try on both the seedHosts to maintain one seedHost failure tolerance
					for _, seed := range rc.seedHosts {
						// The second seedHost tries AddNode on the first seed host only
						// during the start of the cluster.
						// This is based on the assumption that the first seedHost will not
						// fail when the cluster is being bootstrapped for the first time.
						// We don't guarantee seedHost failure tolerance for bootstrap which
						// is one time event in the lifetime of the cluster.
						if proto.Equal(seed, rc.localAddr) {
							continue
						}
						c, err := kronoshttp.NewClusterClient(seed, tlsInfo)
						if err != nil {
							log.Errorf(
								ctx,
								"Failed to create clusterClient for host %v, error: %v",
								seed,
								err,
							)
							continue
						}
						defer c.Close()
						request := &kronoshttp.AddNodeRequest{
							NodeID:  rc.nodeID,
							Address: kronosutil.NodeAddrToString(rc.localAddr),
						}
						ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, clusterRequestTimeout)
						defer cancelFunc()
						err = c.AddNode(ctxWithTimeout, request)
						if err != nil {
							log.Errorf(
								ctxWithTimeout,
								"Failed to addNode, request: %v, error: %v",
								request,
								err,
							)
							continue
						}
						// Add node request succeeded without errors.
						return nil
					}
					return errors.New("add node request failed on all the seed hosts")
				},
			); err != nil {
				log.Fatalf(ctx, "Failed to post add node request to %v, err: %v", rc.seedHosts[0], err)
			}
		}

		// Create an empty cluster. This will be populated by confChange entries/
		// while loading remote snapshots and getting metadata from other nodes.
		rc.cluster, err = metadata.NewCluster(rc.datadir, nil /* cluster */)
		if err != nil {
			log.Fatalf(ctx, "Error creating cluster metadata using data from seed host: %v", err)
		}

		var startingPeers []raft.Peer
		if isFirstSH {
			// only first seed host makes starts the initial cluster first time,
			// and thus only that has non-nil peers in start node.
			raftID := raftID(rc.nodeID)
			startingPeers = []raft.Peer{
				{ID: uint64(raftID), Context: []byte(kronosutil.NodeAddrToString(rc.localAddr))},
			}
		}
		var peerIds []string
		for _, p := range startingPeers {
			peerIds = append(peerIds, types.ID(p.ID).String())
		}
		log.Infof(ctx, "Starting peers: %v", peerIds)
		rc.node = raft.StartNode(c, startingPeers)
	}

	rc.transport = &rafthttp.Transport{
		ID:          selfID,
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(fmt.Sprintf("%v", rc.nodeID)),
		ErrorC:      make(chan error),
		DialTimeout: 5 * time.Second,
		TLSInfo:     tlsInfo,
	}
	if err := rc.transport.Start(); err != nil {
		log.Fatalf(ctx, "Failed to start raft transport, error: %v", err)
	}

	log.Infof(ctx, "Raft Address: %v, raft Id: %v", rc.localAddr, rc.nodeID)
	secure := !rc.transport.TLSInfo.Empty()

	// Add other nodes as seen by the cluster metadata as raft transport peers.
	for nodeID, nodeData := range rc.cluster.ActiveNodes() {
		if nodeID != rc.nodeID {
			raftID := raftID(nodeID)
			hostURL := kronosutil.AddrToURL(nodeData.RaftAddr, secure)
			// In case of an already initialized node, wal and thus some old
			// confChange entries might get replayed. Replay of old confChange
			// entries would add the node with addresses which could be stale due to
			// re-ip. AddPeer won't have any effect as it doesn't update the address
			// of a peer if it already existed. Also, UpdatePeer is noop if the peer
			// doesn't exist.
			// Therefore, AddPeer and then UpdatePeer to handle re_ip where the id
			// might already exist as peer with old address.
			rc.transport.AddPeer(raftID, []string{hostURL.String()})
			rc.transport.UpdatePeer(raftID, []string{hostURL.String()})
		}
	}

	// Add existing cluster as remote to make this node catch up with the raft
	// cluster.
	if !isFirstSH {
		var nodesIncludingRemoved []kronoshttp.Node
		var lastTryError error
		// try on both the seedHosts to maintain one seedHost failure tolerance
		for _, seed := range rc.seedHosts {
			if proto.Equal(seed, rc.localAddr) {
				continue
			}
			nodesIncludingRemoved, lastTryError = rc.getNodesIncludingRemoved(ctx, seed)
			if lastTryError != nil {
				log.Errorf(ctx, "Failed to get nodes from %v, error: %v", seed, lastTryError)
				continue
			}
			break
		}
		if lastTryError != nil {
			if !isNodeInitialized {
				log.Fatalf(ctx, "Failed to get nodes from %v, err: %v", rc.seedHosts, err)
			}
			// Initialized nodes have non-empty cluster metadata implying they would
			// have added those nodes as peers. Don't fatal over here as they are
			// somewhat connected to the cluster through peers.
			log.Errorf(ctx, "Failed to get nodes from %v, err: %v", rc.seedHosts, err)
		}

		// Add existing cluster as remote to make this node catch up with the raft
		// cluster.
		for _, node := range nodesIncludingRemoved {
			if node.IsRemoved {
				if rc.nodeID == node.NodeID {
					log.Fatalf(ctx, "This node (%v) has been removed from the cluster", rc.nodeID)
				} else {
					rc.cluster.RemoveNode(node.NodeID)
				}
				continue
			}
			// node still part of the cluster
			if node.NodeID != rc.nodeID {
				raftID := raftID(node.NodeID)
				hostURL := kronosutil.AddrToURL(node.RaftAddr, secure)
				// AddRemote is noop if the node exists as a peer.
				rc.transport.AddRemote(raftID, []string{hostURL.String()})
			}
		}
	}

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
func (rc *raftNode) publishSnapshot(ctx context.Context, snap sdraftpb.Snapshot) {
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
					// blocks until accepted by raft state machine
					if err := rc.node.Propose(context.TODO(), []byte(prop)); err != nil {
						log.Error(ctx, err)
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
			if err := rc.wal.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatal(ctx, err)
			}
			appliedIndexBeforePublishing := rc.appliedIndex
			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rc.saveSnap(rd.Snapshot); err != nil {
					log.Fatal(ctx, err)
				}

				if err := rc.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
					log.Fatal(ctx, err)
				}
				rc.publishSnapshot(ctx, rd.Snapshot)
				log.Infof(ctx, "Updating cluster metadata")
				rc.updateClusterFromConfState(ctx)
			}

			if err := rc.raftStorage.Append(rd.Entries); err != nil {
				log.Fatal(ctx, err)
			}

			rc.transport.Send(rd.Messages)
			ents := rc.entriesToApply(ctx, rd.CommittedEntries)
			if ok := rc.publishEntries(ctx, ents); !ok {
				rc.stop()
				return
			}
			if appliedIndexBeforePublishing < rc.lastIndex {
				// Only log if we are catching up, to prevent spam.
				log.Infof(ctx, "Published %d entries", len(ents))
			}
			// If the publish moved us ahead or at par to the state at startup, send
			// the special replayedWALMsg on commit channel to signal replay has
			// finished.
			if appliedIndexBeforePublishing < rc.lastIndex && rc.appliedIndex >= rc.lastIndex {
				rc.commitC <- replayedWALMsg
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
	ctx context.Context, confChangeC chan<- sdraftpb.ConfChange, grpcAddr *kronospb.NodeAddr,
) {
	// Listen on all interfaces
	host := net.JoinHostPort("", rc.localAddr.Port)
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
		kronoshttp.NewClusterHandler(confChangeC, rc.datadir, grpcAddr),
	)

	httpServer := &http.Server{Handler: handler}
	var tlsConfig *tls.Config
	if !rc.transport.TLSInfo.Empty() {
		tlsConfig, err = rc.transport.TLSInfo.ServerConfig()
		if err != nil {
			log.Fatalf(ctx, "Failed to get tls config: %v", err)
		}
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

// Process processes the given raft message
func (rc *raftNode) Process(ctx context.Context, m sdraftpb.Message) error {
	return rc.node.Step(ctx, m)
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
