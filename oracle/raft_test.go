package oracle

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/scaledata/etcd/raft"
	"github.com/scaledata/etcd/raft/sdraftpb"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
)

func TestWithProtocol(t *testing.T) {
	a := assert.New(t)
	a.Equal(withProtocol("127.0.0.1:1", true /*secure*/), "https://127.0.0.1:1")
	a.Equal(withProtocol("127.0.0.1:1", false /*secure*/), "http://127.0.0.1:1")
}

func TestIsFirstSeedHost(t *testing.T) {
	a := assert.New(t)
	a.True(
		isFirstSeedHost(
			[]*kronospb.NodeAddr{
				{Host: "127.0.0.1", Port: "1"},
				{Host: "127.0.0.1", Port: "2"},
			},
			&kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"},
		),
	)
	a.False(
		isFirstSeedHost(
			[]*kronospb.NodeAddr{
				{Host: "127.0.0.1", Port: "1"},
			},
			&kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"},
		),
	)
}

func newTestRaftNode(dataDir string) (*raftNode, error) {
	cluster, err := metadata.NewCluster(dataDir, metadata.NewClusterProto())
	if err != nil {
		return nil, err
	}
	node := &raftNode{}
	node.cluster = cluster
	return node, nil
}

func existingTestRaftNode(dataDir string, readOnly bool) (*raftNode, error) {
	cluster, err := metadata.LoadCluster(dataDir, readOnly)
	if err != nil {
		return nil, err
	}
	node := &raftNode{}
	node.cluster = cluster
	return node, nil
}

func TestIsIDRemoved(t *testing.T) {
	a := assert.New(t)
	dataDir, err := ioutil.TempDir("", "data_dir")
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()
	a.NoError(err)

	host1 := &kronospb.NodeAddr{
		Host: "123",
		Port: "123",
	}

	host2 := &kronospb.NodeAddr{
		Host: "124",
		Port: "124",
	}

	host3 := &kronospb.NodeAddr{
		Host: "125",
		Port: "125",
	}

	host4 := &kronospb.NodeAddr{
		Host: "126",
		Port: "126",
	}

	node, err := newTestRaftNode(dataDir)
	a.NoError(err)

	a.NoError(node.cluster.AddNode("1", host1))
	a.NoError(node.cluster.AddNode("2", host2))
	a.NoError(node.cluster.AddNode("3", host3))
	a.NoError(node.cluster.Persist())

	a.Equal(3, len(node.cluster.ActiveNodes()))
	a.Equal(false, node.IsIDRemoved(1))
	a.Equal(false, node.IsIDRemoved(2))
	a.Equal(false, node.IsIDRemoved(3))

	node.cluster.RemoveNode("1")
	a.Equal(true, node.IsIDRemoved(1))
	a.Equal(false, node.IsIDRemoved(2))
	a.Equal(false, node.IsIDRemoved(3))

	node.cluster.RemoveNode("2")
	a.Equal(true, node.IsIDRemoved(1))
	a.Equal(true, node.IsIDRemoved(2))
	a.Equal(false, node.IsIDRemoved(3))
	a.NoError(node.cluster.Persist())

	// node2 is not allowed to persist data
	node2, err := existingTestRaftNode(dataDir, true /* readOnly */)
	a.NoError(err)
	a.Equal(true, node2.IsIDRemoved(1))
	a.Equal(true, node2.IsIDRemoved(2))
	a.Equal(false, node2.IsIDRemoved(3))
	// Node 2 should not be allowed to persist data since it uses a read only
	// cluster
	node2.cluster.RemoveNode("2")
	err = node2.cluster.Persist()
	if a.Error(err) {
		a.Equal("cluster opened in readOnly mode", err.Error())
	}
	a.NoError(node2.cluster.AddNode("9", host4))
	err = node2.cluster.Persist()
	if a.Error(err) {
		a.Equal("cluster opened in readOnly mode", err.Error())
	}

	node.cluster.RemoveNode("3")
	a.Equal(true, node.IsIDRemoved(1))
	a.Equal(true, node.IsIDRemoved(2))
	a.Equal(true, node.IsIDRemoved(3))
}

func TestExtraNodes(t *testing.T) {
	testCases := []struct {
		name             string
		setA             map[string]*kronospb.Node
		setB             []uint64
		expectedExtraInA map[string]struct{}
		expectedExtraInB map[string]struct{}
	}{
		{
			name:             "extra in A",
			setA:             map[string]*kronospb.Node{"1": nil, "2": nil},
			setB:             []uint64{1},
			expectedExtraInA: map[string]struct{}{"2": {}},
			expectedExtraInB: map[string]struct{}{},
		},
		{
			name:             "empty B",
			setA:             map[string]*kronospb.Node{"1": nil, "2": nil},
			setB:             []uint64{},
			expectedExtraInA: map[string]struct{}{"1": {}, "2": {}},
			expectedExtraInB: map[string]struct{}{},
		},
		{
			name:             "extra in B",
			setA:             map[string]*kronospb.Node{"1": nil},
			setB:             []uint64{1, 2},
			expectedExtraInA: map[string]struct{}{},
			expectedExtraInB: map[string]struct{}{"2": {}},
		},
		{
			name:             "empty A",
			setA:             map[string]*kronospb.Node{},
			setB:             []uint64{1, 2},
			expectedExtraInA: map[string]struct{}{},
			expectedExtraInB: map[string]struct{}{"1": {}, "2": {}},
		},
		{
			name:             "extra in both",
			setA:             map[string]*kronospb.Node{"1": nil, "3": nil},
			setB:             []uint64{1, 2},
			expectedExtraInA: map[string]struct{}{"3": {}},
			expectedExtraInB: map[string]struct{}{"2": {}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			extraInA, extraInB := extraNodes(tc.setA, tc.setB)
			a.Equal(tc.expectedExtraInA, extraInA)
			a.Equal(tc.expectedExtraInB, extraInB)
		})
	}
}

func TestSnapTriggerConfig(t *testing.T) {
	testCases := []struct {
		name                string
		appliedIndex        uint64
		snapshotIndex       uint64
		snapTriggerConfig   *snapTriggerConfig
		expectedTriggerSnap bool
	}{
		{
			name:                "new config",
			appliedIndex:        2,
			snapshotIndex:       1,
			snapTriggerConfig:   newSnapTriggerConfig(uint64(100)),
			expectedTriggerSnap: false,
		},
		{
			name:          "should trigger due to confChange",
			appliedIndex:  2,
			snapshotIndex: 1,
			snapTriggerConfig: &snapTriggerConfig{
				lastSnapTime:            time.Now().Add(-1 * time.Hour),
				confChangeSinceLastSnap: true,
				snapCount:               100,
			},
			expectedTriggerSnap: true,
		},
		{
			name:          "no confChange entries",
			appliedIndex:  2,
			snapshotIndex: 1,
			snapTriggerConfig: &snapTriggerConfig{
				lastSnapTime:            time.Now().Add(-1 * time.Hour),
				confChangeSinceLastSnap: false,
				snapCount:               100,
			},
			expectedTriggerSnap: false,
		},
		{
			name:          "snapshot taken recently",
			appliedIndex:  2,
			snapshotIndex: 1,
			snapTriggerConfig: &snapTriggerConfig{
				lastSnapTime:            time.Now().Add(-1 * time.Second),
				confChangeSinceLastSnap: true,
				snapCount:               100,
			},
			expectedTriggerSnap: false,
		},
		{
			name:          "wal > snapCount",
			appliedIndex:  200,
			snapshotIndex: 1,
			snapTriggerConfig: &snapTriggerConfig{
				lastSnapTime:            time.Now().Add(-1 * time.Second),
				confChangeSinceLastSnap: false,
				snapCount:               100,
			},
			expectedTriggerSnap: true,
		},
		{
			name:          "wal < snapCount",
			appliedIndex:  10000,
			snapshotIndex: 9999,
			snapTriggerConfig: &snapTriggerConfig{
				lastSnapTime:            time.Now().Add(-1 * time.Second),
				confChangeSinceLastSnap: false,
				snapCount:               100,
			},
			expectedTriggerSnap: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(
				t,
				tc.expectedTriggerSnap,
				tc.snapTriggerConfig.shouldTrigger(tc.appliedIndex, tc.snapshotIndex),
			)
		})
	}
}

func TestPublishEntries(t *testing.T) {
	ctx := context.TODO()
	confChangeC := make(chan sdraftpb.ConfChange)
	rn := raftNode{
		confChangeC:       confChangeC,
		snapTriggerConfig: newSnapTriggerConfig(100),
	}
	rn.raftStorage = raft.NewMemoryStorage()
	c := &raft.Config{
		ID:              1,
		ElectionTick:    15,
		HeartbeatTick:   5,
		PreVote:         true,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   16 * 1024,
		MaxInflightMsgs: 64,
	}
	rn.node = raft.StartNode(c, []raft.Peer{})
	// Starting node -> no confChange entries -> confChangeSinceLastSnap = false
	assert.Equal(t, rn.snapTriggerConfig.confChangeSinceLastSnap, false)
	// normal entry should not update the confChangeSinceLastSnap
	rn.publishEntries(ctx, []sdraftpb.Entry{{Type: sdraftpb.EntryNormal}})
	assert.Equal(t, rn.snapTriggerConfig.confChangeSinceLastSnap, false)
	// confChange entry should update the confChangeSinceLastSnap
	rn.publishEntries(ctx, []sdraftpb.Entry{{Type: sdraftpb.EntryConfChange}})
	assert.Equal(t, rn.snapTriggerConfig.confChangeSinceLastSnap, true)
}
