package gossip

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rubrikinc/kronos/acceptance/testutil"
	leaktest "github.com/rubrikinc/kronos/crdbutils"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
	"google.golang.org/grpc"
)

func TestUpdateGossipState(t *testing.T) {
	t1 := time.Now().UnixNano()
	t2 := time.Now().Add(time.Second).UnixNano()
	tests := []struct {
		name          string
		upds          []map[GossipKey]*kronospb.Info
		expectedState map[GossipKey]*kronospb.Info
	}{
		{
			name: "Test new keys are getting added",
			upds: []map[GossipKey]*kronospb.Info{
				{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t1,
					},
					"hostPort-2": {
						Data:      []byte("hostPort-2"),
						Timestamp: t1,
					},
				},
			},
			expectedState: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      []byte("hostPort-1"),
					Timestamp: t1,
				},
				"hostPort-2": {
					Data:      []byte("hostPort-2"),
					Timestamp: t1,
				},
			},
		},
		{
			name: "Test existing keys are getting updated",
			upds: []map[GossipKey]*kronospb.Info{
				{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t1,
					},
				},
				{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t2,
					},
				},
			},
			expectedState: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      []byte("hostPort-1"),
					Timestamp: t2,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewServer("", nil, nil, "")
			for _, upd := range test.upds {
				g.updateGossipState(upd)
			}
			if len(g.data) != len(test.expectedState) {
				t.Errorf("Expected %d keys, got %d", len(test.expectedState), len(g.data))
			}
			for k, v := range test.expectedState {
				if _, ok := g.data[k]; !ok {
					t.Errorf("Expected key %s to be present in gossip state", k)
				}
				if g.data[k].Timestamp != v.Timestamp {
					t.Errorf("Expected timestamp %d, got %d", v.Timestamp, g.data[k].Timestamp)
				}
				if string(g.data[k].Data) != string(v.Data) {
					t.Errorf("Expected data %s, got %s", string(v.Data), string(g.data[k].Data))
				}
			}
		})
	}
}

func TestGossipCallBacks(t *testing.T) {
	nodeDesc1 := &kronospb.NodeDescriptor{
		NodeId:   "1",
		GrpcAddr: "127.0.0.1:3002",
		RaftAddr: "https://127.0.0.1:3002",
	}
	nodeDesc2 := &kronospb.NodeDescriptor{
		NodeId:   "2",
		GrpcAddr: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
		RaftAddr: "https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
	}
	nodeDesc3 := &kronospb.NodeDescriptor{
		NodeId:   "3",
		GrpcAddr: "[fe80::204:61ff:fe9d:f156]:3000",
		RaftAddr: "https://[fe80::204:61ff:fe9d:f156]:3000",
	}
	nodeDesc4 := &kronospb.NodeDescriptor{
		NodeId:   "4",
		GrpcAddr: "[fe80::%eth0]:80",
		RaftAddr: "https://[fe80::204:61ff:fe9d:f156]:3000",
	}
	nodeDesc5 := &kronospb.NodeDescriptor{
		NodeId:   "1",
		GrpcAddr: "127.0.0.1:3004",
		RaftAddr: "https://127.0.0.1:3004",
	}
	bytes1, _ := protoutil.Marshal(nodeDesc1)
	bytes2, _ := protoutil.Marshal(nodeDesc2)
	bytes3, _ := protoutil.Marshal(nodeDesc3)
	bytes4, _ := protoutil.Marshal(nodeDesc4)
	bytes5, _ := protoutil.Marshal(nodeDesc5)
	tests := []struct {
		name          string
		upds          map[GossipKey]*kronospb.Info
		expectedPeers []string
	}{
		{
			name: "Test peers are getting updated when a hostPort-* key is" +
				" received",
			upds: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      bytes1,
					Timestamp: time.Now().UnixNano(),
				},
				// ipv6 with port
				"hostPort-2": {
					Data:      bytes2,
					Timestamp: time.Now().UnixNano(),
				},
				// compressed ipv6 with port
				"hostPort-3": {
					Data:      bytes3,
					Timestamp: time.Now().UnixNano(),
				},
				// ipv6 link local with port
				"hostPort-4": {
					Data:      bytes4,
					Timestamp: time.Now().UnixNano(),
				},
			},
			expectedPeers: []string{
				"127.0.0.1:3002",
				"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080",
				"[fe80::204:61ff:fe9d:f156]:3000",
				"[fe80::%eth0]:80",
			},
		},
		{
			name: "Test peers are not getting updated when a non hostPort-" +
				"* key" +
				" is" +
				" received",
			upds: map[GossipKey]*kronospb.Info{
				"nodeee-1": {
					Data:      bytes1,
					Timestamp: time.Now().UnixNano(),
				},
			},
			expectedPeers: []string{},
		},
		{
			name: "Test peers are not getting updated when a hostPort-* key" +
				" is" +
				" received with invalid value",
			upds: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      []byte("hostPort-1,hostPort-1"),
					Timestamp: time.Now().UnixNano(),
				},
			},
			expectedPeers: []string{},
		},
		{
			name: "Test peers are getting updated when a hostPort-* key is" +
				"" + " received with invalid nodeID",
			upds: map[GossipKey]*kronospb.Info{
				"hostPort-5": {
					Data:      bytes5,
					Timestamp: time.Now().UnixNano(),
				},
			},
			expectedPeers: []string{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewServer("", nil, nil, "")
			g.updateGossipState(test.upds)
			peers := g.GetPeers()
			if len(peers) != len(test.expectedPeers) {
				t.Errorf("Expected %d peers, got %d", len(test.expectedPeers), len(peers))
			}
			for _, peer := range test.expectedPeers {
				if !contains(peers, peer) {
					t.Errorf("Expected peer %s to be present in peer list", peer)
				}
			}
		})
	}

}

func contains(peers []string, peer string) bool {
	for _, p := range peers {
		if p == peer {
			return true
		}
	}
	return false
}

// descInfo marshals a NodeDescriptor into a gossip Info with the given
// timestamp, for feeding through updateGossipState in tests.
func descInfo(t *testing.T, desc *kronospb.NodeDescriptor, ts int64) *kronospb.Info {
	t.Helper()
	b, err := protoutil.Marshal(desc)
	if err != nil {
		t.Fatalf("failed to marshal descriptor: %v", err)
	}
	return &kronospb.Info{Data: b, Timestamp: ts}
}

// TestRemovedDescriptorNotAddedAsPeer asserts that learning a descriptor with
// IsRemoved=true via gossip never adds the node to the peer set.
func TestRemovedDescriptorNotAddedAsPeer(t *testing.T) {
	g := NewServer("", nil, nil, "")
	desc := &kronospb.NodeDescriptor{
		NodeId:    "9",
		GrpcAddr:  "10.249.16.68:5767",
		RaftAddr:  "https://10.249.16.68:5767",
		IsRemoved: true,
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, desc, time.Now().UnixNano()),
	})
	if contains(g.GetPeers(), desc.GrpcAddr) {
		t.Errorf("removed descriptor %s was added to peer set", desc.GrpcAddr)
	}
}

// TestRemovePeerTombstones asserts RemovePeer turns a live node into a
// tombstone: peer dropped, descriptor retained with IsRemoved=true and a fresh
// timestamp, liveness key deleted.
func TestRemovePeerTombstones(t *testing.T) {
	g := NewServer("", nil, nil, "")
	desc := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	ts := time.Now().UnixNano()
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, desc, ts),
		LivenessPrefix.Encode("9"):       {Data: []byte("live"), Timestamp: ts},
	})
	if !contains(g.GetPeers(), desc.GrpcAddr) {
		t.Fatalf("precondition: peer %s should be present before removal", desc.GrpcAddr)
	}

	g.RemovePeer("9")

	if contains(g.GetPeers(), desc.GrpcAddr) {
		t.Errorf("peer %s still present after RemovePeer", desc.GrpcAddr)
	}
	got := g.GetNodeDesc("9")
	if got == nil {
		t.Fatalf("descriptor for node 9 was deleted; expected a tombstone")
	}
	if !got.IsRemoved {
		t.Errorf("descriptor for node 9 not marked IsRemoved after RemovePeer")
	}
	g.mu.Lock()
	descKey := NodeDescriptorPrefix.Encode("9")
	stored, ok := g.data[descKey]
	_, livenessOK := g.data[LivenessPrefix.Encode("9")]
	g.mu.Unlock()
	if !ok {
		t.Errorf("tombstone descriptor missing from g.data")
	} else if stored.Timestamp < ts {
		t.Errorf("tombstone timestamp %d not refreshed (was %d)", stored.Timestamp, ts)
	}
	if livenessOK {
		t.Errorf("liveness key for node 9 should be deleted after RemovePeer")
	}
}

// TestRemovePeerUnknownIDWritesTombstone covers the startup / rolling-upgrade
// case: a node removed before this binary was deployed is not yet in nodeList
// (its descriptor has not been re-learned from a peer), so RemovePeer must still
// write a tombstone from a minimal descriptor. Without it, no tombstone is
// published and the next gossip round resurrects the stale live descriptor.
func TestRemovePeerUnknownIDWritesTombstone(t *testing.T) {
	g := NewServer("", nil, nil, "")

	// "9" was never learned: nodeList is empty.
	if got := g.GetNodeDesc("9"); got != nil {
		t.Fatalf("precondition: node 9 should be unknown, got %v", got)
	}

	g.RemovePeer("9")

	got := g.GetNodeDesc("9")
	if got == nil {
		t.Fatalf("no tombstone written for unknown node 9")
	}
	if !got.IsRemoved {
		t.Errorf("descriptor for node 9 not marked IsRemoved")
	}
	g.mu.Lock()
	stored, ok := g.data[NodeDescriptorPrefix.Encode("9")]
	g.mu.Unlock()
	if !ok {
		t.Fatalf("tombstone for unknown node 9 not published into g.data")
	}

	// A later stale live descriptor (older timestamp) must not resurrect it.
	liveOld := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, liveOld, stored.Timestamp-1),
	})
	if contains(g.GetPeers(), liveOld.GrpcAddr) {
		t.Errorf("stale live descriptor resurrected removed peer %s", liveOld.GrpcAddr)
	}
}

// TestRemovedPeerStaysRemovedAfterRelearn is the regression test for the
// reported bug: once a node is tombstoned, re-learning its old live descriptor
// (with an older timestamp) must NOT re-add it, and a fresh server that learns
// a tombstone must never add the peer.
func TestRemovedPeerStaysRemovedAfterRelearn(t *testing.T) {
	g := NewServer("", nil, nil, "")
	desc := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	oldTs := time.Now().Add(-time.Hour).UnixNano()
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, desc, oldTs),
	})
	g.RemovePeer("9")
	if contains(g.GetPeers(), desc.GrpcAddr) {
		t.Fatalf("peer still present immediately after removal")
	}

	// Re-learn the OLD live descriptor (older timestamp than the tombstone).
	liveOld := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, liveOld, oldTs),
	})
	if contains(g.GetPeers(), desc.GrpcAddr) {
		t.Errorf("stale live descriptor resurrected removed peer %s", desc.GrpcAddr)
	}

	// Restart path: a fresh server that learns a tombstone must never add it.
	g2 := NewServer("", nil, nil, "")
	tombstone := &kronospb.NodeDescriptor{
		NodeId:    "9",
		GrpcAddr:  "10.249.16.68:5767",
		RaftAddr:  "https://10.249.16.68:5767",
		IsRemoved: true,
	}
	g2.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, tombstone, time.Now().UnixNano()),
	})
	if contains(g2.GetPeers(), tombstone.GrpcAddr) {
		t.Errorf("fresh server added tombstoned peer %s on restart path", tombstone.GrpcAddr)
	}

	// A re-added node (same id) with a NEWER live descriptor must be re-added.
	reAdded := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, reAdded, time.Now().UnixNano()),
	})
	if !contains(g.GetPeers(), reAdded.GrpcAddr) {
		t.Errorf("re-added node (newer live descriptor) was not restored to peer set")
	}
}

// TestRemovePeerNoRaceWithGetNodeList asserts RemovePeer does not mutate a
// descriptor pointer shared with readers. GetNodeList()/NodeLs()/GetNodeDesc()
// hand out the nodeList descriptor and callers read it after the lock is
// released; removeNodeLocked must clone before setting IsRemoved/LastHeartbeat.
// Run under -race: without the clone this reports a data race.
func TestRemovePeerNoRaceWithGetNodeList(t *testing.T) {
	g := NewServer("", nil, nil, "")
	desc := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, desc, time.Now().UnixNano()),
	})

	// Reader goroutine races against the removal: it reads the fields that
	// removeNodeLocked writes, on the pointer GetNodeList returns. RemovePeer
	// stays in the `ok` branch on every call (the tombstone remains in
	// nodeList), so it keeps rewriting LastHeartbeat — looping both sides makes
	// the read/write overlap reliable enough for -race to catch an in-place
	// mutation.
	stop := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, d := range g.GetNodeList() {
				_ = d.IsRemoved
				_ = d.LastHeartbeat
			}
		}
	}()

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < 2000; i++ {
			g.RemovePeer("9")
		}
	}()

	<-writerDone
	close(stop)
	<-readerDone
}

// TestRemovePeerNoDeadlock asserts RemovePeer terminates: it re-enters
// updateGossipStateLocked -> processCallbacksLocked -> addPeerLocked for the
// tombstone key, which must not recurse or deadlock on the non-reentrant mutex.
func TestRemovePeerNoDeadlock(t *testing.T) {
	g := NewServer("", nil, nil, "")
	desc := &kronospb.NodeDescriptor{
		NodeId:   "9",
		GrpcAddr: "10.249.16.68:5767",
		RaftAddr: "https://10.249.16.68:5767",
	}
	g.updateGossipState(map[GossipKey]*kronospb.Info{
		NodeDescriptorPrefix.Encode("9"): descInfo(t, desc, time.Now().UnixNano()),
	})
	done := make(chan struct{})
	go func() {
		g.RemovePeer("9")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RemovePeer did not return within 5s; likely deadlock or infinite loop")
	}
}

func TestGossip(t *testing.T) {
	t1 := time.Now().UnixNano()
	t2 := time.Now().Add(time.Second).UnixNano()
	t3 := time.Now().Add(2 * time.Second).UnixNano()
	tests := []struct {
		name     string
		data     map[GossipKey]*kronospb.Info
		request  *kronospb.Request
		response *kronospb.Response
		err      error
	}{
		{
			name: "Test gossip returns error when cluster id mismatch",
			request: &kronospb.Request{
				ClusterId: "cluster-2",
			},
			response: nil,
			err:      errors.Errorf("cluster id mismatch: %s != %s", "cluster-2", "cluster-1"),
		},
		{
			name: "Test gossip returns response when cluster id matches",
			data: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      []byte("hostPort-1"),
					Timestamp: t1,
				},
				"hostPort-2": {
					Data:      []byte("hostPort-2"),
					Timestamp: t2,
				},
			},
			request: &kronospb.Request{
				ClusterId: "cluster-1",
			},
			response: &kronospb.Response{
				NodeId: "hostPort-1",
				Data: map[string]*kronospb.Info{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t1,
					},
					"hostPort-2": {
						Data:      []byte("hostPort-2"),
						Timestamp: t2,
					},
				},
			},
		},
		{
			name: "Test gossip returns response with updated data",
			data: map[GossipKey]*kronospb.Info{
				"hostPort-1": {
					Data:      []byte("hostPort-1"),
					Timestamp: t1,
				},
				"hostPort-2": {
					Data:      []byte("hostPort-2"),
					Timestamp: t2,
				},
			},
			request: &kronospb.Request{
				ClusterId: "cluster-1",
				GossipMap: map[string]*kronospb.Info{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t3,
					},
				},
			},
			response: &kronospb.Response{
				NodeId: "hostPort-1",
				Data: map[string]*kronospb.Info{
					"hostPort-1": {
						Data:      []byte("hostPort-1"),
						Timestamp: t3,
					},
					"hostPort-2": {
						Data:      []byte("hostPort-2"),
						Timestamp: t2,
					},
				},
			},
		},
	}
	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := NewServer("", nil, nil, "")
			g.SetClusterID(ctx, "cluster-1")
			g.SetNodeID(ctx, "hostPort-1")
			g.updateGossipState(test.data)
			response, err := g.Gossip(ctx, test.request)
			if err != nil {
				if err.Error() != test.err.Error() {
					t.Errorf("Expected error %v, got %v", test.err, err)
				}
			} else {
				if response.NodeId != test.response.NodeId {
					t.Errorf("Expected node id %s, got %s", test.response.NodeId, response.NodeId)
				}
				if len(response.Data) != len(test.response.Data) {
					t.Errorf("Expected %d keys, got %d", len(test.response.Data), len(response.Data))
				}
				for k, v := range test.response.Data {
					if _, ok := response.Data[k]; !ok {
						t.Errorf("Expected key %s to be present in gossip state", k)
					}
					if response.Data[k].Timestamp != v.Timestamp {
						t.Errorf("Expected timestamp %d, got %d", v.Timestamp, response.Data[k].Timestamp)
					}
					if string(response.Data[k].Data) != string(v.Data) {
						t.Errorf("Expected data %s, got %s", string(v.Data), string(response.Data[k].Data))
					}
				}
			}
		})
	}
}

func TestGossipPropagation(t *testing.T) {
	// Check for any lingering goroutines once the test ends.
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	freePorts, err := testutil.GetFreePorts(ctx, 4)
	if err != nil {
		t.Fatalf("Failed to get free ports, err: %v", err)
	}
	hosts := make([]string, 4)
	for i := 0; i < 4; i++ {
		hosts[i] = "localhost:" + strconv.Itoa(freePorts[i])
	}
	gossipPeriod = time.Millisecond
	// Start 4 gossip servers
	stopCh := make([]chan struct{}, 4)
	gossipServers := make([]*Server, 4)
	grpcServers := make([]*grpc.Server, 4)
	var serving sync.WaitGroup
	for i := 0; i < 4; i++ {
		gossipServers[i] = NewServer(hosts[i],
			&kronospb.NodeAddr{Host: "localhost",
				Port: strconv.Itoa(freePorts[i])},
			hosts[:2], "")
		lis, err := net.Listen(
			"tcp",
			net.JoinHostPort("localhost", strconv.Itoa(freePorts[i])),
		)
		if err != nil {
			t.Fatalf("Failed to listen: %v", err)
		}
		server := grpc.NewServer()
		kronospb.RegisterGossipServer(server, gossipServers[i])
		serving.Add(1)
		go func() {
			defer serving.Done()
			if err := server.Serve(lis); err != nil {
				t.Errorf("Failed to serve: %v", err)
			}
		}()
		grpcServers[i] = server
		gossipServers[i].SetClusterID(ctx, "cluster-1")
		gossipServers[i].SetNodeID(ctx, "node-"+strconv.Itoa(i))
		stopCh[i] = make(chan struct{})
		go gossipServers[i].Start(ctx, stopCh[i])
	}
	timeStamps := []int64{
		time.Now().UnixNano(),
		time.Now().Add(time.Second).UnixNano(),
		time.Now().Add(2 * time.Second).UnixNano(),
		time.Now().Add(3 * time.Second).UnixNano(),
	}
	finalGossipState := map[GossipKey]*kronospb.Info{
		"key-0": {
			Data:      []byte("key-0"),
			Timestamp: timeStamps[0],
		},
		"key-1": {
			Data:      []byte("key-1"),
			Timestamp: timeStamps[1],
		},
		"key-2": {
			Data:      []byte("key-2"),
			Timestamp: timeStamps[2],
		},
		"key-3": {
			Data:      []byte("key-3"),
			Timestamp: timeStamps[3],
		},
	}
	for i := 0; i < 4; i++ {
		gossipServers[i].updateGossipState(map[GossipKey]*kronospb.Info{
			GossipKey(fmt.Sprintf("key-%v", i)): {
				Data:      []byte(fmt.Sprintf("key-%v", i)),
				Timestamp: timeStamps[i],
			},
		})
	}
	// Wait for gossip propagation
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 4; i++ {
		if len(gossipServers[i].data) != len(finalGossipState) {
			t.Errorf("Expected %d keys, got %d", len(finalGossipState), len(gossipServers[i].data))
		}
		for k, v := range finalGossipState {
			if _, ok := gossipServers[i].data[k]; !ok {
				t.Errorf("Expected key %s to be present in gossip state", k)
			}
			if gossipServers[i].data[k].Timestamp != v.Timestamp {
				t.Errorf("Expected timestamp %d, got %d", v.Timestamp, gossipServers[i].data[k].Timestamp)
			}
			if string(gossipServers[i].data[k].Data) != string(v.Data) {
				t.Errorf("Expected data %s, got %s", string(v.Data), string(gossipServers[i].data[k].Data))
			}
		}
	}

	for i := 0; i < 4; i++ {
		close(stopCh[i])
		grpcServers[i].Stop()
	}
	// Wait for every Serve to return: t must not be used after the test
	// ends.
	serving.Wait()
}
