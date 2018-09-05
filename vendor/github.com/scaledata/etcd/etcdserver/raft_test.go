// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdserver

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/scaledata/etcd/etcdserver/membership"
	"github.com/scaledata/etcd/pkg/mock/mockstorage"
	"github.com/scaledata/etcd/pkg/pbutil"
	"github.com/scaledata/etcd/pkg/types"
	"github.com/scaledata/etcd/raft"
	"github.com/scaledata/etcd/raft/sdraftpb"
	"github.com/scaledata/etcd/rafthttp"
)

func TestGetIDs(t *testing.T) {
	addcc := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeAddNode, NodeID: 2}
	addEntry := sdraftpb.Entry{Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc)}
	removecc := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeRemoveNode, NodeID: 2}
	removeEntry := sdraftpb.Entry{Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc)}
	normalEntry := sdraftpb.Entry{Type: sdraftpb.EntryNormal}
	updatecc := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeUpdateNode, NodeID: 2}
	updateEntry := sdraftpb.Entry{Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(updatecc)}

	tests := []struct {
		confState *sdraftpb.ConfState
		ents      []sdraftpb.Entry

		widSet []uint64
	}{
		{nil, []sdraftpb.Entry{}, []uint64{}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{}, []uint64{1}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{addEntry}, []uint64{1, 2}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{addEntry, removeEntry}, []uint64{1}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{addEntry, normalEntry}, []uint64{1, 2}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{addEntry, normalEntry, updateEntry}, []uint64{1, 2}},
		{&sdraftpb.ConfState{Nodes: []uint64{1}},
			[]sdraftpb.Entry{addEntry, removeEntry, normalEntry}, []uint64{1}},
	}

	for i, tt := range tests {
		var snap sdraftpb.Snapshot
		if tt.confState != nil {
			snap.Metadata.ConfState = *tt.confState
		}
		idSet := getIDs(&snap, tt.ents)
		if !reflect.DeepEqual(idSet, tt.widSet) {
			t.Errorf("#%d: idset = %#v, want %#v", i, idSet, tt.widSet)
		}
	}
}

func TestCreateConfigChangeEnts(t *testing.T) {
	m := membership.Member{
		ID:             types.ID(1),
		RaftAttributes: membership.RaftAttributes{PeerURLs: []string{"http://localhost:2380"}},
	}
	ctx, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	addcc1 := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeAddNode, NodeID: 1, Context: ctx}
	removecc2 := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeRemoveNode, NodeID: 2}
	removecc3 := &sdraftpb.ConfChange{Type: sdraftpb.ConfChangeRemoveNode, NodeID: 3}
	tests := []struct {
		ids         []uint64
		self        uint64
		term, index uint64

		wents []sdraftpb.Entry
	}{
		{
			[]uint64{1},
			1,
			1, 1,

			[]sdraftpb.Entry{},
		},
		{
			[]uint64{1, 2},
			1,
			1, 1,

			[]sdraftpb.Entry{{Term: 1, Index: 2, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2},
			1,
			2, 2,

			[]sdraftpb.Entry{{Term: 2, Index: 3, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)}},
		},
		{
			[]uint64{1, 2, 3},
			1,
			2, 2,

			[]sdraftpb.Entry{
				{Term: 2, Index: 3, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 4, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			2,
			2, 2,

			[]sdraftpb.Entry{
				{Term: 2, Index: 3, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
			},
		},
		{
			[]uint64{2, 3},
			1,
			2, 2,

			[]sdraftpb.Entry{
				{Term: 2, Index: 3, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc2)},
				{Term: 2, Index: 4, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(removecc3)},
				{Term: 2, Index: 5, Type: sdraftpb.EntryConfChange, Data: pbutil.MustMarshal(addcc1)},
			},
		},
	}

	for i, tt := range tests {
		gents := createConfigChangeEnts(tt.ids, tt.self, tt.term, tt.index)
		if !reflect.DeepEqual(gents, tt.wents) {
			t.Errorf("#%d: ents = %v, want %v", i, gents, tt.wents)
		}
	}
}

func TestStopRaftWhenWaitingForApplyDone(t *testing.T) {
	n := newNopReadyNode()
	r := newRaftNode(raftNodeConfig{
		Node:        n,
		storage:     mockstorage.NewStorageRecorder(""),
		raftStorage: raft.NewMemoryStorage(),
		transport:   rafthttp.NewNopTransporter(),
	})
	srv := &EtcdServer{r: *r}
	srv.r.start(nil)
	n.readyc <- raft.Ready{}
	select {
	case <-srv.r.applyc:
	case <-time.After(time.Second):
		t.Fatalf("failed to receive apply struct")
	}

	srv.r.stopped <- struct{}{}
	select {
	case <-srv.r.done:
	case <-time.After(time.Second):
		t.Fatalf("failed to stop raft loop")
	}
}

// TestConfgChangeBlocksApply ensures apply blocks if committed entries contain config-change.
func TestConfgChangeBlocksApply(t *testing.T) {
	n := newNopReadyNode()

	r := newRaftNode(raftNodeConfig{
		Node:        n,
		storage:     mockstorage.NewStorageRecorder(""),
		raftStorage: raft.NewMemoryStorage(),
		transport:   rafthttp.NewNopTransporter(),
	})
	srv := &EtcdServer{r: *r}

	srv.r.start(&raftReadyHandler{updateLeadership: func(bool) {}})
	defer srv.r.Stop()

	n.readyc <- raft.Ready{
		SoftState:        &raft.SoftState{RaftState: raft.StateFollower},
		CommittedEntries: []sdraftpb.Entry{{Type: sdraftpb.EntryConfChange}},
	}
	ap := <-srv.r.applyc

	continueC := make(chan struct{})
	go func() {
		n.readyc <- raft.Ready{}
		<-srv.r.applyc
		close(continueC)
	}()

	select {
	case <-continueC:
		t.Fatalf("unexpected execution: raft routine should block waiting for apply")
	case <-time.After(time.Second):
	}

	// finish apply, unblock raft routine
	<-ap.notifyc

	select {
	case <-continueC:
	case <-time.After(time.Second):
		t.Fatalf("unexpected blocking on execution")
	}
}
