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

package rafthttp

import (
	"testing"

	"github.com/scaledata/etcd/raft/sdraftpb"
)

func TestPeerPick(t *testing.T) {
	tests := []struct {
		msgappWorking  bool
		messageWorking bool
		m              sdraftpb.Message
		wpicked        string
	}{
		{
			true, true,
			sdraftpb.Message{Type: sdraftpb.MsgSnap},
			pipelineMsg,
		},
		{
			true, true,
			sdraftpb.Message{Type: sdraftpb.MsgApp, Term: 1, LogTerm: 1},
			streamAppV2,
		},
		{
			true, true,
			sdraftpb.Message{Type: sdraftpb.MsgProp},
			streamMsg,
		},
		{
			true, true,
			sdraftpb.Message{Type: sdraftpb.MsgHeartbeat},
			streamMsg,
		},
		{
			false, true,
			sdraftpb.Message{Type: sdraftpb.MsgApp, Term: 1, LogTerm: 1},
			streamMsg,
		},
		{
			false, false,
			sdraftpb.Message{Type: sdraftpb.MsgApp, Term: 1, LogTerm: 1},
			pipelineMsg,
		},
		{
			false, false,
			sdraftpb.Message{Type: sdraftpb.MsgProp},
			pipelineMsg,
		},
		{
			false, false,
			sdraftpb.Message{Type: sdraftpb.MsgSnap},
			pipelineMsg,
		},
		{
			false, false,
			sdraftpb.Message{Type: sdraftpb.MsgHeartbeat},
			pipelineMsg,
		},
	}
	for i, tt := range tests {
		peer := &peer{
			msgAppV2Writer: &streamWriter{working: tt.msgappWorking},
			writer:         &streamWriter{working: tt.messageWorking},
			pipeline:       &pipeline{},
		}
		_, picked := peer.pick(tt.m)
		if picked != tt.wpicked {
			t.Errorf("#%d: picked = %v, want %v", i, picked, tt.wpicked)
		}
	}
}
