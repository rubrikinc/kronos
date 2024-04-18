package kronoshttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
)

func TestClusterHandlerServeHTTP(t *testing.T) {

	testCases := []struct {
		name string
		// requestGenerator generates a http request to be fed to the cluster handler.
		requestGenerator func(t *testing.T) *http.Request
		// clusterBeforeConfChange is the cluster metadata persisted in dataDirectory
		// on the server side before confChange has been run.
		clusterBeforeConfChange *kronospb.Cluster
		// clusterAfterConfChange is the cluster metadata persisted in dataDirectory
		// on the server side after confChange has been run.
		clusterAfterConfChange *kronospb.Cluster
		// requestType eg.: requestTypeAdd, requestTypeRemove
		requestType string
		// confChange entry that we expect to receive on confChangeC.
		expectedConfChange *raftpb.ConfChange
		// expected http status code.
		expectedCode int
		// expectedRespGenerator generates the expected http response from the
		// cluster handler.
		expectedRespGenerator func(t *testing.T) []byte
	}{
		{
			name: "get grpc address",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				req, err := http.NewRequest(http.MethodGet, "", nil)
				a.NoError(err)
				req.RequestURI = "/cluster/grpc_addr"
				return req
			},
			requestType:  requestTypeGRPCAddr,
			expectedCode: http.StatusOK,
			expectedRespGenerator: func(*testing.T) []byte {
				return []byte("localhost:5767")
			},
		},
		{
			name: "get nodes",
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				req, err := http.NewRequest(http.MethodGet, "", nil)
				a.NoError(err)
				req.RequestURI = "/cluster/nodes"
				return req
			},
			requestType:  requestTypeNodes,
			expectedCode: http.StatusOK,
			expectedRespGenerator: func(t *testing.T) []byte {
				a := assert.New(t)
				nodes := []Node{
					{
						NodeID: "1",
						Node: &kronospb.Node{
							RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"},
						},
					},
					{
						NodeID: "2",
						Node: &kronospb.Node{
							RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"},
						},
					},
				}
				resp, err := json.Marshal(nodes)
				a.NoError(err)
				return resp
			},
		},
		{
			name: "cluster metadata contains added node",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				requestJSON, err := json.Marshal(&AddNodeRequest{NodeID: "1", Address: "127.0.0.1:1"})
				a.NoError(err)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader(requestJSON),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/add"
				return req
			},
			requestType: requestTypeAdd,
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{},
			},
			clusterAfterConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			expectedConfChange: &raftpb.ConfChange{
				NodeID:  1,
				Type:    raftpb.ConfChangeAddNode,
				Context: []byte("127.0.0.1:1"),
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "cluster metadata doesn't contain added node",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				requestJSON, err := json.Marshal(&AddNodeRequest{NodeID: "1", Address: "127.0.0.1:1"})
				a.NoError(err)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader(requestJSON),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/add"
				return req
			},
			requestType: requestTypeAdd,
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
				},
			},
			clusterAfterConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
				},
			},
			expectedConfChange: &raftpb.ConfChange{
				NodeID:  1,
				Type:    raftpb.ConfChangeAddNode,
				Context: []byte("127.0.0.1:1"),
			},
			// expect StatusRequestTimeout, as the node being added is not in the
			// persisted cluster on the server.
			expectedCode: http.StatusRequestTimeout,
		},
		{
			name: "bad request",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader([]byte("wrongJson")),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/add"
				return req
			},
			requestType:  requestTypeAdd,
			expectedCode: http.StatusBadRequest,
		},
		{
			name: "cluster metadata doesn't contains removed node",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				requestJSON, err := json.Marshal(&RemoveNodeRequest{NodeID: "1"})
				a.NoError(err)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader(requestJSON),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/remove"
				return req
			},
			requestType: requestTypeRemove,
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			clusterAfterConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
				},
			},
			expectedConfChange: &raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "cluster metadata contains removed node as removed",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				requestJSON, err := json.Marshal(&RemoveNodeRequest{NodeID: "1"})
				a.NoError(err)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader(requestJSON),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/remove"
				return req
			},
			requestType: requestTypeRemove,
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			clusterAfterConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"2": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "2"}},
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}, IsRemoved: true},
				},
			},
			expectedConfChange: &raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "cluster metadata contains removed node",
			requestGenerator: func(t *testing.T) *http.Request {
				a := assert.New(t)
				requestJSON, err := json.Marshal(&RemoveNodeRequest{NodeID: "1"})
				a.NoError(err)
				req, err := http.NewRequest(
					http.MethodPost,
					"",
					bytes.NewReader(requestJSON),
				)
				a.NoError(err)
				req.RequestURI = "/cluster/remove"
				return req
			},
			requestType: requestTypeRemove,
			clusterBeforeConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			clusterAfterConfChange: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": {RaftAddr: &kronospb.NodeAddr{Host: "127.0.0.1", Port: "1"}},
				},
			},
			expectedConfChange: &raftpb.ConfChange{
				Type:   raftpb.ConfChangeRemoveNode,
				NodeID: 1,
			},
			// expect StatusRequestTimeout, as the node being removed is in the
			// persisted cluster on the server.
			expectedCode: http.StatusRequestTimeout,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			confChangeC := make(chan raftpb.ConfChange, 10)
			done := make(chan struct{})
			dataDir, err := ioutil.TempDir("", "data_dir")
			a.NoError(err)
			metadata.FetchOrAssignClusterUUID(context.Background(), dataDir,
				false)
			defer func() { _ = os.RemoveAll(dataDir) }()
			if tc.clusterBeforeConfChange != nil {
				a.NoError(updateCluster(dataDir, tc.clusterBeforeConfChange))
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				ccCount := 0
				for {
					select {
					// Keep on reading the confChanges to let the handler retry multiple
					// times
					case cc, ok := <-confChangeC:
						if !ok {
							return
						}
						ccCount++
						a.True(proto.Equal(tc.expectedConfChange, &cc))
						// Persist cluster metadata in the data directory after we receive
						// a confChange.
						if tc.clusterAfterConfChange != nil {
							a.NoError(updateCluster(dataDir, tc.clusterAfterConfChange))
						}
					case <-done:
						if tc.expectedConfChange != nil {
							a.True(ccCount > 0)
						} else {
							a.Zero(ccCount)
						}
						return
					}
				}
			}()
			a.NoError(err)
			ch := ClusterHandler{
				confChangeC:            confChangeC,
				dataDir:                dataDir,
				confChangeTotalTimeout: 10 * time.Millisecond,
				grpcAddr:               &kronospb.NodeAddr{Host: "localhost", Port: "5767"},
			}

			req := tc.requestGenerator(t)
			rr := httptest.NewRecorder()
			ch.ServeHTTP(rr, req)
			close(confChangeC)
			close(done)
			wg.Wait()
			a.Equal(tc.expectedCode, rr.Code)
			if tc.expectedCode != http.StatusOK || tc.requestType != requestTypeNodes {
				// no need to check the response if response status is not OK
				// or requestType is not requestTypeNode
				return
			}
			respJSON, err := ioutil.ReadAll(rr.Body)
			a.NoError(err)
			a.Equal(tc.expectedRespGenerator(t), respJSON)
		})
	}
}

func updateCluster(dataDir string, cluster *kronospb.Cluster) error {
	c, err := metadata.NewCluster(dataDir, cluster)
	if err != nil {
		return err
	}
	defer func() {
		_ = c.Close()
	}()
	return c.Persist()
}
