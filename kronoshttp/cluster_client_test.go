package kronoshttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
)

type addNodeHandlerTestCase struct {
	name      string
	request   *AddNodeRequest // request is the AddNodeRequest as sent by the client
	errServer error           // errServer is the error returned by the server.
	// responseCode is the HTTP response code sent by the server in case errServer
	// is not  nil. If errServer is nil then this field is neglected and StatusOK
	// is sent.
	responseCode      int
	expectedErrClient error // expectedErrClient is expected error seen on client side.
}

// testAddNodeHandler handles HTTP requests on /add. It simply checks if the
// request is a valid AddNodeRequest and responds with errServer and
// responseCode as supplied in the test case in response to request. It sends
// requests received onto receivedReq channel
type testAddNodeHandler struct {
	t        *testing.T
	testCase addNodeHandlerTestCase
	// handler sends the add node requests on receivedReq.
	receivedReq chan<- *AddNodeRequest
}

func (h *testAddNodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a := assert.New(h.t)
	rc := h.testCase.responseCode
	errToReturn := h.testCase.errServer
	// Only POST requests are supported by cluster handler.
	if r.Method != http.MethodPost {
		h.t.Fatalf("Expected method post, got %v", r.Method)
	}
	if errToReturn != nil {
		http.Error(w, errToReturn.Error(), rc)
		return
	}
	// Check if the request has a valid AddNodeRequest.
	req, err := addNodeRequestFromReader(r.Body)
	a.NoError(err)
	h.receivedReq <- req
	w.WriteHeader(http.StatusOK)
}

func TestClusterClientAddNode(t *testing.T) {
	testCases := []addNodeHandlerTestCase{
		{
			name:    "no error",
			request: &AddNodeRequest{NodeID: "1", Address: "127.0.0.1:1"},
		},
		{
			name:         "server error",
			request:      &AddNodeRequest{NodeID: "1", Address: "127.0.0.1:1"},
			errServer:    errors.New("test error1"),
			responseCode: http.StatusNotFound,
			expectedErrClient: errors.New(
				`add node request {"node_id":"1","address":"127.0.0.1:1"} failed. status: 404, msg: test error1`),
		},
	}
	ctx := context.TODO()
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				receivedReq := make(chan *AddNodeRequest, 2)
				defer close(receivedReq)
				handler := http.NewServeMux()
				handler.Handle(
					"/add",
					&testAddNodeHandler{testCase: tc, t: t, receivedReq: receivedReq},
				)
				ts := httptest.NewServer(handler)
				defer ts.Close()
				tsURL, err := url.Parse(ts.URL)
				a := assert.New(t)
				a.NoError(err)
				c := ClusterClient{url: *tsURL, client: http.DefaultClient}
				err = c.AddNode(ctx, tc.request)
				if tc.expectedErrClient == nil {
					a.NoError(err)
				} else {
					a.EqualError(err, tc.expectedErrClient.Error())
				}
				if tc.expectedErrClient == nil {
					req := <-receivedReq
					a.Equal(req, tc.request)
				}
			},
		)
	}
}

func TestRequestURL(t *testing.T) {
	a := assert.New(t)
	baseURL, err := url.Parse("http://127.0.0.1/cluster")
	a.NoError(err)
	reqURL := kronosutil.AddToURLPath(*baseURL, "add")
	a.Equal(reqURL.String(), "http://127.0.0.1/cluster/add")
	reqURL = kronosutil.AddToURLPath(*baseURL, "remove")
	a.Equal(reqURL.String(), "http://127.0.0.1/cluster/remove")
}

type removeNodeHandlerTestCase struct {
	name      string
	request   *RemoveNodeRequest // request is the RemoveNodeRequest as sent by the client
	errServer error              // errServer is expected error returned by the server.
	// responseCode is the HTTP response code sent by the server in case errServer
	// is not  nil. If errServer is nil then this field is neglected and StatusOK
	// is sent.
	responseCode      int
	expectedErrClient error // expectedErrClient is expected error seen on client side.
}

// testRemoveNodeHandler handles HTTP requests on /remove. It simply checks if
// the request is a valid RemoveNodeRequest and replies with errServer and
// responseCode as supplied by the test case on request. It sends
// requests received onto receivedReq channel.
type testRemoveNodeHandler struct {
	t        *testing.T
	testCase removeNodeHandlerTestCase
	// handler sends the remove node requests on receivedReq.
	receivedReq chan<- *RemoveNodeRequest
}

func (h *testRemoveNodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a := assert.New(h.t)
	status := h.testCase.responseCode
	errToReturn := h.testCase.errServer
	// Only POST requests are supported by cluster handler.
	if r.Method != http.MethodPost {
		h.t.Fatalf("Expected method post, got %v", r.Method)
	}
	if errToReturn != nil {
		http.Error(w, errToReturn.Error(), status)
		return
	}
	// Check if the request has a valid RemoveNodeRequest.
	req, err := removeNodeRequestFromReader(r.Body)
	a.NoError(err)
	h.receivedReq <- req
	w.WriteHeader(http.StatusOK)
}

func TestClusterClientRemoveNode(t *testing.T) {
	testCases := []removeNodeHandlerTestCase{
		{
			name:    "no error",
			request: &RemoveNodeRequest{NodeID: "1"},
		},
		{
			name:         "server error",
			request:      &RemoveNodeRequest{NodeID: "1"},
			errServer:    errors.Errorf("test error1"),
			responseCode: http.StatusNotFound,
			expectedErrClient: errors.Errorf(
				`remove node request {"node_id":"1"} failed. status: 404, msg: test error1`,
			),
		},
	}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				receivedReq := make(chan *RemoveNodeRequest, 2)
				defer close(receivedReq)
				handler := http.NewServeMux()
				handler.Handle(
					"/remove",
					&testRemoveNodeHandler{testCase: tc, t: t, receivedReq: receivedReq},
				)
				ts := httptest.NewServer(handler)
				defer ts.Close()
				tsURL, err := url.Parse(ts.URL)
				a := assert.New(t)
				a.NoError(err)
				c := ClusterClient{url: *tsURL, client: http.DefaultClient}
				err = c.RemoveNode(context.TODO(), tc.request)
				if tc.expectedErrClient == nil {
					a.NoError(err)
				} else {
					a.EqualError(err, tc.expectedErrClient.Error())
				}
				if err == nil {
					req := <-receivedReq
					a.Equal(tc.request, req)
				}
			},
		)
	}
}

type nodesHandlerTestCase struct {
	name      string
	errServer error // errServer is expected error returned by the server.
	nodes     []Node
	// responseCode is the HTTP response code sent by the server in case errServer
	// is not  nil. If errServer is nil then this field is neglected and StatusOK
	// is sent.
	responseCode      int
	expectedErrClient error // expectedErrClient is expected error seen on client side.
}

// testNodesHandler handles HTTP requests on /node. It replies with errServer and
// responseCode as supplied by the ith test case on ith request. If errServer is
// not nil, it also returns cluster in response as described by the ith
// test case. This also implies that it can only handle only n requests where n
// is the number of test cases.
type testNodesHandler struct {
	t         *testing.T
	testCases []nodesHandlerTestCase
	// increment on every response to give the ith response on ith request.
	i int
}

func (h *testNodesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a := assert.New(h.t)
	status := h.testCases[h.i].responseCode
	nodes := h.testCases[h.i].nodes
	errToReturn := h.testCases[h.i].errServer
	h.i++
	if r.Method != http.MethodGet {
		h.t.Fatalf("Expected method get, got %v", r.Method)
	}
	if errToReturn != nil {
		http.Error(w, errToReturn.Error(), status)
		return
	}
	cp, err := json.Marshal(nodes)
	a.NoError(err)
	_, err = w.Write(cp)
	a.NoError(err)
	w.WriteHeader(http.StatusOK)
}

func TestClusterClientNodes(t *testing.T) {
	testCases := []nodesHandlerTestCase{
		{
			name: "no error",
			nodes: []Node{
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
			},
		},
		{
			name:         "server error",
			errServer:    errors.Errorf("test error1"),
			responseCode: http.StatusNotFound,
			expectedErrClient: errors.Errorf(
				`nodes request failed. status: 404, msg: test error1`,
			),
		},
	}
	handler := http.NewServeMux()
	handler.Handle(
		"/nodes",
		&testNodesHandler{testCases: testCases, t: t},
	)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	tsURL, err := url.Parse(ts.URL)
	a := assert.New(t)
	a.NoError(err)
	c := ClusterClient{url: *tsURL, client: http.DefaultClient}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				a := assert.New(t)
				nodes, err := c.Nodes(context.TODO())
				if tc.expectedErrClient == nil {
					a.NoError(err)
				} else {
					a.EqualError(err, tc.expectedErrClient.Error())
				}
				if err != nil {
					a.Equal(tc.nodes, nodes)
				}
			},
		)
	}
}

type grpcAddrHandlerTestCase struct {
	name      string
	errServer error // errServer is expected error returned by the server.
	grpcAddr  *kronospb.NodeAddr
	// responseCode is the HTTP response code sent by the server in case errServer
	// is not nil. If errServer is nil then this field is ignored and StatusOK
	// is sent.
	responseCode      int
	expectedErrClient error // expectedErrClient is expected error seen on client side.
}

func (h *grpcAddrHandlerTestCase) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status := h.responseCode
	errToReturn := h.errServer
	if r.Method != http.MethodGet {
		http.Error(w, errToReturn.Error(), http.StatusBadRequest)
		return
	}
	if errToReturn != nil {
		http.Error(w, errToReturn.Error(), status)
		return
	}
	if _, err := w.Write([]byte(kronosutil.NodeAddrToString(h.grpcAddr))); err != nil {
		http.Error(w, errToReturn.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TestClusterClientGRPCAddr(t *testing.T) {
	testCases := []grpcAddrHandlerTestCase{
		{
			name:     "no error",
			grpcAddr: &kronospb.NodeAddr{Host: "localhost", Port: "5767"},
		},
		{
			name:         "server error",
			errServer:    errors.Errorf("test error1"),
			responseCode: http.StatusNotFound,
			expectedErrClient: errors.Errorf(
				`grpc address request failed. status: 404, msg: test error1`,
			),
		},
	}
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				a := assert.New(t)
				handler := http.NewServeMux()
				handler.Handle(
					"/grpc_addr",
					&tc,
				)
				ts := httptest.NewServer(handler)
				defer ts.Close()
				tsURL, err := url.Parse(ts.URL)
				a.NoError(err)
				c := ClusterClient{url: *tsURL, client: http.DefaultClient}
				grpcAddr, err := c.GRPCAddr(context.TODO())
				if tc.expectedErrClient == nil {
					a.NoError(err)
				} else {
					a.EqualError(err, tc.expectedErrClient.Error())
				}
				if err != nil {
					a.Equal(tc.grpcAddr, grpcAddr)
				}

			},
		)
	}
}
