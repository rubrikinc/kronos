package kronoshttp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/pkg/errors"
	"github.com/scaledata/etcd/pkg/types"
	"github.com/scaledata/etcd/raft/sdraftpb"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	"github.com/rubrikinc/kronos/pb"
)

// supported HTTP URIs
const (
	// requestTypeAdd is the suffix for HTTP endpoint to request node addition.
	requestTypeAdd = "add"
	// requestTypeRemove is the suffix for HTTP endpoint to request node removal.
	requestTypeRemove = "remove"
	// requestTypeNodes is the suffix for HTTP endpoint to get the nodes in the
	// cluster according to the server.
	requestTypeNodes = "nodes"
	// requestTypeGRPCAddr is the suffix for HTTP endpoint to get the grpc address
	// of the node.
	requestTypeGRPCAddr = "grpc_addr"
)

// Node is used to store the metadata for every node that we send in response to
// /cluster/nodes requests.
type Node struct {
	*kronospb.Node
	NodeID string `json:"node_id"`
}

// AddNodeRequest is used by a new node to send the data required to add
// itself to an existing cluster. The server adds the node with given raft
// address with raft id NodeID.
type AddNodeRequest struct {
	// NodeID should be in hex format. uin64 can be converted to nodeID using:
	// nodeID uint64 -> types.Id(nodeID).String().
	NodeID string `json:"node_id"`
	// Address should be in ip_address:raft_port format.
	Address string `json:"address"`
}

func addNodeRequestFromReader(r io.ReadCloser) (*AddNodeRequest, error) {
	requestJSON, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var request AddNodeRequest
	if err := json.Unmarshal(requestJSON, &request); err != nil {
		return nil, err
	}
	return &request, err
}

// RemoveNodeRequest is used to remove node with raft id NodeID.
type RemoveNodeRequest struct {
	NodeID string `json:"node_id"`
}

// removeNodeRequestFromReader returns a remove node request after reading it
// in marshaled form from r.
func removeNodeRequestFromReader(r io.ReadCloser) (*RemoveNodeRequest, error) {
	requestJSON, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var request RemoveNodeRequest
	if err := json.Unmarshal(requestJSON, &request); err != nil {
		return nil, err
	}
	return &request, err
}

// ClusterHandler handles cluster operations via to http requests.
type ClusterHandler struct {
	// confChangeC is used to pass the configuration changes to the cluster.
	confChangeC chan<- sdraftpb.ConfChange
	// dataDir is the data directory of the raft node. It is used to get the
	// cluster metadata, which is by default stored in cluster-info file in
	// data directory.
	dataDir string
	// confChangeTotalTimeout represents the total time for which we retry the
	// confChange.
	confChangeTotalTimeout time.Duration
	// grpcAddr is the grpc address of this node
	grpcAddr *kronospb.NodeAddr
}

// NewClusterHandler returns a cluster handler which handles cluster requests
// and sends the corresponding confChanges to confChangeC.
func NewClusterHandler(
	confChangeC chan<- sdraftpb.ConfChange, dataDir string, grpcAddr *kronospb.NodeAddr,
) *ClusterHandler {
	// DefaultConfChangeTotalTimeout represents the total time for which we retry the
	// confChange.
	const DefaultConfChangeTotalTimeout = time.Minute
	return &ClusterHandler{
		confChangeC:            confChangeC,
		dataDir:                dataDir,
		confChangeTotalTimeout: DefaultConfChangeTotalTimeout,
		grpcAddr:               grpcAddr,
	}
}

// tryConfChange proposes a confChange cc to confChangeC and retries till verify
// returns true.
// ConfChanges can fail as etcd/raft only allows one pending confChange at a
// time. ConfChange is an asynchronous process, so verify is executed
// periodically to validate that the conf change has been applied. eg.
// we need to poll the cluster metadata (which is updated in raft.go, when we
// get a confChange entry to apply) to check if the confChange has succeeded.
// Idempotency of retries: raft internally maintains raft state for every
// peer as a map from nodeID to progress (and state of the peer according to the
// node).
// When an add node conf entry is to be applied, it updates the same map. If the
// node already exists in the map, the node state is reset to previous value. But
// raft on new node has not started and so resetting it has no effect. Cluster
// metadata also maintains a map from nodeID to address in cluster metadata,
// which makes the confChange entries idempotent even in our metadata updates.
// It has been observed empirically that retrying a successful confChange entry
// doesn't affect length of progress slice which is used to track quorum in raft.
// In case of removeNode, the confChange entry just deletes the nodeID from the
// map, making it idempotent.
func tryConfChange(
	confChangeC chan<- sdraftpb.ConfChange,
	cc sdraftpb.ConfChange,
	timeout time.Duration,
	verify func() bool,
) error {
	if verify() {
		return nil
	}
	// retry till the request times out or verify returns true.
	return retry.ForDuration(
		timeout,
		func() error {
			confChangeC <- cc
			// confChangeAttemptTimeout represents the time for which we wait for the
			// confChange to get reflected in the cluster metadata before sending another
			// confChangeRequest.
			const confChangeAttemptTimeout = time.Second
			time.Sleep(confChangeAttemptTimeout)
			// check if the confChange is reflected in cluster metadata.
			if verify() {
				return nil
			}
			return errors.Errorf("confChange %v has not reflected in cluster metadata", cc)
		},
	)
}

// httpError is a wrapper around error and is returned if there were any errors
// processing a HTTP request in cluster handler.
type httpError struct {
	error                 // base error
	handler string        // handler type where the error occurred
	request *http.Request // request which got the error
	event   string        // event describes the event which caused the error
}

func (h httpError) Error() string {
	return fmt.Sprintf(
		"handle error: handler: %s, request: %v, event: %s, error: %v",
		h.handler,
		h.request,
		h.event,
		h.error,
	)
}

// handleAdd handles the request with requestType Add.
func (h *ClusterHandler) handleAdd(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (int, error) {
	const handler = "clusterHandler-handleAdd"
	request, err := addNodeRequestFromReader(r.Body)
	if err != nil {
		return http.StatusBadRequest,
			httpError{
				error:   err,
				handler: handler,
				request: r,
				event:   "httpRequest-to-AddNodeRequest",
			}
	}
	id, err := types.IDFromString(request.NodeID)
	if err != nil {
		return http.StatusBadRequest,
			httpError{error: err, handler: handler, request: r, event: "id-from-string"}
	}
	// tryConfChange and retry till it gets reflected in the cluster metadata.
	if err := tryConfChange(
		h.confChangeC,
		sdraftpb.ConfChange{
			Type:    sdraftpb.ConfChangeAddNode,
			NodeID:  uint64(id),
			Context: []byte(request.Address),
		},
		h.confChangeTotalTimeout,
		func() bool {
			// check if cluster metadata has the added node
			cluster, err := metadata.LoadCluster(h.dataDir, true /*readOnly*/)
			if err != nil {
				log.Errorf(ctx, "Failed to load cluster-metadata, error: %v", err)
				return false
			}
			_, ok := cluster.Node(request.NodeID)
			return ok
		},
	); err != nil {
		return http.StatusRequestTimeout,
			httpError{error: err, handler: handler, request: r, event: "add-node-retry"}
	}
	log.Infof(
		ctx,
		"AddNodeRequest: %v Succeeded",
		request,
	)
	return http.StatusOK, nil
}

// handleRemove handles the RemoveNode requests.
func (h *ClusterHandler) handleRemove(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (int, error) {
	const handler = "clusterHandler-handleRemove"
	request, err := removeNodeRequestFromReader(r.Body)
	if err != nil {
		return http.StatusBadRequest,
			httpError{
				error:   err,
				handler: handler,
				request: r,
				event:   "http-request-to-removeNode-request",
			}
	}
	nodeID, err := types.IDFromString(request.NodeID)
	if err != nil {
		return http.StatusBadRequest,
			httpError{error: err, handler: handler, request: r, event: "id-from-string"}
	}
	// tryConfChange and retry till it gets reflected in the cluster metadata.
	if err := tryConfChange(
		h.confChangeC,
		sdraftpb.ConfChange{
			Type:   sdraftpb.ConfChangeRemoveNode,
			NodeID: uint64(nodeID),
		},
		h.confChangeTotalTimeout,
		func() bool {
			// check if node has been removed from cluster metadata.
			cluster, err := metadata.LoadCluster(h.dataDir, true /*readOnly*/)
			if err != nil {
				log.Errorf(ctx, "Failed to load cluster-metadata, error: %v", err)
				return false
			}
			node, ok := cluster.Node(request.NodeID)
			if ok {
				return node.IsRemoved
			}
			// Node is not present in cluster metadata. Remove is a no-op
			return true
		},
	); err != nil {
		return http.StatusRequestTimeout,
			httpError{error: err, handler: handler, request: r, event: "remove-node-retry"}
	}
	return http.StatusOK, nil
}

// handleNodes handles the node requests. It responds with nodes currently part
// of the cluster.
func (h *ClusterHandler) handleNodes(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (int, error) {
	const handler = "clusterHandler-handleNodes"
	cluster, err := metadata.LoadCluster(h.dataDir, true /*readOnly*/)
	if err != nil {
		return http.StatusInternalServerError,
			httpError{error: err, handler: handler, request: r, event: "cluster-load"}
	}
	var nodes []Node
	for nodeID, node := range cluster.NodesIncludingRemoved() {
		nodes = append(nodes, Node{NodeID: nodeID, Node: node})
	}
	sort.Slice(
		nodes,
		func(i, j int) bool {
			return nodes[i].NodeID < nodes[j].NodeID
		},
	)
	respJSON, err := json.Marshal(nodes)
	if err != nil {
		return http.StatusInternalServerError,
			httpError{error: err, handler: handler, request: r, event: "marshal-cluster"}
	}
	if _, err := w.Write(respJSON); err != nil {
		return http.StatusInternalServerError,
			httpError{error: err, handler: handler, request: r, event: "write-response"}
	}
	return http.StatusOK, nil
}

// handleGRPCAddr handles the /grpc_addr requests . It responds with grpc address
// of the node.
func (h *ClusterHandler) handleGRPCAddr(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (int, error) {
	const handler = "clusterHandler-handleGRPCAddr"
	if _, err := w.Write([]byte(kronosutil.NodeAddrToString(h.grpcAddr))); err != nil {
		return http.StatusInternalServerError,
			httpError{error: err, handler: handler, request: r, event: "write-response"}
	}
	return http.StatusOK, nil
}

// ServeHTTP serves HTTP requests using ClusterHandler
func (h *ClusterHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.TODO()
	// r.Body can be nil in GET calls.
	if r.Body != nil {
		defer r.Body.Close()
	}
	log.Infof(ctx, "ClusterHandler received Request: %v", r)
	uri := r.RequestURI
	requestType := strings.TrimPrefix(uri, fmt.Sprintf("/%s/", ClusterPath))
	log.Infof(ctx, "Received request, uri: %s, requestType: %s", uri, requestType)
	switch r.Method {
	case http.MethodPost:
		switch requestType {
		case requestTypeAdd:
			status, err := h.handleAdd(ctx, w, r)
			if err != nil {
				log.Error(ctx, err)
				http.Error(w, err.Error(), status)
				return
			}
		case requestTypeRemove:
			status, err := h.handleRemove(ctx, w, r)
			if err != nil {
				http.Error(w, err.Error(), status)
				return
			}
		default:
			w.Header().Add("AllowURI", requestTypeAdd)
			w.Header().Add("AllowURI", requestTypeRemove)
			http.Error(w, "URI not allowed", http.StatusNotFound)
		}
	case http.MethodGet:
		switch requestType {
		case requestTypeNodes:
			status, err := h.handleNodes(ctx, w, r)
			if err != nil {
				log.Error(ctx, err)
				http.Error(w, err.Error(), status)
				return
			}
		case requestTypeGRPCAddr:
			status, err := h.handleGRPCAddr(ctx, w, r)
			if err != nil {
				log.Error(ctx, err)
				http.Error(w, err.Error(), status)
				return
			}
		default:
			w.Header().Add("AllowURI", requestTypeNodes)
			http.Error(w, "URI not allowed", http.StatusNotFound)
		}
	default:
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodGet)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}
