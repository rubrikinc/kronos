package kronoshttp

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/scaledata/etcd/pkg/transport"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
)

// ClusterPath is the endpoint of HTTP server which handles cluster requests.
const ClusterPath = "cluster"

// ClusterClient issues HTTP requests on the URL for performing cluster
// operations like removal or addition of nodes. This contains a http client
// that is thread-safe and should be reused to avoid leaking TCP connections.
// Close should be called after completing all the requests to avoid connection
// leaks.
type ClusterClient struct {
	// client is used to issue HTTP requests to url.
	client *http.Client
	// transport is used to maintain connections to the server.
	transport *http.Transport
	// url denotes a HTTP endpoint running on an active member of the raft cluster
	// on which cluster operations need to be performed.
	url url.URL
}

// NewClusterClient creates a new ClusterClient which can be used to perform
// cluster operations like addition or removal of nodes on the raft HTTP server
// listening on the given host. All the requests of this client have a default
// timeout of 10 minutes. Requests can be passed with contexts with smaller
// timeouts as per use case. Close should be called after completing all the
// requests to avoid connection leaks.
func NewClusterClient(host *kronospb.NodeAddr, tlsInfo transport.TLSInfo) (*ClusterClient, error) {
	secure := !tlsInfo.Empty()
	hostURL := kronosutil.AddrToURL(host, secure)
	clusterURL, err := hostURL.Parse(ClusterPath)
	if err != nil {
		return nil, err
	}
	const dialTimeout = 10 * time.Second
	rt, err := transport.NewTransport(tlsInfo, dialTimeout)
	if err != nil {
		return nil, err
	}
	return &ClusterClient{
		url:       *clusterURL,
		client:    &http.Client{Transport: rt, Timeout: 10 * time.Minute},
		transport: rt,
	}, nil
}

// AddNode sends a request to add a new node to the raft HTTP server of
// ClusterClient.
func (c *ClusterClient) AddNode(ctx context.Context, request *AddNodeRequest) error {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return err
	}
	addNodeURL := kronosutil.AddToURLPath(c.url, requestTypeAdd)
	log.Infof(
		ctx,
		"AddNode URL: %s, request: %s",
		addNodeURL.String(),
		string(requestJSON),
	)
	httpReq, err := http.NewRequest(
		http.MethodPost,
		addNodeURL.String(),
		bytes.NewReader(requestJSON),
	)
	if err != nil {
		return err
	}
	httpReq = httpReq.WithContext(ctx)
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf(
			"add node request %s failed. status: %v, msg: %s",
			requestJSON,
			resp.StatusCode,
			bytes.TrimSpace(msg),
		)
	}
	return nil
}

// RemoveNode sends a request to remove a node to the raft HTTP server of
// ClusterClient.
func (c *ClusterClient) RemoveNode(ctx context.Context, request *RemoveNodeRequest) error {
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return err
	}
	removeNodeURL := kronosutil.AddToURLPath(c.url, requestTypeRemove)
	log.Infof(
		ctx,
		"RemoveNode URL: %s, request: %s",
		removeNodeURL.String(),
		string(requestJSON),
	)
	httpReq, err := http.NewRequest(
		http.MethodPost,
		removeNodeURL.String(),
		bytes.NewReader(requestJSON),
	)
	if err != nil {
		return err
	}
	httpReq = httpReq.WithContext(ctx)
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := ioutil.ReadAll(resp.Body)
		return errors.Errorf(
			"remove node request %s failed. status: %v, msg: %s",
			requestJSON,
			resp.StatusCode,
			bytes.TrimSpace(msg),
		)
	}
	return nil
}

// Nodes sends a request to get current nodes of the cluster to the raft HTTP
// server of ClusterClient.
func (c *ClusterClient) Nodes(ctx context.Context) ([]Node, error) {
	nodesURL := kronosutil.AddToURLPath(c.url, requestTypeNodes)
	log.Infof(
		ctx,
		"Get nodes URL: %s",
		nodesURL.String(),
	)
	httpReq, err := http.NewRequest(
		http.MethodGet,
		nodesURL.String(),
		nil,
	)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf(
			"nodes request failed. status: %v, msg: %s",
			resp.StatusCode,
			bytes.TrimSpace(msg),
		)
	}
	respJSON, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var nodes []Node
	if err := json.Unmarshal(respJSON, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// GRPCAddr sends a request to get grpc address of the raft HTTP
// server of ClusterClient.
func (c *ClusterClient) GRPCAddr(ctx context.Context) (*kronospb.NodeAddr, error) {
	grpcAddrURL := kronosutil.AddToURLPath(c.url, requestTypeGRPCAddr)
	httpReq, err := http.NewRequest(
		http.MethodGet,
		grpcAddrURL.String(),
		nil,
	)
	if err != nil {
		return nil, err
	}
	httpReq = httpReq.WithContext(ctx)
	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := ioutil.ReadAll(resp.Body)
		return nil, errors.Errorf(
			"grpc address request failed. status: %v, msg: %s",
			resp.StatusCode,
			bytes.TrimSpace(msg),
		)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return kronosutil.NodeAddr(string(body))
}

// Close closes all the idle connections that cluster client has made.
func (c *ClusterClient) Close() {
	c.transport.CloseIdleConnections()
}
