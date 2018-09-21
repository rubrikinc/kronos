package mock

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

// Client is an in memory implementation of Client used in tests
// This is tested in server_test.go
type Client struct {
	// connectedServer is the kronos Server this Client is currently connected to
	connectedServer string
	// Nodes is a map from NodeAddr.String() to Kronos Servers. This map is used
	// by the Client to query kronos servers
	nodes map[string]*Node
	// Latency is the Latency added to each request served
	Latency time.Duration
}

var _ server.Client = &Client{}

// connect connects to the given Server if not already connected
func (c *Client) connect(ctx context.Context, server *kronospb.NodeAddr) error {
	if server == nil {
		return errors.Errorf("nil server")
	}

	if _, ok := c.nodes[server.String()]; !ok {
		return errors.Errorf("server %s does not exist", server)
	}

	c.connectedServer = server.String()
	return nil
}

func (c *Client) node(ctx context.Context, server *kronospb.NodeAddr) (*Node, error) {
	if err := c.connect(ctx, server); err != nil {
		return nil, err
	}

	node, ok := c.nodes[c.connectedServer]
	if !ok {
		return nil, errors.Errorf("server %s does not exist", c.connectedServer)
	}
	return node, nil
}

// OracleTime implements the Client interface.
func (c *Client) OracleTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.OracleTimeResponse, error) {
	node, err := c.node(ctx, server)
	if err != nil {
		return nil, err
	}
	response, err := node.Server.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	if err != nil {
		return response, err
	}
	response.Rtt = int64(c.Latency)
	return response, nil
}

// KronosTime implements the Client interface.
func (c *Client) KronosTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.KronosTimeResponse, error) {
	node, err := c.node(ctx, server)
	if err != nil {
		return nil, err
	}
	response, err := node.Server.KronosTimeNow(ctx)
	if err != nil {
		return response, err
	}
	response.Rtt = int64(c.Latency)
	return response, nil
}

// Status implements the Client interface.
func (c *Client) Status(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.StatusResponse, error) {
	node, err := c.node(ctx, server)
	if err != nil {
		return nil, err
	}
	return node.Server.Status(ctx, &kronospb.StatusRequest{})
}

// Close closes all open connections.
func (c *Client) Close() error {
	return nil
}

// newInMemClient returns an instance of Client
func newInMemClient(nodes map[string]*Node) server.Client {
	return &Client{nodes: nodes}
}
