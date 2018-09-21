package server

import (
	"context"
	"net"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
)

const rpcTimeout = 30 * time.Second

// grpcClient is an implementation of KronosClient which uses GRPC to
// query Time from Kronos servers
// grpcClient caches the last connection it made so subsequent queries
// made to the same server will not create new connections
type grpcClient struct {
	server     *kronospb.NodeAddr
	conn       *grpc.ClientConn
	grpcClient kronospb.TimeServiceClient
	certsDir   string
}

var _ Client = &grpcClient{}

// connect connects to the given server if not already connected
func (c *grpcClient) connect(ctx context.Context, server *kronospb.NodeAddr) error {
	if c.conn == nil || !proto.Equal(c.server, server) {
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				log.Error(ctx, err)
			}
		}

		var err error
		var dialOpts grpc.DialOption
		if c.certsDir == "" {
			dialOpts = grpc.WithInsecure()
		} else {
			creds, err := kronosutil.SSLCreds(c.certsDir)
			if err != nil {
				return errors.Wrap(err, "could not load TLS keys: %s")
			}
			dialOpts = grpc.WithTransportCredentials(creds)
		}

		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, rpcTimeout)
		defer cancel()
		c.conn, err = grpc.DialContext(
			ctx,
			net.JoinHostPort(server.Host, server.Port),
			dialOpts,
		)
		if err != nil {
			return err
		}

		c.grpcClient = kronospb.NewTimeServiceClient(c.conn)
		c.server = server
	}
	return nil
}

// OracleTime implements the Client interface.
func (c *grpcClient) OracleTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.OracleTimeResponse, error) {
	if err := c.connect(ctx, server); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	startTime := time.Now()
	timeResponse, err := c.grpcClient.OracleTime(ctx, &kronospb.OracleTimeRequest{})
	if timeResponse != nil {
		// time.Since uses monotonic time and is immune to clock jumps
		timeResponse.Rtt = int64(time.Since(startTime))
	}
	return timeResponse, err
}

// KronosTime implements the Client interface.
func (c *grpcClient) KronosTime(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.KronosTimeResponse, error) {
	if err := c.connect(ctx, server); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	startTime := time.Now()
	timeResponse, err := c.grpcClient.KronosTime(ctx, &kronospb.KronosTimeRequest{})
	// time.Since uses monotonic time and is immune to clock jumps
	if timeResponse != nil {
		timeResponse.Rtt = int64(time.Since(startTime))
	}
	return timeResponse, err
}

// Status implements the Client interface.
func (c *grpcClient) Status(
	ctx context.Context, server *kronospb.NodeAddr,
) (*kronospb.StatusResponse, error) {
	if err := c.connect(ctx, server); err != nil {
		return nil, err
	}

	return c.grpcClient.Status(
		ctx,
		&kronospb.StatusRequest{},
	)
}

func (c *grpcClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// NewGRPCClient creates a GRPC client to query Kronos servers
func NewGRPCClient(certsDir string) Client {
	return &grpcClient{certsDir: certsDir}
}
