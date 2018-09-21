package server

import (
	"context"
	"io"

	"github.com/rubrikinc/kronos/pb"
)

// Client is used by KronosServer to query KronosTime of nodes in the
// Kronos cluster
// Client is not thread safe
type Client interface {
	io.Closer
	// OracleTime returns the KronosTime if the server is the current oracle.
	// Otherwise it returns an NOT_ORACLE in OracleTimeResponse
	OracleTime(
		ctx context.Context,
		server *kronospb.NodeAddr,
	) (*kronospb.OracleTimeResponse, error)
	// KronosTime returns the KronosTime for given server even if it is
	// not the oracle.
	KronosTime(
		ctx context.Context,
		server *kronospb.NodeAddr,
	) (*kronospb.KronosTimeResponse, error)
	// Status returns the status of the given Server
	Status(
		ctx context.Context,
		server *kronospb.NodeAddr,
	) (*kronospb.StatusResponse, error)
}
