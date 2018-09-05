package kronos

import (
	"context"
	"errors"
	"time"

	"github.com/scaledata/kronos/kronoshttp"
	"github.com/scaledata/kronos/kronosstats"
	"github.com/scaledata/kronos/kronosutil/log"
	"github.com/scaledata/kronos/server"
)

var kronosServer *server.Server

// Initialize initializes the kronos server.
// After Initialization, Now() in this package returns kronos time.
// If not initialized, Now() in this package returns system time
func Initialize(ctx context.Context, config server.Config) error {
	// Stop previous server
	if kronosServer != nil {
		kronosServer.Stop()
	}

	var err error
	kronosServer, err = server.NewKronosServer(ctx, config)
	if err != nil {
		return err
	}

	go func() {
		if err := kronosServer.RunServer(ctx); err != nil {
			log.Fatal(ctx, err)
		}
	}()

	log.Info(ctx, "Kronos server initialized")
	return nil
}

// Stop stops the kronos server
func Stop() {
	if kronosServer != nil {
		kronosServer.Stop()
		log.Info(context.TODO(), "Kronos server stopped")
	}
}

// IsActive returns whether kronos is running.
func IsActive() bool {
	return kronosServer != nil
}

// Now returns Kronos time if Kronos is initialized, otherwise returns
// system time
func Now() int64 {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 100 * time.Millisecond
	ctx := context.TODO()

	for {
		kt, err := kronosServer.KronosTimeNow(ctx)
		if err != nil {
			log.Errorf(
				ctx,
				"Failed to get KronosTime, err: %v. Sleeping for %s before retrying.",
				err, timePollInterval,
			)
			time.Sleep(timePollInterval)
			continue
		}
		return kt.Time
	}
}

// NodeID returns the NodeID of the kronos server in the kronos raft cluster.
// NodeID returns an empty string if kronosServer is not initialized
func NodeID(ctx context.Context) string {
	if kronosServer == nil {
		return ""
	}

	id, err := kronosServer.ID()
	if err != nil {
		log.Fatalf(ctx, "Failed to get kronosServer.ID, err: %v", err)
	}

	return id
}

// RemoveNode removes the given node from the kronos raft cluster
func RemoveNode(ctx context.Context, nodeID string) error {
	if len(nodeID) == 0 {
		return errors.New("node id is empty")
	}

	log.Infof(ctx, "Removing kronos node %s", nodeID)
	client, err := kronosServer.NewClusterClient()
	if err != nil {
		return err
	}
	defer client.Close()

	return client.RemoveNode(ctx, &kronoshttp.RemoveNodeRequest{
		NodeID: nodeID,
	})
}

// Metrics returns KronosMetrics
func Metrics() *kronosstats.KronosMetrics {
	if kronosServer == nil {
		return nil
	}
	return kronosServer.Metrics
}
