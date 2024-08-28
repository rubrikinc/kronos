package kronos

import (
	"context"
	"errors"
	"fmt"
	kronospb "github.com/rubrikinc/kronos/pb"
	"time"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosstats"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/server"
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
	var count = 0

	for {
		kt, err := kronosServer.KronosTimeNow(ctx)
		if err != nil {
			// We print the first 10 retries, then every 10th retry until 200 retries, and then every 100th retry
			if count < 10 || (count < 200 && count%10 == 0) || count%100 == 0 {
				log.Errorf(
					ctx,
					"Failed to get KronosTime after %d retries, err: %v. Sleeping for %s before retrying.",
					count, err, timePollInterval,
				)
			}
			time.Sleep(timePollInterval)
			count += 1
			continue
		}
		return kt.Time
	}
}

// Uptime returns Kronos uptime. This function can block if kronos uptime
// is invalid.
func Uptime() int64 {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 500 * time.Millisecond
	ctx := context.TODO()

	for {
		ut, err := kronosServer.KronosUptimeNow(ctx)
		if err != nil {
			time.Sleep(timePollInterval)
			continue
		}
		return ut.Uptime
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

// GetTime returns kronos time and an error if not able to
// get time within timeout
func GetTime(timeout time.Duration) (int64, error) {
	if kronosServer == nil {
		log.Fatalf(context.TODO(), "Kronos server is not initialized")
	}
	// timePollInterval is the time to wait before internally retrying
	// this function.
	// This function blocks if not initialized or if KronosTime is stale
	const timePollInterval = 100 * time.Millisecond
	ctx := context.TODO()
	var count = 0
	start := time.Now()
	for timeout == 0 || time.Since(start) < timeout {
		kt, err := kronosServer.KronosTimeNow(ctx)
		if err != nil {
			// We print the first 10 retries, then every 10th retry until 200 retries, and then every 100th retry
			if count < 10 || (count < 200 && count%10 == 0) || count%100 == 0 {
				log.Errorf(
					ctx,
					"Failed to get KronosTime after %d retries, err: %v. Sleeping for %s before retrying.",
					count, err, timePollInterval,
				)
			}
			time.Sleep(timePollInterval)
			count += 1
			continue
		}
		return kt.Time, nil
	}
	return 0, errors.New(fmt.Sprintf(
		"Couldn't get kronos time within timeout - %v", timeout))
}

func Bootstrap(ctx context.Context, expectedNodeCount int32) error {
	if kronosServer == nil {
		return errors.New("kronos server is not initialized")
	}
	_, err := kronosServer.Bootstrap(ctx, &kronospb.BootstrapRequest{
		ExpectedNodeCount: expectedNodeCount,
	})
	return err
}
