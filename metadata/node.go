package metadata

import (
	"context"
	"os"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/scaledata/etcd/pkg/types"

	"github.com/rubrikinc/kronos/checksumfile"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

// nodeIDFileName is used to store the nodeID of the raft instance being created.
const nodeIDFileName = "node-info"

func newNodeID() types.ID {
	r, _ := randutil.NewPseudoRand()
	return types.ID(r.Uint64())
}

// nodeInfoFilePath returns the path to the file storing Node ID given
// the path to the data directory
func nodeInfoFilePath(dataDir string) string {
	return filepath.Join(dataDir, nodeIDFileName)
}

// FetchOrAssignNodeID returns the nodeID if the nodeInfoFile exists in data
// directory. Otherwise it persists a new nodeID to nodeInfoFile and returns
// the same.
func FetchOrAssignNodeID(ctx context.Context, dataDir string) (id types.ID) {
	nodeInfoFile := nodeInfoFilePath(dataDir)
	if _, err := os.Stat(nodeInfoFile); err != nil {
		if os.IsNotExist(err) {
			id = newNodeID()
			if err := checksumfile.Write(nodeInfoFile, []byte(id.String())); err != nil {
				log.Fatalf(ctx, "Failed to write nodeID to file, error: %v", err)
			}
		} else {
			log.Fatal(ctx, err)
		}
	} else {
		if id, err = FetchNodeID(dataDir); err != nil {
			log.Fatal(ctx, err)
		}
	}
	return
}

// FetchNodeID returns the nodeID if nodeInfoFile exists in data directory
// Otherwise it returns an error
func FetchNodeID(dataDir string) (id types.ID, err error) {
	nodeInfoFile := nodeInfoFilePath(dataDir)
	if _, err := os.Stat(nodeInfoFile); err != nil {
		return 0, err
	}

	rawNodeID, err := checksumfile.Read(nodeInfoFile)
	if err != nil {
		return 0, err
	}

	return types.IDFromString(string(rawNodeID))
}
