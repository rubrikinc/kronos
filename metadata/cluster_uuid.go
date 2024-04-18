package metadata

import (
	"context"
	"os"
	"path/filepath"

	"github.com/rubrikinc/kronos/checksumfile"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"go.etcd.io/etcd/pkg/v3/types"
)

const (
	clusterUUIDFileName = "cluster-uuid"
)

func newClusterID() types.ID {
	r, _ := checksumfile.NewPseudoRand()
	return types.ID(r.Uint64())
}

// clusterUUIDFilePath returns the path to the file storing cluster UUID given
// the path to the data directory
func clusterUUIDFilePath(dataDir string) string {
	return filepath.Join(dataDir, clusterUUIDFileName)
}

// FetchOrAssignClusterUUID returns the cluster UUID if the clusterUUIDFile exists in data
// directory. Otherwise it persists a new cluster UUID to clusterUUIDFile and returns
// the same.
func FetchOrAssignClusterUUID(ctx context.Context,
	dataDir string, assignDefault bool) (id types.ID) {
	clusterUUIDFile := clusterUUIDFilePath(dataDir)
	if _, err := os.Stat(clusterUUIDFile); err != nil {
		if os.IsNotExist(err) {
			if assignDefault {
				// To take care of old clusters upgrading
				// since we used default ID of 0x1000 earlier.
				id = types.ID(0x1000)
			} else {
				id = newClusterID()
			}
			if err := checksumfile.Write(clusterUUIDFile, []byte(id.String())); err != nil {
				log.Fatalf(ctx, "Failed to write cluster UUID to file, error: %v", err)
			}
		} else {
			log.Fatal(ctx, err)
		}
	} else {
		if id, err = FetchClusterUUID(dataDir); err != nil {
			log.Fatal(ctx, err)
		}
	}
	return
}

// FetchClusterUUID returns the cluster UUID if clusterUUIDFile exists in data
// directory
func FetchClusterUUID(dataDir string) (id types.ID, err error) {
	clusterUUIDFile := clusterUUIDFilePath(dataDir)
	if _, err := os.Stat(clusterUUIDFile); err != nil {
		return 0, err
	}

	rawClusterUUID, err := checksumfile.Read(clusterUUIDFile)
	if err != nil {
		return 0, err
	}

	return types.IDFromString(string(rawClusterUUID))
}

// PersistClusterUUID persists the cluster UUID to clusterUUIDFile
func PersistClusterUUID(ctx context.Context, dataDir string, id types.ID) error {
	clusterUUIDFile := clusterUUIDFilePath(dataDir)
	if err := checksumfile.Write(clusterUUIDFile, []byte(id.String())); err != nil {
		log.Fatalf(ctx, "Failed to write cluster UUID to file, error: %v", err)
	}
	return nil
}
