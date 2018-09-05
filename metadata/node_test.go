package metadata

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFetchOrAssignNodeID(t *testing.T) {
	a := assert.New(t)
	ctx := context.TODO()
	dataDir, err := ioutil.TempDir("", "data_dir")
	a.NoError(err)
	nodeID1 := FetchOrAssignNodeID(ctx, dataDir)
	nodeID2 := FetchOrAssignNodeID(ctx, dataDir)
	a.Equal(nodeID1, nodeID2)
	nodeID3, err := FetchNodeID(dataDir)
	a.NoError(err)
	a.Equal(nodeID1, nodeID3)
	_, err = FetchNodeID("invalid_dir_entry")
	a.NotNil(err)
}

func TestNewNodeID(t *testing.T) {
	a := assert.New(t)
	nodeIDs := make(map[uint64]bool)
	numNodes := 1000
	for i := 0; i < numNodes; i++ {
		nodeIDs[uint64(newNodeID())] = true
	}
	a.Equal(numNodes, len(nodeIDs))
}
