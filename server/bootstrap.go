package server

import (
	"context"
	"errors"
	"time"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/scaledata/etcd/pkg/types"
)

func (k *Server) Bootstrap(ctx context.Context, req *kronospb.BootstrapRequest) (*kronospb.BootstrapResponse, error) {
	if _, err := metadata.FetchClusterUUID(k.dataDir); err == nil {
		log.Infof(ctx, "Cluster already bootstrapped")
		return &kronospb.BootstrapResponse{}, nil
	}
	k.bootstrapReqCh <- *req
	var clusterID types.ID
	var err error
	err = kronoshttp.RetryUntil(ctx, 5*time.Minute, func() error {
		clusterID, err = metadata.FetchClusterUUID(k.dataDir)
		return err
	})
	if err != nil {
		return nil, err
	}
	log.Infof(ctx, "Cluster bootstrapped with ID : %v", clusterID)
	bootstrappedNodeCount := int32(0)
	err = kronoshttp.RetryUntil(ctx, 5*time.Minute, func() error {
		nodes := k.GossipServer.GetNodeList()
		count := int32(0)
		for _, node := range nodes {
			if node.IsBootstrapped && node.ClusterId == clusterID.String() {
				count++
			}
		}
		log.Infof(ctx,
			"Waiting for expected number of nodes to join the cluster, "+
				"current : %v, expected : %v", count, req.ExpectedNodeCount)
		if count >= (req.ExpectedNodeCount+1)/2 {
			bootstrappedNodeCount = count
			return nil
		}
		return errors.New("waiting for expected number of nodes to join the cluster")
	})
	if err != nil {
		return nil, err
	}
	return &kronospb.BootstrapResponse{
		ClusterId: clusterID.String(),
		NodeCount: bootstrappedNodeCount,
	}, nil
}
