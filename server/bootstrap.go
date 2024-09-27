package server

import (
	"context"
	"errors"
	"time"

	"github.com/gogo/status"
	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/metadata"
	kronospb "github.com/rubrikinc/kronos/pb"
	"go.etcd.io/etcd/pkg/v3/types"
	"google.golang.org/grpc/codes"
)

func (k *Server) Bootstrap(ctx context.Context, req *kronospb.BootstrapRequest) (*kronospb.BootstrapResponse, error) {
	if clusterID, err := metadata.FetchClusterUUID(k.dataDir); err == nil {
		cluster, err := metadata.LoadCluster(k.dataDir, true)
		if err != nil {
			return nil, err
		}
		stat := status.New(codes.AlreadyExists, "Cluster already bootstrapped")
		statWithDetails, err := stat.WithDetails(&kronospb.BootstrapResponse{ClusterId: clusterID.String(), NodeCount: int32(len(cluster.ActiveNodes()))})
		if err != nil {
			return nil, err
		}
		return nil, statWithDetails.Err()
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
		if count >= req.ExpectedNodeCount/2+1 {
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
