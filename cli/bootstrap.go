package cli

import (
	"context"
	"fmt"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var bootstrapCtx struct {
	grpcAddr          string
	expectedNodeCount int32
}

var bootstrapCmd = &cobra.Command{
	Use:   "bootstrap",
	Short: "Bootstrap the kronos cluster",
	Long: `
Bootstrap the kronos cluster with the given node as the first node in the
cluster.
`,
	Run: func(cmd *cobra.Command, args []string) { runBootstrap() },
}

func init() {
	bootstrapCmd.Flags().StringVar(
		&bootstrapCtx.grpcAddr,
		"grpc-addr",
		fmt.Sprintf("localhost:%s", server.KronosDefaultGRPCPort),
		"GRPC address of the node to bootstrap the cluster.",
	)
	bootstrapCmd.Flags().Int32Var(
		&bootstrapCtx.expectedNodeCount,
		"exepected-node-count",
		1,
		"Expected number of nodes in the cluster.",
	)
}

func runBootstrap() {
	ctx := context.Background()
	addr := bootstrapCtx.grpcAddr
	var dialOpts grpc.DialOption
	certsDir := kronosCertsDir()
	if certsDir == "" {
		dialOpts = grpc.WithInsecure()
	} else {
		creds, err := kronosutil.SSLCreds(certsDir)
		if err != nil {
			log.Infof(ctx, "Error creating SSL creds: %v", err)
			return
		}
		dialOpts = grpc.WithTransportCredentials(creds)
	}
	var err error
	conn, err := grpc.Dial(addr, dialOpts)
	if err != nil {
		log.Fatalf(ctx, "Error dialing peer %s: %v", addr, err)
		return
	}
	defer kronosutil.CloseWithErrorLog(ctx, conn)
	client := kronospb.NewBootstrapClient(conn)
	res, err := client.Bootstrap(ctx,
		&kronospb.BootstrapRequest{ExpectedNodeCount: bootstrapCtx.expectedNodeCount})
	if err != nil {
		log.Errorf(ctx, "Attempt to bootstrap the cluster encountered "+
			"error, bootstrap may have failed, err: %+v", err)
		return
	}
	if res.NodeCount < (bootstrapCtx.expectedNodeCount+1)/2 {
		log.Errorf(ctx, "Attempt to bootstrap the cluster failed, "+
			"expected node count: %d, actual node count: %d",
			bootstrapCtx.expectedNodeCount, res.NodeCount)
		return
	}
	gossipClient := kronospb.NewGossipClient(conn)
	resp, err := gossipClient.NodeLs(ctx, &kronospb.NodeLsRequest{})
	if err != nil {
		log.Errorf(ctx, "Error fetching gossiped information: %v", err)
		return
	}
	count := int32(0)
	for _, node := range resp.Nodes {
		if node.IsBootstrapped && !node.IsRemoved && node.ClusterId == res.ClusterId {
			count++
		}
	}
	if count < (bootstrapCtx.expectedNodeCount+1)/2 {
		log.Errorf(ctx, "Attempt to bootstrap the cluster failed, "+
			"expected node count: %d, actual node count: %d",
			bootstrapCtx.expectedNodeCount, count)
		return
	} else {
		log.Infof(ctx, "Cluster bootstrapped successfully with cluster-id: %s", res.ClusterId)
	}
}
