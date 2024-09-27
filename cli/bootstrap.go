package cli

import (
	"context"
	"fmt"

	"github.com/gogo/status"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
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
		"expected-node-count",
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
			log.Fatalf(ctx, "Error creating SSL creds: %v", err)
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
		st, ok := status.FromError(err)
		if ok && status.Code(err) == codes.AlreadyExists {
			res := st.Details()[0].(*kronospb.BootstrapResponse)
			if res == nil || res.NodeCount < bootstrapCtx.expectedNodeCount/2+1 {
				log.Fatalf(ctx, "Cluster already bootstrapped, but actual node count: %d is less than expected node count: %d",
					res.NodeCount, bootstrapCtx.expectedNodeCount)
			}
			log.Infof(ctx, "Cluster already bootstrapped with cluster-id: %s and %d nodes", res.ClusterId, res.NodeCount)
			return
		}
		log.Fatalf(ctx, "Attempt to bootstrap kronos cluster encountered "+
			"error, bootstrap failed with err: %+v", err)
	}
	quorum := bootstrapCtx.expectedNodeCount/2 + 1
	if res.NodeCount < quorum {
		log.Fatalf(ctx, "Attempt to bootstrap kronos cluster failed, "+
			"expected node count: %d, actual node count: %d",
			bootstrapCtx.expectedNodeCount, res.NodeCount)
	} else {
		log.Infof(ctx, "Cluster bootstrapped successfully with cluster-id: %s", res.ClusterId)
	}
}
