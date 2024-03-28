package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var gossipCtx struct {
	grpcAddr string
}

var gossipCmd = &cobra.Command{
	Use:   "gossip",
	Short: "Fetch gossiped information from a node",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Usage()
		os.Exit(1)
	},
}

var nodeLsCmd = &cobra.Command{
	Use:   "ls",
	Short: "Show gossiped information of nodes in the cluster",
	Run: func(cmd *cobra.Command, args []string) {
		runGossipLs()
	},
}

func init() {
	gossipCmd.AddCommand(nodeLsCmd)
	gossipCmd.Flags().StringVar(
		&gossipCtx.grpcAddr,
		"grpc-addr",
		fmt.Sprintf("localhost:%s", server.KronosDefaultGRPCPort),
		"GRPC address of the node to bootstrap the cluster.",
	)
}

func runGossipLs() {
	ctx := context.Background()
	addr := gossipCtx.grpcAddr
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
	client := kronospb.NewGossipClient(conn)
	resp, err := client.NodeLs(ctx, &kronospb.NodeLsRequest{})
	if err != nil {
		log.Fatalf(ctx, "Error fetching gossiped information: %v", err)
		return
	}
	for _, node := range resp.Nodes {
		fmt.Printf("%+v\n", *node)
	}
}
