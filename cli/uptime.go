package cli

import (
	"context"
	"fmt"
	"net"

	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

var uptimeCtx struct {
	grpcAddr string
}

var uptimeCmd = &cobra.Command{
	Use:   "uptime",
	Short: "Uptime of kronos node",
	Long:  `Returns the uptime on the kronos server listening on the given GRPC addr`,
	Run: func(cmd *cobra.Command, args []string) {
		runUptime()
	},
}

func init() {
	uptimeCmd.Flags().StringVar(
		&uptimeCtx.grpcAddr,
		"grpc-addr",
		fmt.Sprintf("localhost:%s", server.KronosDefaultGRPCPort),
		"GRPC address to use for fetching uptime of the node(s)",
	)
}

func runUptime() {
	ctx := context.Background()
	certsDir := kronosCertsDir()
	grpcClient := server.NewGRPCClient(certsDir)
	host, port, err := net.SplitHostPort(uptimeCtx.grpcAddr)
	if err != nil {
		log.Fatalf(ctx, "Invalid GRPC address: %v", err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, grpcClient)
	t, err := grpcClient.KronosUptime(ctx, &kronospb.NodeAddr{
		Host: host,
		Port: port,
	})
	if err != nil {
		log.Fatalf(ctx, "Unable to get kronos uptime: %v", err)
	}

	fmt.Println(t.Uptime)
}
