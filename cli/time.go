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

var timeCtx struct {
	grpcAddr string
}

var timeCmd = &cobra.Command{
	Use:   "time",
	Short: "Time of kronos node",
	Long:  `Returns the time on the kronos server listening on the given GRPC addr`,
	Run: func(cmd *cobra.Command, args []string) {
		runTime()
	},
}

func init() {
	timeCmd.Flags().StringVar(
		&timeCtx.grpcAddr,
		"grpc-addr",
		fmt.Sprintf("localhost:%s", server.KronosDefaultGRPCPort),
		"GRPC address to use for fetching time of the node(s)",
	)
}

func runTime() {
	ctx := context.Background()
	certsDir := kronosCertsDir()
	grpcClient := server.NewGRPCClient(certsDir)
	host, port, err := net.SplitHostPort(timeCtx.grpcAddr)
	if err != nil {
		log.Fatalf(ctx, "Invalid GRPC address: %v", err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, grpcClient)
	t, err := grpcClient.KronosTime(ctx, &kronospb.NodeAddr{
		Host: host,
		Port: port,
	})
	if err != nil {
		log.Fatal(ctx, "Unable to get kronos time: %v", err)
	}

	fmt.Println(t.Time)
}
