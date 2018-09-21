package cli

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

var removeCtx struct {
	host    string
	timeout time.Duration
}

var clusterRemoveCmd = &cobra.Command{
	Use:   "remove [<nodeID1> <nodeID2>...] [flags]",
	Short: "Remove node(s) from kronos cluster",
	Long: `
Remove node(s) from the raft cluster of kronos.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runRemove(args)
	},
}

func init() {
	clusterRemoveCmd.Flags().StringVar(
		&removeCtx.host,
		"host",
		"localhost:5766",
		"host to run decommission through (advertiseHost:raftPort)",
	)

	clusterRemoveCmd.Flags().DurationVar(
		&removeCtx.timeout,
		timeoutFlag,
		30*time.Second,
		"timeout for the command",
	)
}

func runRemove(args []string) {
	ctx := context.Background()
	if removeCtx.timeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, removeCtx.timeout)
		defer cancel()
	}
	raftAddr, err := kronosutil.NodeAddr(removeCtx.host)
	if err != nil {
		log.Fatalf(
			ctx,
			"Failed to split host and port from host %s", removeCtx.host,
		)
	}
	tlsInfo := kronosutil.TLSInfo(kronosCertsDir())
	c, err := kronoshttp.NewClusterClient(
		raftAddr,
		tlsInfo,
	)
	if err != nil {
		log.Fatalf(ctx, "Failed to create cluster_client, error: %v", err)
	}
	defer c.Close()
	for _, nodeID := range args {
		err = c.RemoveNode(ctx, &kronoshttp.RemoveNodeRequest{NodeID: nodeID})
		if err != nil {
			log.Fatalf(ctx, "Failed to remove node, error: %v", err)
		}
	}
}
