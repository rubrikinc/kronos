package cli

import (
	"context"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

var hostsCtx struct {
	raftAddr string
	local    bool
}

func addHostsFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&hostsCtx.raftAddr,
		"raft-addr",
		fmt.Sprintf("localhost:%s", server.KronosDefaultRaftPort),
		"Raft address to use for fetching metadata of the node(s)",
	)
	cmd.Flags().BoolVar(
		&hostsCtx.local,
		"local",
		false,
		"Denotes whether command should be run only for local node.",
	)
}

// fetchRaftNodes returns slice of kronoshttp.Node fetched from
// nodes endpoint of hostsCtx.raftAddr. Only node of hostsCtx.raftAddr is
// returned if hostsCtx.local is set.
func fetchRaftNodes(ctx context.Context) ([]kronoshttp.Node, error) {
	var raftAddr *kronospb.NodeAddr
	raftAddr, err := kronosutil.NodeAddr(hostsCtx.raftAddr)
	if err != nil {
		return nil, err
	}
	tlsInfo := kronosutil.TLSInfo(kronosCertsDir())

	client, err := kronoshttp.NewClusterClient(raftAddr, tlsInfo)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	nodes, err := client.Nodes(ctx)
	if err != nil {
		return nil, err
	}
	if hostsCtx.local {
		for _, node := range nodes {
			if proto.Equal(raftAddr, node.RaftAddr) {
				return []kronoshttp.Node{node}, nil
			}
		}
	}
	return nodes, nil
}
