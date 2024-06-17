package cli

import (
	"bytes"
	"context"
	"sync"

	"github.com/rubrikinc/kronos/kronoshttp"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
)

type errMap map[string]error

func (e errMap) Error() string {
	var b bytes.Buffer
	for tag, err := range e {
		b.WriteString(tag + ":" + err.Error())
	}
	return b.String()
}

const (
	grpcAddrErrTag = "grpc-addr"
	statusErrTag   = "status"
	timeErrTag     = "time"
	uptimeErrTag   = "uptime"
)

// NodeInfo stores the information for a node required by cli commands like
// status, validate and any errors that occurred while fetching it.
type NodeInfo struct {
	ID             string                `json:"id"`
	RaftAddr       *kronospb.NodeAddr    `json:"raft_addr"`
	GRPCAddr       *kronospb.NodeAddr    `json:"grpc_addr"`
	ServerStatus   kronospb.ServerStatus `json:"server_status"`
	OracleState    *kronospb.OracleState `json:"oracle_state"`
	Delta          int64                 `json:"delta"`
	Time           int64                 `json:"time"`
	Uptime         int64                 `json:"uptime"`
	Err            errMap                `json:"error"`
	RaftLeader     bool                  `json:"raft_leader"`
	RaftTerm       uint64                `json:"raft_term"`
	AppliedIndex   uint64                `json:"applied_index"`
	CommittedIndex uint64                `json:"committed_index"`
}

func (n *NodeInfo) addError(err error, tag string) {
	if n.Err == nil {
		n.Err = make(errMap)
	}
	n.Err[tag] = err
}

func (n *NodeInfo) getError(tag string) error {
	if n.Err == nil {
		return nil
	}
	return n.Err[tag]
}

// nodeInfoFetcher contains fields which can be set to true to fetch them in
// fetch method. grpcAddr is always fetched.
type nodeInfoFetcher struct {
	time   bool // whether to fetch time
	status bool // whether to fetch status
}

// fetch fetches the information about nodes which is asked in n
func (n nodeInfoFetcher) fetch(
	ctx context.Context, nodesIncludingRemoved []kronoshttp.Node,
) []*NodeInfo {
	var activeNodes []kronoshttp.Node
	for _, node := range nodesIncludingRemoved {
		if !node.IsRemoved {
			activeNodes = append(activeNodes, node)
		}
	}
	infoStores := make([]*NodeInfo, len(activeNodes))
	var wg sync.WaitGroup
	certsDir := kronosCertsDir()

	tlsInfo := kronosutil.TLSInfo(kronosCertsDir())

	for i, node := range activeNodes {
		infoStores[i] = &NodeInfo{
			ID:       node.NodeID,
			RaftAddr: node.RaftAddr,
		}
		wg.Add(1)
		go func(ndInfo *NodeInfo) {
			defer wg.Done()

			client, err := kronoshttp.NewClusterClient(ndInfo.RaftAddr, tlsInfo)
			if err != nil {
				ndInfo.addError(err, grpcAddrErrTag)
				return
			}
			defer client.Close()
			raftStatus, err := client.RaftStatus(ctx)
			if err != nil {
				ndInfo.addError(err, statusErrTag)
				return
			}

			ndInfo.RaftLeader = raftStatus.RaftLeader
			ndInfo.RaftTerm = raftStatus.RaftTerm
			ndInfo.AppliedIndex = raftStatus.AppliedIndex
			ndInfo.CommittedIndex = raftStatus.CommittedIndex

			grpcAddr, err := fetchGRPCAddr(ctx, ndInfo.RaftAddr, certsDir)
			if err != nil {
				ndInfo.addError(err, grpcAddrErrTag)
				return
			}
			ndInfo.GRPCAddr = grpcAddr
			grpcClient := server.NewGRPCClient(certsDir)
			defer kronosutil.CloseWithErrorLog(ctx, grpcClient)
			if n.time {
				tr, err := grpcClient.KronosTime(ctx, grpcAddr)
				if err != nil {
					ndInfo.addError(err, timeErrTag)
				} else {
					ndInfo.Time = tr.Time
				}
				ut, err := grpcClient.KronosUptime(ctx, grpcAddr)
				if err != nil {
					ndInfo.addError(err, uptimeErrTag)
				} else {
					ndInfo.Uptime = ut.Uptime
				}
			}
			if n.status {
				st, err := grpcClient.Status(ctx, grpcAddr)
				if err != nil {
					ndInfo.addError(err, statusErrTag)
				} else {
					ndInfo.ServerStatus = st.ServerStatus
					ndInfo.OracleState = st.OracleState
					ndInfo.Delta = st.Delta
				}
			}
		}(infoStores[i])
	}
	wg.Wait()
	return infoStores
}

func fetchGRPCAddr(
	ctx context.Context, raftAddr *kronospb.NodeAddr, certsDir string,
) (*kronospb.NodeAddr, error) {
	tlsInfo := kronosutil.TLSInfo(certsDir)
	client, err := kronoshttp.NewClusterClient(raftAddr, tlsInfo)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	return client.GRPCAddr(ctx)
}
