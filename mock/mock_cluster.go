package mock

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/kronosstats"
	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/oracle"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
	"github.com/rubrikinc/kronos/tm"
)

// Node is used by Cluster in tests
type Node struct {
	Server                 *server.Server
	Clock                  *tm.ManualClock
	manageOracleTickCh     chan<- time.Time
	manageOracleTickDoneCh <-chan struct{}
}

// Cluster is an in memory Kronos cluster used in tests
type Cluster struct {
	addrs        []kronospb.NodeAddr
	Nodes        map[string]*Node
	StateMachine oracle.StateMachine
	Client       *Client
}

// TickN executes Tick n times.
func (tc *Cluster) TickN(testNode *Node, n int) {
	for i := 0; i < n; i++ {
		tc.Tick(testNode)
	}
}

// Tick executes a Tick on the given Node and returns after
// the Tick is processed
func (tc *Cluster) Tick(testNode *Node) {
	server := testNode.Server.GRPCAddr.String()
	tc.Nodes[server].manageOracleTickCh <- time.Now().UTC()
	<-tc.Nodes[server].manageOracleTickDoneCh
}

// Node returns the Node given the Node index
func (tc *Cluster) Node(idx int) *Node {
	if idx < len(tc.addrs) {
		return tc.Nodes[tc.addrs[idx].String()]
	}

	return nil
}

// Stop stops the all the kronos servers
func (tc *Cluster) Stop() {
	for _, addr := range tc.addrs {
		if node, ok := tc.Nodes[addr.String()]; ok {
			node.Server.Stop()
		}
	}
}

// IsClusterInSync returns whether kronos time is in sync on all the Nodes
// of the cluster
func (tc *Cluster) IsClusterInSync(ctx context.Context, nodes ...*Node) error {
	if len(nodes) == 0 {
		log.Fatal(ctx, "no Nodes passed")
	}

	maxTm := int64(math.MinInt64)
	minTm := int64(math.MaxInt64)
	for _, node := range nodes {
		kt, err := node.Server.KronosTimeNow(ctx)
		kronosTime := kt.Time
		if err != nil {
			return err
		}
		if maxTm < kronosTime {
			maxTm = kronosTime
		}
		if minTm > kronosTime {
			minTm = kronosTime
		}
	}

	timeDiff := maxTm - minTm
	tolerance := int64(10 * time.Millisecond)
	if timeDiff > tolerance {
		return fmt.Errorf(
			"time difference too high. Expected %v < %v",
			timeDiff,
			tolerance,
		)
	}
	return nil
}

// addNode creates a test kronos Node. It returns the created Node
func (tc *Cluster) addNode(idx int, isNew bool) *Node {
	addr := kronospb.NodeAddr{
		Host: fmt.Sprintf("test_%d", idx),
		Port: strconv.Itoa(idx),
	}
	if isNew {
		tc.addrs = append(tc.addrs, addr)
	} else {
		if node, ok := tc.Nodes[tc.addrs[idx].String()]; ok {
			node.Server.Stop()
		}
	}

	clock := tm.NewManualClock()
	server := NewServerForTest(
		&addr,
		clock,
		tc.StateMachine,
		tc.Client,
		15*time.Second,
	)
	tickerCh := make(chan time.Time)
	tickDoneCh := make(chan struct{})
	tickCallback := func() {
		tickDoneCh <- struct{}{}
	}
	go server.ManageOracle(
		tickerCh,
		tickCallback,
	)

	newNode := &Node{
		Server:                 server,
		Clock:                  clock,
		manageOracleTickCh:     tickerCh,
		manageOracleTickDoneCh: tickDoneCh,
	}
	tc.Nodes[addr.String()] = newNode

	return newNode
}

// StopNode stops the given kronos Node
func (tc *Cluster) StopNode(ctx context.Context, testNode *Node) {
	nodeIdx := -1
	for idx, addr := range tc.addrs {
		if proto.Equal(&addr, testNode.Server.GRPCAddr) {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		log.Fatalf(ctx, "Node %v not found", testNode.Server.GRPCAddr)
	}

	nodeAddr := tc.addrs[nodeIdx].String()
	tc.Nodes[nodeAddr].Server.Stop()
	delete(tc.Nodes, nodeAddr)
}

// RestartNode replaces the given Node with a new Node
func (tc *Cluster) RestartNode(ctx context.Context, testNode *Node) *Node {
	nodeIdx := -1
	for idx, addr := range tc.addrs {
		if proto.Equal(&addr, testNode.Server.GRPCAddr) {
			nodeIdx = idx
			break
		}
	}
	if nodeIdx == -1 {
		log.Fatalf(ctx, "Node %v not found", testNode.Server.GRPCAddr)
	}

	return tc.addNode(nodeIdx, false /* isNew */)
}

// NewKronosCluster returns a new test kronos cluster
func NewKronosCluster(numNodes int) *Cluster {
	nodes := make(map[string]*Node)
	addrs := []kronospb.NodeAddr{}
	client := newInMemClient(nodes).(*Client)
	stateMachine := oracle.NewMemStateMachine()
	cluster := &Cluster{
		addrs:        addrs,
		Nodes:        nodes,
		StateMachine: stateMachine,
		Client:       client,
	}
	for i := 0; i < numNodes; i++ {
		cluster.addNode(i, true /* isNew */)
	}

	return cluster
}

// InitializeCluster creates a cluster and sets a different time on each Node
func InitializeCluster(a *assert.Assertions, numNodes int) (cluster *Cluster, nodes []*Node) {
	cluster = NewKronosCluster(numNodes)
	for i := 0; i < numNodes; i++ {
		node := cluster.Node(i)
		a.NotNil(node)
		node.Clock.SetTime(int64(i+1) * int64(time.Hour))
		nodes = append(nodes, node)
	}
	return
}

// NewServerForTest returns a Server which can be used in unit tests
func NewServerForTest(
	address *kronospb.NodeAddr,
	clock *tm.ManualClock,
	oracle oracle.StateMachine,
	client server.Client,
	oracleTimeCapDelta time.Duration,
) *server.Server {
	return &server.Server{
		Clock:              clock,
		OracleSM:           oracle,
		Client:             client,
		GRPCAddr:           address,
		StopC:              make(chan struct{}),
		OracleTimeCapDelta: oracleTimeCapDelta,
		Metrics:            kronosstats.NewMetrics(),
	}
}
