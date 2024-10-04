package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc/connectivity"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/protoutil"
	"google.golang.org/grpc"
)

var (
	nodeDescriptorPeriod = 1 * time.Second
	livenessPeriod       = 1 * time.Second
	gossipPeriod         = time.Second
	printGossipPeriod    = 1 * time.Minute
	delimiter            = "-"
)

const (
	NodeDescriptorPrefix = PrefixKey("hostPort")
	LivenessPrefix       = PrefixKey("liveness")
)

type PrefixKey string
type GossipKey string

func SetGossipPeriod(period time.Duration) {
	gossipPeriod = period
}

func SetNodeDescriptorPeriod(period time.Duration) {
	nodeDescriptorPeriod = period
}

func SetLivenessPeriod(period time.Duration) {
	livenessPeriod = period
}

func SetPrintGossipPeriod(period time.Duration) {
	printGossipPeriod = period
}

func (p PrefixKey) String() string {
	return string(p)
}

func (p PrefixKey) Encode(id string) GossipKey {
	return GossipKey(string(p) + delimiter + id)
}

func (p PrefixKey) Decode(encodedKey GossipKey) string {
	return string(encodedKey)[len(p)+len(delimiter):]
}

func (p PrefixKey) isPrefixOf(encodedKey GossipKey) bool {
	return len(p) <= len(encodedKey) && string(p) == string(encodedKey[:len(p)])
}

type peerSet map[string]struct{}

func (p peerSet) add(peer string) {
	p[peer] = struct{}{}
}
func (p peerSet) remove(peer string) {
	delete(p, peer)
}

type Callback func(*Server, GossipKey, *kronospb.Info)

// Server is the gossip server.
type Server struct {
	mu                 sync.Mutex
	advertisedHostPort string
	nodeID             string
	clusterID          string
	certsDir           string
	raftAddr           *kronospb.NodeAddr
	data               map[GossipKey]*kronospb.Info
	peers              peerSet
	nodeList           map[string]*kronospb.NodeDescriptor
	connMap            map[string]*grpc.ClientConn
	numGossip          uint64
	callBacks          map[PrefixKey][]Callback
	isBootstrapped     bool
	isRemoved          bool
}

func (g *Server) NodeLs(ctx context.Context,
	request *kronospb.NodeLsRequest) (*kronospb.NodeLsResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes := make([]*kronospb.NodeDescriptor, 0)
	for _, desc := range g.nodeList {
		nodes = append(nodes, desc)
	}
	return &kronospb.NodeLsResponse{Nodes: nodes}, nil
}

func (g *Server) RegisterCallback(prefix PrefixKey, cb Callback,
	runOnExistingKeys bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.callBacks[prefix]; !exists {
		g.callBacks[prefix] = make([]Callback, 0)
	}

	g.callBacks[prefix] = append(g.callBacks[prefix], cb)

	if !runOnExistingKeys {
		return
	}

	for k, v := range g.data {
		g.processCallbacksLocked(k, v)
	}
}

// Gossip is the RPC handler for gossiping information between nodes in the
// cluster.
func (g *Server) Gossip(ctx context.Context,
	request *kronospb.Request) (*kronospb.Response, error) {
	if request.ClusterId != g.clusterID && request.ClusterId != "" && g.
		clusterID != "" {
		return nil, errors.Errorf("cluster id mismatch: %s != %s",
			request.ClusterId, g.clusterID)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	updMap := make(map[GossipKey]*kronospb.Info)
	for k, v := range request.GossipMap {
		updMap[GossipKey(k)] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
	}
	g.updateGossipStateLocked(updMap)
	res := &kronospb.Response{}
	res.NodeId = g.nodeID
	// deep copy g.data
	res.Data = make(map[string]*kronospb.Info)
	for k, v := range g.data {
		res.Data[string(k)] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
	}
	return res, nil
}

func (g *Server) removeConnLocked(ctx context.Context, peer string) {
	if conn, ok := g.connMap[peer]; ok {
		err := conn.Close()
		if err != nil {
			log.Errorf(ctx, "Error closing connection to peer %s: %v", peer, err)
			return
		}
	}
	delete(g.connMap, peer)
}

func (g *Server) removePeerLocked(ctx context.Context, peer string) {
	g.peers.remove(peer)
	g.removeConnLocked(ctx, peer)
}

// AddPeer adds a new peer to the gossip server.
func (g *Server) addPeerLocked(ctx context.Context,
	desc *kronospb.NodeDescriptor) {
	nodeId := desc.NodeId
	grpcAddr := desc.GrpcAddr
	if nodeId == g.nodeID {
		return
	}
	g.peers.add(grpcAddr)
	if oldDesc, ok := g.nodeList[nodeId]; ok {
		if oldDesc.GrpcAddr != grpcAddr {
			g.removePeerLocked(ctx, oldDesc.GrpcAddr)
		}
	}
	g.nodeList[nodeId] = desc
	// TODO: Persist the new peer to disk.
}

func (g *Server) getPeersLocked() []string {
	peers := make([]string, 0)
	for peer, _ := range g.peers {
		peers = append(peers, peer)
	}
	return peers
}

// GetPeers Return the current list of peers.
func (g *Server) GetPeers() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getPeersLocked()
}

// GetNodeList returns the current list of nodes.
func (g *Server) GetNodeList() []*kronospb.NodeDescriptor {
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes := make([]*kronospb.NodeDescriptor, 0)
	for _, desc := range g.nodeList {
		nodes = append(nodes, desc)
	}
	return nodes
}

// SetInfo sets the value for the given key.
func (g *Server) SetInfo(key GossipKey, value []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.updateGossipStateLocked(map[GossipKey]*kronospb.Info{
		key: {Data: value, Timestamp: time.Now().UnixNano()},
	})
}

func (g *Server) SetNodeID(ctx context.Context, nodeID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.nodeID != "" {
		log.Warningf(ctx,
			"Node ID being changed from %s to %s", g.nodeID, nodeID)
	}
	g.nodeID = nodeID
}

func (g *Server) SetClusterID(ctx context.Context, clusterID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.clusterID != "" {
		log.Warningf(ctx,
			"Cluster ID being changed from %s to %s", g.clusterID, clusterID)
	}
	log.Infof(ctx, "Setting cluster ID to %s", clusterID)
	g.clusterID = clusterID
}

func (g *Server) SetBootstrapped(ctx context.Context, isBootstrapped bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.isRemoved && isBootstrapped {
		log.Fatalf(ctx, "Cannot set bootstrapped on a removed node")
	}
	log.Infof(ctx, "Setting bootstrapped to %v", isBootstrapped)
	g.isBootstrapped = isBootstrapped
}

func (g *Server) SetRemoved(ctx context.Context, isRemoved bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isBootstrapped && isRemoved {
		log.Fatalf(ctx, "Cannot set removed on a non-bootstrapped node")
	}
	g.isRemoved = isRemoved
}

func (g *Server) gossip(ctx context.Context) {
	peerList := g.GetPeers()
	if len(peerList) == 0 {
		if log.V(1) {
			log.Infof(ctx, "No peers to gossip with")
		}
		return
	}
	for _, peer := range peerList {
		func() {
			var conn *grpc.ClientConn
			if _, ok := g.connMap[peer]; ok {
				conn = g.connMap[peer]
			}
			if conn == nil || conn.GetState() == connectivity.Shutdown || conn.GetState() == connectivity.TransientFailure {
				if conn != nil {
					conn.Close()
				}
				var dialOpts grpc.DialOption
				if g.certsDir == "" {
					dialOpts = grpc.WithInsecure()
				} else {
					creds, err := kronosutil.SSLCreds(g.certsDir)
					if err != nil {
						log.Infof(ctx, "Error creating SSL creds: %v", err)
						return
					}
					dialOpts = grpc.WithTransportCredentials(creds)
				}
				var err error
				conn, err = grpc.Dial(peer, dialOpts)
				if err != nil {
					log.Errorf(ctx, "Error dialing peer %s: %v", peer, err)
					return
				}
				g.connMap[peer] = conn
			}
			client := kronospb.NewGossipClient(conn)
			// TODO: make this timeout configurable.
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			g.mu.Lock()
			gossipMap := make(map[string]*kronospb.Info)
			// deep copy g.data
			for k, v := range g.data {
				gossipMap[string(k)] = &kronospb.Info{Data: v.Data,
					Timestamp: v.Timestamp}
			}
			req := &kronospb.Request{
				ClusterId:          g.clusterID,
				GossipMap:          gossipMap,
				NodeId:             g.nodeID,
				AdvertisedHostPort: g.advertisedHostPort,
			}
			g.mu.Unlock()
			res, err := client.Gossip(ctx, req)
			if err != nil {
				log.Errorf(ctx, "error gossiping with peer %s: %v", peer, err)
				return
			}
			updMap := make(map[GossipKey]*kronospb.Info)
			for k, v := range res.Data {
				updMap[GossipKey(k)] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
			}
			g.updateGossipState(updMap)
		}()
	}
}

func (g *Server) processCallbacksLocked(key GossipKey, info *kronospb.Info) {
	for prefix, cbs := range g.callBacks {
		if prefix.isPrefixOf(key) {
			for _, cb := range cbs {
				cb(g, key, info)
			}
		}
	}
}

func (g *Server) updateGossipStateLocked(gossipMap map[GossipKey]*kronospb.Info) {
	g.numGossip++
	for k, v := range gossipMap {
		if cur, ok := g.data[k]; !ok || cur.Timestamp < v.Timestamp {
			g.processCallbacksLocked(k, v)
			g.data[k] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
		}
	}
}

func (g *Server) updateGossipState(gossipMap map[GossipKey]*kronospb.Info) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.updateGossipStateLocked(gossipMap)
}

func runPeriodically(stopCh chan struct{}, period time.Duration, f func()) {
	ticker := time.NewTicker(period)
	for {
		f()
		select {
		case <-stopCh:
			f() // Run one last time before returning to take care of removal.
			return
		case <-ticker.C:
		}
	}
}

func (g *Server) periodicPrintGossipData(ctx context.Context,
	stopCh chan struct{}) {
	f := func() {
		logMsg := ""
		err := func() error {
			g.mu.Lock()
			defer g.mu.Unlock()
			logMsg += "Gossip Info\n"
			logMsg += fmt.Sprintf("peers : %v\n", g.getPeersLocked())
			jsonData, err := json.Marshal(g.data)
			if err != nil {
				log.Errorf(ctx, "Error marshalling gossip data: %v", err)
				return err
			}
			logMsg += fmt.Sprintf("data : %s\n", string(jsonData))
			return nil
		}()
		if err != nil {
			return
		}
		log.Infof(ctx, "%s", logMsg)
	}
	runPeriodically(stopCh, printGossipPeriod, f)
}

// NewServer creates a new gossip server.
func NewServer(advertisedHostPort string,
	raftAddr *kronospb.NodeAddr, peers []string, certsDir string) *Server {
	s := &Server{
		advertisedHostPort: advertisedHostPort,
		raftAddr:           raftAddr,
		certsDir:           certsDir,
		peers:              make(peerSet),
		data:               make(map[GossipKey]*kronospb.Info),
		nodeList:           make(map[string]*kronospb.NodeDescriptor),
		connMap:            make(map[string]*grpc.ClientConn),
		callBacks:          make(map[PrefixKey][]Callback),
	}
	// Remove self from peers
	for _, p := range peers {
		if p != advertisedHostPort {
			s.peers.add(p)
		}
	}

	s.RegisterCallback(NodeDescriptorPrefix, func(g *Server, k GossipKey, i *kronospb.Info) {
		var desc kronospb.NodeDescriptor
		err := protoutil.Unmarshal(i.Data, &desc)
		ctx := context.Background()
		if err != nil {
			log.Errorf(ctx, "Error unmarshalling node descriptor: %v", err)
			return
		}
		if NodeDescriptorPrefix.Decode(k) != desc.NodeId {
			log.Errorf(ctx, "Node ID mismatch: %s != %s",
				NodeDescriptorPrefix.Decode(k), desc.NodeId)
			return
		}
		g.addPeerLocked(ctx, &desc)
	}, false)

	return s
}

// Start starts the gossip server.
func (g *Server) Start(ctx context.Context, stopCh chan struct{}) {
	go g.periodicPrintGossipData(ctx, stopCh)
	// Periodically gossip with other nodes in the cluster.
	f := func() {
		g.gossip(ctx)
	}
	runPeriodically(stopCh, gossipPeriod, f)
	// Clean up conns when we are shutting down.
	peers := g.GetPeers()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, peer := range peers {
		g.removeConnLocked(ctx, peer)
	}
}

// Liveness periodically gossips liveness information to other nodes in the
// cluster.
func Liveness(ctx context.Context, g *Server, stopCh chan struct{}) {
	f := func() {
		g.SetInfo(LivenessPrefix.Encode(g.nodeID), []byte("live"))
	}
	runPeriodically(stopCh, livenessPeriod, f)
}

// NodeDescriptor periodically gossips node descriptor information to other
// nodes in the cluster.
func NodeDescriptor(ctx context.Context, g *Server, stopCh chan struct{}) {
	f := func() {
		secure := g.certsDir != ""
		url := kronosutil.AddrToURL(g.raftAddr, secure)
		g.mu.Lock()
		desc := &kronospb.NodeDescriptor{
			NodeId:         g.nodeID,
			RaftAddr:       url.String(),
			GrpcAddr:       g.advertisedHostPort,
			IsBootstrapped: g.isBootstrapped,
			IsRemoved:      g.isRemoved,
			LastHeartbeat:  time.Now().UnixNano(),
			ClusterId:      g.clusterID,
		}
		g.nodeList[g.nodeID] = desc
		g.mu.Unlock()
		descBytes, err := protoutil.Marshal(desc)
		if err != nil {
			log.Errorf(ctx,
				"Error marshalling node descriptor : %v", err)
			return
		}
		g.SetInfo(NodeDescriptorPrefix.Encode(g.nodeID), descBytes)
	}
	runPeriodically(stopCh, nodeDescriptorPeriod, f)
}

func (g *Server) WaitForNRoundsofGossip(timeout time.Duration, n uint64) {
	// no need to lock here as numGossip is only written in the gossip
	// function.
	startGossip := g.numGossip
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		if g.numGossip-startGossip >= n {
			return
		}
		time.Sleep(gossipPeriod)
	}
}

func (g *Server) RemovePeer(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	node := g.nodeList[id]
	if node != nil {
		g.removePeerLocked(context.Background(), node.GrpcAddr)
	}
}

func (g *Server) GetNodeDesc(id string) *kronospb.NodeDescriptor {
	g.mu.Lock()
	defer g.mu.Unlock()
	desc, ok := g.nodeList[id]
	if !ok {
		return nil
	}
	return protoutil.Clone(desc).(*kronospb.NodeDescriptor)
}

func IsNodeLive(desc *kronospb.NodeDescriptor) bool {
	return desc != nil && desc.IsRemoved == false && desc.
		LastHeartbeat > time.Now().Add(-5*nodeDescriptorPeriod).UnixNano()
}
