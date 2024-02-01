package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rubrikinc/kronos/kronosutil/log"
	kronospb "github.com/rubrikinc/kronos/pb"
	"google.golang.org/grpc"
)

var (
	nodeDescriptorPrefix = "hostPort-"
	livenessPrefix       = "liveness-"
	nodeDescriptorPeriod = 10 * time.Minute
	livenessPeriod       = 1 * time.Second
	gossipPeriod         = time.Second
	printGossipPeriod    = time.Second
)

var gossipCallBacks map[string]func(*Server, *kronospb.Info)

func init() {
	gossipCallBacks = make(map[string]func(*Server, *kronospb.Info))
	gossipCallBacks[nodeDescriptorPrefix] = func(g *Server, info *kronospb.Info) {
		g.addPeerLocked(string(info.Data))
	}
}

// Server is the gossip server.
type Server struct {
	mu                 sync.Mutex
	advertisedHostPort string
	nodeID             string
	clusterID          string
	data               map[string]*kronospb.Info
	knownPeerHostPort  map[string]struct{}
}

// Gossip is the RPC handler for gossiping information between nodes in the
// cluster.
func (g *Server) Gossip(ctx context.Context,
	request *kronospb.Request) (*kronospb.Response, error) {
	if request.ClusterId != g.clusterID {
		return nil, errors.Errorf("cluster id mismatch: %s != %s",
			request.ClusterId, g.clusterID)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.updateGossipStateLocked(request.GossipMap)
	res := &kronospb.Response{}
	res.NodeId = g.nodeID
	// deep copy g.data
	res.Data = make(map[string]*kronospb.Info)
	for k, v := range g.data {
		res.Data[k] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
	}
	return res, nil
}

// AddPeer adds a new peer to the gossip server.
func (g *Server) addPeerLocked(addr string) {
	if _, err := netip.ParseAddrPort(addr); err != nil {
		// not a valid IP
		return
	}
	if g.advertisedHostPort == addr {
		return
	}
	log.Infof(context.Background(), "Adding peer %s", addr)
	g.knownPeerHostPort[addr] = struct{}{}
	// TODO: Persist the new peer to disk.
}

// GetPeers Return the current list of peers.
func (g *Server) GetPeers() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	peers := make([]string, 0)
	for peer := range g.knownPeerHostPort {
		peers = append(peers, peer)
	}
	return peers
}

// SetInfo sets the value for the given key.
func (g *Server) SetInfo(key string, value []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.updateGossipStateLocked(map[string]*kronospb.Info{
		key: {Data: value, Timestamp: time.Now().UnixNano()},
	})
}

func (g *Server) SetNodeID(nodeID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.nodeID != "" {
		log.Warningf(context.Background(),
			"Node ID being changed from %s to %s", g.nodeID, nodeID)
	}
	g.nodeID = nodeID
}

func (g *Server) SetClusterID(clusterID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.clusterID != "" {
		log.Warningf(context.Background(),
			"Cluster ID being changed from %s to %s", g.clusterID, clusterID)
	}
	g.clusterID = clusterID
}

func (g *Server) gossip(ctx context.Context) {
	peerList := g.GetPeers()
	if len(peerList) == 0 {
		log.Infof(ctx, "No peers to gossip with")
		return
	}
	for _, peer := range peerList {
		func() {
			// TODO: remove insecure
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Errorf(ctx, "Error dialing peer %s: %v", peer, err)
				return
			}
			defer conn.Close()
			client := kronospb.NewGossipClient(conn)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			g.mu.Lock()
			gossipMap := make(map[string]*kronospb.Info)
			// deep copy g.data
			for k, v := range g.data {
				gossipMap[k] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
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
			g.updateGossipState(res.Data)
		}()
	}
}

func (g *Server) processCallback(key string, info *kronospb.Info) {
	for prefix, f := range gossipCallBacks {
		if len(prefix) <= len(key) && prefix == key[:len(prefix)] {
			f(g, info)
		}
	}
}

func (g *Server) updateGossipStateLocked(gossipMap map[string]*kronospb.Info) {
	for k, v := range gossipMap {
		if cur, ok := g.data[k]; !ok || cur.Timestamp < v.Timestamp {
			g.processCallback(k, v)
			g.data[k] = &kronospb.Info{Data: v.Data, Timestamp: v.Timestamp}
		}
	}
}

func (g *Server) updateGossipState(gossipMap map[string]*kronospb.Info) {
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
			logMsg += fmt.Sprintf("peers : %v\n", g.knownPeerHostPort)
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
func NewServer(advertisedHostPort string, peers []string) *Server {
	// remove addr from peers
	s := &Server{
		data:               make(map[string]*kronospb.Info),
		advertisedHostPort: advertisedHostPort,
		knownPeerHostPort:  make(map[string]struct{}),
	}
	// Remove self from peers
	for _, p := range peers {
		if p != advertisedHostPort {
			s.knownPeerHostPort[p] = struct{}{}
		}
	}
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
}

// Liveness periodically gossips liveness information to other nodes in the
// cluster.
func Liveness(g *Server, stopCh chan struct{}) {
	f := func() {
		g.SetInfo(livenessPrefix+g.nodeID, []byte("live"))
	}
	runPeriodically(stopCh, livenessPeriod, f)
}

// NodeDescriptor periodically gossips node descriptor information to other
// nodes in the cluster.
func NodeDescriptor(g *Server, stopCh chan struct{}) {
	f := func() {
		g.SetInfo(nodeDescriptorPrefix+g.nodeID, []byte(g.advertisedHostPort))
	}
	runPeriodically(stopCh, nodeDescriptorPeriod, f)
}
