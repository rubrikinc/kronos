package metadata

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
	"github.com/scaledata/etcd/pkg/fileutil"

	"github.com/rubrikinc/kronos/checksumfile"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
)

const (
	// clusterInfoFileName is the name of the file in which cluster information is
	// persisted. This needs to be exported because it is also accessed by update_addr
	// and cluster_info subcommands.
	clusterInfoFileName = "cluster_info"
	lockFileName        = "cluster_info.LOCK"
)

// ErrNoClusterFile is returned when the cluster file does not exist when trying
// to load an existing cluster.
var ErrNoClusterFile = errors.New("cluster file does not exist")

// NewClusterProto returns an initialized cluster proto.
func NewClusterProto() *kronospb.Cluster {
	return &kronospb.Cluster{AllNodes: make(map[string]*kronospb.Node)}
}

// Cluster is a wrapper around *kronospb.Cluster so that methods can be defined
// on it. It is thread-safe and it creates a lock file in dataDir when
// readOnly is false, so that at max one instance of Cluster with write access
// can exist at a time for a dataDir across all processes/threads.
type Cluster struct {
	mu struct {
		syncutil.RWMutex
		cluster *kronospb.Cluster
	}
	filename string
	readOnly bool
	lockFile *fileutil.LockedFile
}

// activeNodesLocked returns the nodes which are an active part of the cluster.
// This excludes the nodes which have been removed from the cluster.
// This function expects a read lock to be held on c.mu
func (c *Cluster) activeNodesLocked() map[string]*kronospb.Node {
	activeNodes := make(map[string]*kronospb.Node)
	for id, node := range c.mu.cluster.AllNodes {
		if !node.IsRemoved {
			activeNodes[id] = protoutil.Clone(node).(*kronospb.Node)
		}
	}

	return activeNodes
}

// ActiveNodes returns the nodes which are an active part of the cluster.
// This excludes the nodes which have been removed from the cluster.
func (c *Cluster) ActiveNodes() map[string]*kronospb.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.activeNodesLocked()
}

// NodesIncludingRemoved returns all the nodes which have been a part of this cluster.
// This includes the nodes which have been removed from the cluster.
func (c *Cluster) NodesIncludingRemoved() map[string]*kronospb.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Create a deep copy.
	allNodes := make(map[string]*kronospb.Node)
	for id, node := range c.mu.cluster.AllNodes {
		allNodes[id] = protoutil.Clone(node).(*kronospb.Node)
	}

	return allNodes
}

// Persist persists c to file using checksumfile. It fails if cluster was loaded
// in readOnly mode.
func (c *Cluster) Persist() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.readOnly {
		return errors.New("cluster opened in readOnly mode")
	}
	data, err := protoutil.Marshal(c.mu.cluster)
	if err != nil {
		return err
	}
	return checksumfile.Write(c.filename, data)
}

// NewCluster creates a new Cluster initialized from cluster. If cluster is nil,
// an empty cluster is created. The returned cluster is persistable, but not
// persisted yet. It needs to be explicitly persisted if required.
func NewCluster(dataDir string, cluster *kronospb.Cluster) (*Cluster, error) {
	c, err := emptyCluster(dataDir, false /* readOnly */)
	if err != nil {
		return nil, err
	}
	if cluster != nil && len(cluster.AllNodes) > 0 {
		c.mu.cluster = protoutil.Clone(cluster).(*kronospb.Cluster)
	}
	return c, nil
}

// ClusterInfoFilename returns the name of the file which stores cluster info.
func ClusterInfoFilename(dataDir string) string {
	return filepath.Join(dataDir, clusterInfoFileName)
}

// emptyCluster creates a Cluster without populating metadata.
// An flock is acquired if readOnly is false to ensure a single writer
// to the metadata directory.
func emptyCluster(dataDir string, readOnly bool) (*Cluster, error) {
	c := &Cluster{
		filename: ClusterInfoFilename(dataDir),
		readOnly: readOnly,
	}
	c.mu.cluster = NewClusterProto()
	if !c.readOnly {
		lockFile, err := fileutil.TryLockFile(
			filepath.Join(dataDir, lockFileName),
			os.O_CREATE|os.O_RDWR|os.O_TRUNC,
			fileutil.PrivateFileMode,
		)
		if err != nil {
			return nil, err
		}
		c.lockFile = lockFile
	}
	return c, nil
}

// LoadCluster loads the persisted Cluster from dataDir if the cluster info file
// exists inside it. If no cluster info file exists in dataDir, it returns
// ErrNoClusterFile. If readOnly is true, then the returned Cluster is only good
// for reading, and trying to persist it will return an error.
func LoadCluster(dataDir string, readOnly bool) (*Cluster, error) {
	if _, err := os.Stat(ClusterInfoFilename(dataDir)); os.IsNotExist(err) {
		return nil, ErrNoClusterFile
	}
	c, err := emptyCluster(dataDir, readOnly)
	if err != nil {
		return nil, err
	}
	data, err := checksumfile.Read(c.filename)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if err := protoutil.Unmarshal(data, c.mu.cluster); err != nil {
		_ = c.lockFile.Close()
		return nil, err
	}
	return c, nil
}

// Node returns the node with the given ID from the cluster metadata.
// If the node does not exist, ok will be false.
func (c *Cluster) Node(id string) (node *kronospb.Node, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	n, ok := c.mu.cluster.AllNodes[id]
	return protoutil.Clone(n).(*kronospb.Node), ok
}

// AddNode adds a new node with given ID/address. It returns an error if a node
// with this ID already exists.
func (c *Cluster) AddNode(id string, addr *kronospb.NodeAddr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.mu.cluster.AllNodes[id]; ok {
		return errors.Errorf("node %s already exists", id)
	}

	c.mu.cluster.AllNodes[id] = &kronospb.Node{
		RaftAddr: protoutil.Clone(addr).(*kronospb.NodeAddr),
	}
	return nil
}

// RemoveNode sets nodeID as removed. It creates the node and marks it as
// removed if it doesn't already exist in metadata. This function is idempotent
// and it is OK to remove an already removed node.
func (c *Cluster) RemoveNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.mu.cluster.AllNodes[nodeID]
	if !ok {
		c.mu.cluster.AllNodes[nodeID] = &kronospb.Node{}
	}
	c.mu.cluster.AllNodes[nodeID].IsRemoved = true
}

// UpdateAddrs updates raft addresses of nodes in c.
// oldToNewAddr should contain mapping of old to new addresses.
// If withPorts is true, kronos raft ports are also updated, otherwise only
// hosts are updated. Both old and new addresses must be present in 'host:port'
// format in oldToNewAddr if this is set to true. If withPorts is false,
// oldToNewAddr should contain only hosts and no ports.
func (c *Cluster) UpdateAddrs(oldToNewAddr map[string]string, withPorts bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// IPs of active nodes have to be changed
	for id, nd := range c.activeNodesLocked() {
		if withPorts {
			key := nd.RaftAddr.Host + ":" + nd.RaftAddr.Port
			newAddr, ok := oldToNewAddr[key]
			if !ok {
				return errors.Errorf("no mapping exists for %s", key)
			}
			raftAddr, err := kronosutil.NodeAddr(newAddr)
			if err != nil {
				return err
			}
			c.mu.cluster.AllNodes[id].RaftAddr = raftAddr
		} else {
			key := nd.RaftAddr.Host
			newAddr, ok := oldToNewAddr[key]
			if !ok {
				return errors.Errorf("no mapping exists for %s", key)
			}
			c.mu.cluster.AllNodes[id].RaftAddr = &kronospb.NodeAddr{Host: newAddr, Port: nd.RaftAddr.Port}
		}
	}
	return nil
}

// PrettyPrint returns a string which is a table having node ids, addresses and
// whether the node has been removed for nodes stored in c.
func (c *Cluster) PrettyPrint() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var b bytes.Buffer
	tw := tabwriter.NewWriter(
		&b,
		2,   /* minWidth */
		2,   /* tabWidth */
		2,   /* padding */
		' ', /* padChar */
		0,   /* flags */
	)
	ids := make([]string, 0, len(c.mu.cluster.AllNodes))
	for k := range c.mu.cluster.AllNodes {
		ids = append(ids, k)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	const format = "%s\t%s\t%v\n"
	fmt.Fprintf(tw, fmt.Sprintf(format, "ID", "Address", "IsRemoved"))
	for _, id := range ids {
		node := c.mu.cluster.AllNodes[id]
		fmt.Fprintf(
			tw,
			fmt.Sprintf(
				format,
				id,
				net.JoinHostPort(node.RaftAddr.Host, node.RaftAddr.Port),
				node.IsRemoved,
			),
		)
	}

	if err := tw.Flush(); err != nil {
		return "", err
	}

	return b.String(), nil
}

// Close closes the cluster.
func (c *Cluster) Close() error {
	if c.lockFile != nil {
		return c.lockFile.Close()
	}
	return nil
}
