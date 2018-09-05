package metadata

import (
	"io/ioutil"
	"os"
	"regexp"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/scaledata/kronos/pb"
)

func createNode(host, port string) *kronospb.Node {
	return &kronospb.Node{RaftAddr: &kronospb.NodeAddr{Host: host, Port: port}}
}

func createRemovableNode(host, port string, isRemoved bool) *kronospb.Node {
	node := createNode(host, port)
	node.IsRemoved = isRemoved
	return node
}

func getClusterForTest() (*Cluster, string) {
	dir, err := ioutil.TempDir("", "metadata")
	if err != nil {
		panic(err)
	}
	cluster := NewClusterProto()
	cluster.AllNodes["1"] = createNode("1.2.3.4", "1234")
	cluster.AllNodes["2"] = createNode("2.3.4.1", "2341")
	cluster.AllNodes["3"] = createNode("3.4.1.2", "3413")
	cluster.AllNodes["4"] = createNode("3.4.1.2", "3414")
	cluster.AllNodes["5"] = createRemovableNode("3.4.1.2", "3415", true /* isRemoved */)
	cluster.AllNodes["6"] = createRemovableNode("3.4.1.2", "3416", true /* isRemoved */)
	c, err := NewCluster(dir, cluster)
	if err != nil {
		panic(err)
	}
	return c, dir
}

func TestNodes(t *testing.T) {
	testCases := []struct {
		name                string
		cluster             *kronospb.Cluster
		expectedActiveNodes map[string]*kronospb.Node
	}{
		{
			name: "all active",
			cluster: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": createRemovableNode("1.2.3.4", "1234", false),
					"2": createRemovableNode("1.2.3.5", "1234", false),
					"3": createRemovableNode("1.2.3.6", "1234", false),
				},
			},
			expectedActiveNodes: map[string]*kronospb.Node{
				"1": createRemovableNode("1.2.3.4", "1234", false),
				"2": createRemovableNode("1.2.3.5", "1234", false),
				"3": createRemovableNode("1.2.3.6", "1234", false),
			},
		},
		{
			name: "some active",
			cluster: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": createRemovableNode("1.2.3.4", "1234", false),
					"2": createRemovableNode("1.2.3.5", "1234", true),
					"3": createRemovableNode("1.2.3.6", "1234", true),
				},
			},
			expectedActiveNodes: map[string]*kronospb.Node{
				"1": createRemovableNode("1.2.3.4", "1234", false),
			},
		},
		{
			name: "none active",
			cluster: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": createRemovableNode("1.2.3.4", "1234", true),
					"2": createRemovableNode("1.2.3.5", "1234", true),
					"3": createRemovableNode("1.2.3.6", "1234", true),
				},
			},
			expectedActiveNodes: map[string]*kronospb.Node{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := &Cluster{}
			cluster.mu.cluster = tc.cluster
			a := assert.New(t)
			// Test NodesIncludingRemoved
			a.Equal(cluster.mu.cluster.AllNodes, cluster.NodesIncludingRemoved())
			// Test ActiveNodes
			a.Equal(tc.expectedActiveNodes, cluster.ActiveNodes())
		})
	}
}

func TestRemoveNodeAndPersist(t *testing.T) {
	a := assert.New(t)

	cluster, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	a.Equal(len(cluster.NodesIncludingRemoved()), 6)
	a.Equal(len(cluster.ActiveNodes()), 4)
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": createNode("1.2.3.4", "1234"),
		"2": createNode("2.3.4.1", "2341"),
		"3": createNode("3.4.1.2", "3413"),
		"4": createNode("3.4.1.2", "3414"),
		"5": createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
		"6": createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
	})
	cluster.RemoveNode("2")
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": createNode("1.2.3.4", "1234"),
		"2": createRemovableNode("2.3.4.1", "2341", true),
		"3": createNode("3.4.1.2", "3413"),
		"4": createNode("3.4.1.2", "3414"),
		"5": createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
		"6": createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
	})

	cluster.RemoveNode("1")
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": createRemovableNode("1.2.3.4", "1234", true),
		"2": createRemovableNode("2.3.4.1", "2341", true),
		"3": createNode("3.4.1.2", "3413"),
		"4": createNode("3.4.1.2", "3414"),
		"5": createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
		"6": createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
	})

	cluster.RemoveNode("30")

	a.NoError(cluster.Persist())
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1":  createRemovableNode("1.2.3.4", "1234", true),
		"2":  createRemovableNode("2.3.4.1", "2341", true),
		"3":  createNode("3.4.1.2", "3413"),
		"30": {IsRemoved: true},
		"4":  createNode("3.4.1.2", "3414"),
		"5":  createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
		"6":  createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
	})

	cluster2, err := LoadCluster(dir, true /* readOnly */)
	if a.NoError(err) {
		a.Equal(cluster2.NodesIncludingRemoved(), map[string]*kronospb.Node{
			"1":  createRemovableNode("1.2.3.4", "1234", true),
			"2":  createRemovableNode("2.3.4.1", "2341", true),
			"3":  createNode("3.4.1.2", "3413"),
			"30": {IsRemoved: true},
			"4":  createNode("3.4.1.2", "3414"),
			"5":  createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
			"6":  createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
		})
	}

	// Test persist failure as cluster2 is read only
	cluster2.RemoveNode("2")
	err = cluster2.Persist()
	if a.Error(err) {
		a.Equal("cluster opened in readOnly mode", err.Error())
	}
}

func TestUpdateNodeAndPersist(t *testing.T) {
	a := assert.New(t)
	dataDir, err := ioutil.TempDir("", "data_dir")
	defer func() {
		_ = os.RemoveAll(dataDir)
	}()
	a.NoError(err)

	host1 := &kronospb.NodeAddr{
		Host: "123",
		Port: "123",
	}

	host2 := &kronospb.NodeAddr{
		Host: "124",
		Port: "124",
	}

	host3 := &kronospb.NodeAddr{
		Host: "125",
		Port: "125",
	}

	host4 := &kronospb.NodeAddr{
		Host: "126",
		Port: "126",
	}

	_, err = LoadCluster(dataDir, false /* readOnly */)
	a.Equal(ErrNoClusterFile, err)
	_, err = LoadCluster(dataDir, true /* readOnly */)
	a.Equal(ErrNoClusterFile, err)

	cluster, err := NewCluster(dataDir, NewClusterProto())
	a.NoError(err)

	a.NoError(cluster.AddNode("1", host1))
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": {
			RaftAddr: host1,
		},
	})
	a.NoError(cluster.AddNode("2", host2))
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": {
			RaftAddr: host1,
		},
		"2": {
			RaftAddr: host2,
		},
	})

	// Adding the same node ID again should return an error
	err = cluster.AddNode("1", host4)
	if a.Error(err) {
		a.Equal("node 1 already exists", err.Error())
	}
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": {
			RaftAddr: host1,
		},
		"2": {
			RaftAddr: host2,
		},
	})

	a.NoError(cluster.AddNode("3", host3))
	a.NoError(cluster.Persist())
	a.Equal(cluster.NodesIncludingRemoved(), map[string]*kronospb.Node{
		"1": {
			RaftAddr: host1,
		},
		"2": {
			RaftAddr: host2,
		},
		"3": {
			RaftAddr: host3,
		},
	})

	cluster2, err := LoadCluster(dataDir, true /* readOnly */)
	if a.NoError(err) {
		a.NoError(cluster2.AddNode("4", host4))
		err = cluster2.Persist()
		if a.Error(err) {
			a.Equal("cluster opened in readOnly mode", err.Error())
		}
	}
}

func TestUpdateAddr(t *testing.T) {
	testCases := []struct {
		name         string
		oldToNewAddr map[string]string
		expected     *kronospb.Cluster
		withPorts    bool
		errRegexp    *regexp.Regexp
	}{
		{
			name: "without ports",
			oldToNewAddr: map[string]string{
				"1.2.3.4": "4.3.2.1",
				"2.3.4.1": "1.4.3.2",
				"3.4.1.2": "2.1.4.3",
			},
			withPorts: false,
			expected: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": createNode("4.3.2.1", "1234"),
					"2": createNode("1.4.3.2", "2341"),
					"3": createNode("2.1.4.3", "3413"),
					"4": createNode("2.1.4.3", "3414"),
					// Removed nodes are unchanged and do not require mapping
					"5": createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
					"6": createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
				},
			},
		},
		{
			name: "with ports",
			oldToNewAddr: map[string]string{
				"1.2.3.4:1234": "4.3.2.1:4321",
				"2.3.4.1:2341": "1.4.3.2:1432",
				"3.4.1.2:3413": "2.1.4.3:2143",
				"3.4.1.2:3414": "2.1.4.3:2144",
			},
			withPorts: true,
			expected: &kronospb.Cluster{
				AllNodes: map[string]*kronospb.Node{
					"1": createNode("4.3.2.1", "4321"),
					"2": createNode("1.4.3.2", "1432"),
					"3": createNode("2.1.4.3", "2143"),
					"4": createNode("2.1.4.3", "2144"),
					// Removed nodes are unchanged and do not require mapping
					"5": createRemovableNode("3.4.1.2", "3415", true /* isRemoved */),
					"6": createRemovableNode("3.4.1.2", "3416", true /* isRemoved */),
				},
			},
		},
		{
			name: "no mapping exists with ports",
			oldToNewAddr: map[string]string{
				"1.2.3.4:1234": "4.3.2.1:4321",
				"3.4.1.2:3412": "2.1.4.3:2143",
			},
			withPorts: true,
			errRegexp: regexp.MustCompile("no mapping exists for.*"),
		},
		{
			name: "no mapping exists without ports",
			oldToNewAddr: map[string]string{
				"1.2.3.4:1234": "4.3.2.1:4321",
				"2.3.4.1:2341": "1.4.3.2:1432",
				"3.4.1.2:3412": "2.1.4.3:2143",
			},
			withPorts: false,
			errRegexp: regexp.MustCompile("no mapping exists for.*"),
		},
		{
			name: "no mapping exists with ports 2",
			oldToNewAddr: map[string]string{
				"1.2.3.4": "4.3.2.1",
				"2.3.4.1": "1.4.3.2",
				"3.4.1.2": "2.1.4.3",
			},
			withPorts: true,
			errRegexp: regexp.MustCompile("no mapping exists for.*"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			c, dir := getClusterForTest()
			defer func() {
				_ = os.RemoveAll(dir)
			}()
			err := c.UpdateAddrs(tc.oldToNewAddr, tc.withPorts)
			if tc.errRegexp != nil {
				a.Regexp(tc.errRegexp, err.Error())
			} else {
				a.NoError(err)
			}
			if tc.expected != nil {
				a.Equal(tc.expected, c.mu.cluster)
			}
			a.NoError(c.Close())
		})
	}
}

func TestLoadAndPersist(t *testing.T) {
	c, dir := getClusterForTest()
	assert.NoError(t, c.Persist())
	assert.NoError(t, c.Close())
	c, err := LoadCluster(dir, false /*readOnly*/)
	assert.NoError(t, err)
	expected := &kronospb.Cluster{
		AllNodes: map[string]*kronospb.Node{
			"1": createNode("1.2.3.4", "1234"),
			"2": createNode("2.3.4.1", "2341"),
			"3": createNode("3.4.1.2", "3413"),
			"4": createNode("3.4.1.2", "3414"),
			"5": createRemovableNode("3.4.1.2", "3415", true),
			"6": createRemovableNode("3.4.1.2", "3416", true),
		},
	}
	assert.True(t, proto.Equal(c.mu.cluster, expected))
	assert.Nil(t, c.Close())
}

func TestReadOnlyLoadAndPersist(t *testing.T) {
	c, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.Nil(t, c.Persist())
	assert.Nil(t, c.Close())
	c, err := LoadCluster(dir, true /*readOnly*/)
	assert.Nil(t, err)
	expected := &kronospb.Cluster{
		AllNodes: map[string]*kronospb.Node{
			"1": createNode("1.2.3.4", "1234"),
			"2": createNode("2.3.4.1", "2341"),
			"3": createNode("3.4.1.2", "3413"),
			"4": createNode("3.4.1.2", "3414"),
			"5": createRemovableNode("3.4.1.2", "3415", true),
			"6": createRemovableNode("3.4.1.2", "3416", true),
		},
	}
	assert.True(t, proto.Equal(c.mu.cluster, expected))
	err = c.Persist()
	assert.Regexp(t, regexp.MustCompile("cluster opened in readOnly mode"), err.Error())
	assert.Nil(t, c.Close())
}

func TestPrettyPrint(t *testing.T) {
	a := assert.New(t)
	c, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	expected := "ID  Address       IsRemoved\n" +
		"1   1.2.3.4:1234  false\n" +
		"2   2.3.4.1:2341  false\n" +
		"3   3.4.1.2:3413  false\n" +
		"4   3.4.1.2:3414  false\n" +
		"5   3.4.1.2:3415  true\n" +
		"6   3.4.1.2:3416  true\n"
	s, err := c.PrettyPrint()
	a.NoError(err)
	a.Equal(expected, s)
	a.NoError(c.Close())
}

func TestSingleReadWriteCluster(t *testing.T) {
	c, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	assert.NoError(t, c.Persist())
	_, err := LoadCluster(dir, false /*readOnly*/)
	assert.Regexp(t, regexp.MustCompile("file already locked"), err.Error())
	assert.NoError(t, c.Close())
}

func TestMultipleReadOnlyClusters(t *testing.T) {
	a := assert.New(t)
	c, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	a.NoError(c.Persist())
	a.NoError(c.Close())
	d, err := LoadCluster(dir, true /*readOnly*/)
	a.NoError(err)
	e, err := LoadCluster(dir, true /*readOnly*/)
	a.NoError(err)
	a.NoError(d.Close())
	a.NoError(e.Close())
}

func TestLoadingEmptyCluster(t *testing.T) {
	a := assert.New(t)
	c, dir := getClusterForTest()
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	a.NoError(c.Close())
	_, err := LoadCluster(dir, false /*readOnly*/)
	if a.Error(err) {
		a.Equal("cluster file does not exist", err.Error())
	}
	_, err = LoadCluster(dir, true /*readOnly*/)
	if a.Error(err) {
		a.Equal("cluster file does not exist", err.Error())
	}

	c, dir = getClusterForTest()
	a.NoError(c.Persist())
	a.NoError(c.Close())
	d, err := LoadCluster(dir, false /*readOnly*/)
	a.NoError(err)
	a.NoError(d.Close())
	e, err := LoadCluster(dir, true /*readOnly*/)
	a.NoError(err)
	a.NoError(e.Close())
}
