//go:build upgrade

package acceptance

import (
	"context"
	"fmt"
	"github.com/rubrikinc/kronos/syncutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"text/template"
	"time"

	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/acceptance/testutil"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// as of 9.1
const templatestringOld = `
{{.KronosPath}} start --advertise-host {{.AdvertiseHost}} --raft-port {{.RaftPort}} --grpc-port {{.GrpcPort}} --use-drift-clock --drift-port {{.DriftPort}} --data-dir {{.DataDir}} --pprof-addr :{{.PprofAddr}} --seed-hosts {{.SeedHosts}} --manage-oracle-tick-interval 1s --raft-snap-count 5 2>>{{.LogDir}}/kronos-stderr.log 1>>{{.LogDir}}/kronos-stdout.log
`

// as of 9.2.1
const templateStringNew = `
{{.KronosPath}} start --advertise-host {{.AdvertiseHost}} --listen-addr {{.ListenAddr}} --raft-port {{.RaftPort}} --grpc-port {{.GrpcPort}} --gossip-seed-hosts {{.GossipSeedHosts}} --use-drift-clock --drift-port {{.DriftPort}} --data-dir {{.DataDir}} --pprof-addr {{.PprofAddr}} --seed-hosts {{.SeedHosts}} --manage-oracle-tick-interval 1s --raft-snap-count 5 --test-mode true 2>>{{.LogDir}}/kronos-stderr.log 1>>{{.LogDir}}/kronos-stdout.log
`

const kronosPreUpgrade = "kronos-1"
const kronosPostUpgrade = "kronos-2"

// Config as of 9.1
type KronosConfigOld struct {
	Index         int
	KronosPath    string
	AdvertiseHost string
	RaftPort      int
	GrpcPort      int
	DriftPort     int
	DataDir       string
	PprofAddr     int
	SeedHosts     string
	LogDir        string
}

// Config as of 9.2.1 - needs to be updated as the binary changes
type KronosConfigNew struct {
	KronosConfigOld
	ListenAddr      string
	GossipSeedHosts string
}

func generateConfigs(tmpDir string, numNodes int) []KronosConfigOld {
	configs := make([]KronosConfigOld, numNodes)
	ports, err := testutil.GetFreePorts(context.Background(), 4*numNodes+1)
	if err != nil {
		panic(err)
	}
	binaryPath, err := exec.LookPath(kronosPreUpgrade)
	if err != nil {
		panic(err)
	}
	for i := 0; i < numNodes; i++ {
		symlinkPath := fmt.Sprintf("%v/kronos-node-%v", tmpDir, i)
		err = os.Symlink(binaryPath, symlinkPath)
		if err != nil {
			panic(err)
		}
		configs[i] = KronosConfigOld{
			Index:         i,
			KronosPath:    symlinkPath,
			AdvertiseHost: "127.0.0.1",
			RaftPort:      ports[4*i],
			GrpcPort:      ports[4*i+1],
			DriftPort:     ports[4*i+2],
			PprofAddr:     ports[4*i+3],
			SeedHosts:     fmt.Sprintf("127.0.0.1:%v,127.0.0.1:%v", ports[0], ports[4]),
			DataDir:       fmt.Sprintf("%v/data-%v", tmpDir, i),
			LogDir:        fmt.Sprintf("%v/log-%v", tmpDir, i),
		}
	}
	return configs
}

func generateProcFile(dirPath string, oldConfigs []KronosConfigOld, useNew []bool) (string, error) {
	file, err := os.OpenFile(filepath.Join(dirPath, "Procfile"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()
	oldTmpl, err := template.New("kronos-old").Parse(templatestringOld)
	if err != nil {
		return "", err
	}
	newTmpl, err := template.New("kronos-new").Parse(templateStringNew)
	if err != nil {
		return "", err
	}
	newBinPath, err := exec.LookPath(kronosPostUpgrade)
	if err != nil {
		return "", err
	}

	for i, config := range oldConfigs {
		cmdFile, err := os.OpenFile(filepath.Join(dirPath, fmt.Sprintf("cmd-kronos-node-%v.sh", i)), os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return "", err
		}
		defer cmdFile.Close()
		if useNew[i] {
			newConfig := KronosConfigNew{
				KronosConfigOld: config,
				ListenAddr:      config.AdvertiseHost,
				GossipSeedHosts: fmt.Sprintf("%v:%v,%v:%v", oldConfigs[0].AdvertiseHost, oldConfigs[0].GrpcPort, oldConfigs[1].AdvertiseHost, oldConfigs[1].GrpcPort),
			}
			err = os.Remove(config.KronosPath)
			if err != nil {
				return "", err
			}
			err = os.Symlink(newBinPath, config.KronosPath)
			if err != nil {
				return "", err
			}
			err := newTmpl.Execute(cmdFile, newConfig)
			if err != nil {
				return "", err
			}
		} else {
			err = oldTmpl.Execute(cmdFile, config)
			if err != nil {
				return "", err
			}
		}
		_, err = file.Write([]byte(fmt.Sprintf("kronos%v: %v\n", config.Index, cmdFile.Name())))
		if err != nil {
			return "", err
		}
	}
	return file.Name(), nil
}

// This test expects two binaries to be present in the path: kronos-1 and kronos-2
// which are the old and new versions of the binary respectively.
func TestRU(t *testing.T) {
	ctx := context.Background()
	fs := afero.NewOsFs()
	a := require.New(t)
	tmpDir, err := afero.TempDir(fs, "", "kronos_test_dir_")
	a.NoError(err)
	numNodes := 8
	configs := generateConfigs(tmpDir, numNodes)
	useNew := make([]bool, 8)
	for i := 0; i < 8; i++ {
		useNew[i] = false
	}
	tc := cluster.TestCluster{}
	tc.Nodes = make([]*cluster.TestNode, numNodes)
	for i, config := range configs {
		tc.Nodes[i] = &cluster.TestNode{
			Id:            fmt.Sprintf("kronos%v", i),
			KronosBinary:  config.KronosPath,
			AdvertiseHost: config.AdvertiseHost,
			ListenHost:    config.AdvertiseHost,
			RaftPort:      strconv.Itoa(config.RaftPort),
			GrpcPort:      strconv.Itoa(config.GrpcPort),
			DriftPort:     strconv.Itoa(config.DriftPort),
			PprofAddr:     strconv.Itoa(config.PprofAddr),
			DataDirectory: config.DataDir,
			LogDir:        config.LogDir,
			IsRunning:     true,
			Mu:            &syncutil.RWMutex{},
		}
		err = fs.MkdirAll(config.DataDir, 0755)
		a.NoError(err)
		err = fs.MkdirAll(config.LogDir, 0755)
		a.NoError(err)
	}
	tc.Procfile, err = generateProcFile(tmpDir, configs, useNew)
	a.NoError(err)
	tc.Fs = fs
	a.NoError(tc.Start(ctx))
	defer kronosutil.CloseWithErrorLog(ctx, &tc)
	time.Sleep(bootstrapTime)
	_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
	a.NoError(err)
	for i := 0; i < 8; i++ {
		useNew[i] = true
		a.NoError(tc.RunOperation(ctx, cluster.Stop, i))
		a.NoError(err)
		_, err = generateProcFile(tmpDir, configs, useNew)
		a.NoError(err)
		a.NoError(tc.RunOperation(ctx, cluster.Start, i))
		time.Sleep(kronosStabilizationBufferTime)
		_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
		a.NoError(err)
	}
}

// This test expects two binaries to be present in the path: kronos-1 and kronos-2
// which are the old and new versions of the binary respectively.
func TestRUAllNodesRestart(t *testing.T) {
	ctx := context.Background()
	fs := afero.NewOsFs()
	a := require.New(t)
	tmpDir, err := afero.TempDir(fs, "", "kronos_test_dir_")
	a.NoError(err)
	numNodes := 8
	configs := generateConfigs(tmpDir, numNodes)
	useNew := make([]bool, 8)
	for i := 0; i < 8; i++ {
		useNew[i] = false
	}
	tc := cluster.TestCluster{}
	tc.Nodes = make([]*cluster.TestNode, numNodes)
	for i, config := range configs {
		tc.Nodes[i] = &cluster.TestNode{
			Id:            fmt.Sprintf("kronos%v", i),
			KronosBinary:  config.KronosPath,
			AdvertiseHost: config.AdvertiseHost,
			ListenHost:    config.AdvertiseHost,
			RaftPort:      strconv.Itoa(config.RaftPort),
			GrpcPort:      strconv.Itoa(config.GrpcPort),
			DriftPort:     strconv.Itoa(config.DriftPort),
			PprofAddr:     strconv.Itoa(config.PprofAddr),
			DataDirectory: config.DataDir,
			LogDir:        config.LogDir,
			IsRunning:     true,
			Mu:            &syncutil.RWMutex{},
		}
		err = fs.MkdirAll(config.DataDir, 0755)
		a.NoError(err)
		err = fs.MkdirAll(config.LogDir, 0755)
		a.NoError(err)
	}
	tc.Procfile, err = generateProcFile(tmpDir, configs, useNew)
	a.NoError(err)
	tc.Fs = fs
	a.NoError(tc.Start(ctx))
	defer kronosutil.CloseWithErrorLog(ctx, &tc)
	time.Sleep(bootstrapTime)
	_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
	a.NoError(err)
	for i := 0; i < 8; i++ {
		useNew[i] = true
		a.NoError(tc.Stop(ctx))
		_, err = generateProcFile(tmpDir, configs, useNew)
		a.NoError(err)
		a.NoError(tc.Start(ctx))
		time.Sleep(bootstrapTime)
		_, _, err = tc.ValidateTimeInConsensus(ctx, 50*time.Millisecond, false)
		a.NoError(err)
	}
}
