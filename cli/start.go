package cli

import (
	"context"
	"io/ioutil"
	"net"
	"net/http"
	// net/http/pprof is included for profiling
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/rubrikinc/kronos/kronosutil/log"
	"github.com/rubrikinc/kronos/oracle"
	"github.com/rubrikinc/kronos/pb"
	"github.com/rubrikinc/kronos/server"
	"github.com/rubrikinc/kronos/tm"
)

const (
	advertiseHostFlag            = "advertise-host"
	dataDirFlag                  = "data-dir"
	grpcPortFlag                 = "grpc-port"
	manageOracleTickIntervalFlag = "manage-oracle-tick-interval"
	oracleTimeCapDeltaFlag       = "oracle-time-cap-delta"
	raftPortFlag                 = "raft-port"
	raftSnapCountFlag            = "raft-snap-count"
	seedHostsFlag                = "seed-hosts"
)

var startCtx struct {
	advertiseHost            string
	dataDir                  string
	grpcPort                 string
	manageOracleTickInterval time.Duration
	oracleTimeCapDelta       time.Duration
	pprofAddr                string
	raftPort                 string
	seedHosts                string
	raftSnapCount            uint64
	driftClock               struct {
		servicePort     string
		startConfigFile string
		enable          bool
	}
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start a kronos node",
	Long: `
Start a kronos node with synchronizes time with other kronos nodes and returns
nearly the same time on all the nodes irrespective of system time.
The peers of this node are specified via seed-hosts.
`,
	Run: func(cmd *cobra.Command, args []string) { runStart() },
}

func init() {
	ctx := context.TODO()
	startCmd.Flags().StringVar(
		&startCtx.advertiseHost,
		advertiseHostFlag,
		"",
		"advertise-host of kronos",
	)
	if err := startCmd.MarkFlagRequired(advertiseHostFlag); err != nil {
		log.Fatal(ctx, err)
	}

	startCmd.Flags().StringVar(
		&startCtx.grpcPort,
		grpcPortFlag,
		server.KronosDefaultGRPCPort,
		"GRPC port of kronos",
	)

	startCmd.Flags().StringVar(
		&startCtx.seedHosts,
		seedHostsFlag,
		"",
		"Comma separated list of kronos seed hosts in the cluster",
	)
	if err := startCmd.MarkFlagRequired(seedHostsFlag); err != nil {
		log.Fatal(ctx, err)
	}

	startCmd.Flags().Uint64Var(
		&startCtx.raftSnapCount,
		raftSnapCountFlag,
		0,
		"Number of raft entries after which a snapshot is triggered."+
			"If the value is <= 0, a default value is used",
	)

	startCmd.Flags().StringVar(
		&startCtx.raftPort,
		raftPortFlag,
		server.KronosDefaultRaftPort,
		"Raft port of kronos",
	)

	startCmd.Flags().StringVar(
		&startCtx.dataDir,
		dataDirFlag,
		"./",
		"Data directory for storing wal and snapshots",
	)

	startCmd.Flags().DurationVar(
		&startCtx.manageOracleTickInterval,
		manageOracleTickIntervalFlag,
		3*time.Second,
		"manageOracleTickInterval is the period where an action is taken based "+
			"on the state of the oracle state machine",
	)

	startCmd.Flags().DurationVar(
		&startCtx.oracleTimeCapDelta,
		oracleTimeCapDeltaFlag,
		server.DefaultOracleTimeCapDelta,
		"OracleTimeCapDelta is the delta added to current KronosTime to determine the time cap."+
			"No server will return a KronosTime more than the time cap known to it."+
			" The time cap is persisted in the oracle state machine and is used to"+
			" ensure monotonicity on cluster restarts. The delta should at least be twice"+
			" as much as "+manageOracleTickIntervalFlag,
	)

	startCmd.Flags().BoolVar(
		&startCtx.driftClock.enable,
		"use-drift-clock",
		false,
		"Whether to use drifting clock",
	)

	startCmd.Flags().StringVar(
		&startCtx.driftClock.servicePort,
		"drift-port",
		server.KronosDefaultDriftServerPort,
		"Drift time service port",
	)

	// using a file to get a drift config so that we can avoid changing goreman
	// Procfile to change the time config during the test.
	startCmd.Flags().StringVar(
		&startCtx.driftClock.startConfigFile,
		"drift-start-config",
		"",
		"Drift config file to start with",
	)

	startCmd.Flags().StringVar(
		&startCtx.pprofAddr,
		"pprof-addr",
		net.JoinHostPort("localhost", server.KronosDefaultPProfPort),
		"Address to enable pprof on. Empty string disables pprof",
	)

}

func initHTTPPprof(ctx context.Context) {
	if startCtx.pprofAddr == "" {
		return
	}
	log.Infof(ctx, "Starting http pprof. To see debug info go to http://%s/debug/pprof", startCtx.pprofAddr)
	go func() {
		if err := http.ListenAndServe(startCtx.pprofAddr, nil); err != nil {
			log.Fatal(ctx, "HTTP pprof ListenAndServer error", err)
		}
	}()
}

func runStart() {
	ctx := context.Background()
	// oracleTimeCapDelta should at least be twice as much as
	// manageOracleTickInterval otherwise KronosTime can hang by hitting
	// Time Cap
	tickIntervalUpperBound := startCtx.oracleTimeCapDelta / 2
	if startCtx.manageOracleTickInterval > tickIntervalUpperBound {
		log.Fatalf(
			ctx,
			"%s should at most be %s (%s / 2)",
			manageOracleTickIntervalFlag,
			tickIntervalUpperBound,
			oracleTimeCapDeltaFlag,
		)
	}

	initHTTPPprof(ctx)
	var clock tm.Clock
	if startCtx.driftClock.enable {
		lis, err := net.Listen(
			"tcp",
			net.JoinHostPort(startCtx.advertiseHost, startCtx.driftClock.servicePort),
		)
		if err != nil {
			log.Fatalf(ctx, "Failed to listen: %v", err)
		}
		server := grpc.NewServer()
		var driftTimeConfig *kronospb.DriftTimeConfig
		if startCtx.driftClock.startConfigFile != "" {
			content, err := ioutil.ReadFile(startCtx.driftClock.startConfigFile)
			if err != nil {
				log.Fatalf(ctx, "Failed to read file: %s", startCtx.driftClock.startConfigFile)
			}
			driftTimeConfig = &kronospb.DriftTimeConfig{}
			if err := protoutil.Unmarshal(content, driftTimeConfig); err != nil {
				log.Fatalf(
					ctx,
					"Failed to unmarshal proto from file: %s",
					startCtx.driftClock.startConfigFile,
				)
			}
		}
		driftServer := tm.NewUpdateDriftClockServer(driftTimeConfig)
		kronospb.RegisterUpdateDriftTimeServiceServer(server, driftServer)
		go func() {
			if err := server.Serve(lis); err != nil {
				log.Fatal(ctx, err)
			}
		}()
		clock = driftServer.Clock
	} else {
		clock = tm.NewMonotonicClock()
	}
	config := server.Config{
		Clock: clock,
		ManageOracleTickInterval: startCtx.manageOracleTickInterval,
		OracleTimeCapDelta:       startCtx.oracleTimeCapDelta,
		RaftConfig: &oracle.RaftConfig{
			CertsDir: kronosCertsDir(),
			DataDir:  startCtx.dataDir,
			GRPCHostPort: &kronospb.NodeAddr{
				Host: startCtx.advertiseHost,
				Port: startCtx.grpcPort,
			},
			RaftHostPort: &kronospb.NodeAddr{
				Host: startCtx.advertiseHost,
				Port: startCtx.raftPort,
			},
			SeedHosts: strings.Split(startCtx.seedHosts, ","),
			SnapCount: startCtx.raftSnapCount,
		},
	}
	server, err := server.NewKronosServer(ctx, config)
	if err != nil {
		log.Fatal(ctx, err)
	}
	defer server.Stop()
	if err := server.RunServer(ctx); err != nil {
		log.Fatal(ctx, err)
	}
}
