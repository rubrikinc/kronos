package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

const (
	timeoutFlag = "timeout"
	formatFlag  = "format"
)

var statusCtx struct {
	timeout time.Duration
	format  string
	all     bool
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Status of kronos node(s)",
	Long: `
Takes a comma-separated list of host:port and returns the status and time
server information for them.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runStatus()
	},
}

func init() {
	addHostsFlags(statusCmd)

	statusCmd.Flags().StringVar(
		&statusCtx.format,
		formatFlag,
		"pretty",
		"format to print status in. Supported values are: json, pretty",
	)

	statusCmd.Flags().DurationVar(
		&statusCtx.timeout,
		timeoutFlag,
		30*time.Second,
		"timeout for the command",
	)

	statusCmd.Flags().BoolVar(
		&statusCtx.all,
		"all",
		false,
		"show all fields in status",
	)
}

func runStatus() {
	ctx := context.Background()

	if statusCtx.timeout != 0 {
		var cancelCtx func()
		ctx, cancelCtx = context.WithTimeout(context.Background(), statusCtx.timeout)
		defer cancelCtx()
	}
	nodes, err := fetchRaftNodes(ctx)
	if err != nil {
		log.Fatal(ctx, err)
	}
	stores := nodeInfoFetcher{time: true, status: true}.fetch(ctx, nodes)
	numErrors := 0
	for _, store := range stores {
		if store.Err != nil {
			numErrors++
		}
	}
	switch statusCtx.format {
	case "pretty":
		if err := prettyPrintStatus(os.Stdout, os.Stderr, stores, statusCtx.all); err != nil {
			log.Fatal(ctx, err)
		}
	case "json":
		serialized, err := json.MarshalIndent(stores, "", "  ")
		if err != nil {
			log.Fatalf(ctx, "Couldn't serialize status: %v", stores)
		}
		fmt.Println(string(serialized))
	default:
		log.Fatalf(ctx, "format %s not supported for status", statusCtx.format)
	}
	if numErrors != 0 {
		os.Exit(1)
	}
}

const (
	raftIDHeader         = "Raft ID"
	raftAddrHeader       = "Raft Address"
	grpcAddrHeader       = "GRPC Address"
	serverStatusHeader   = "Server Status"
	oracleAddrHeader     = "Oracle Address"
	oracleIDHeader       = "Oracle Id"
	timeCapHeader        = "Time Cap"
	deltaHeader          = "Delta"
	timeHeader           = "Time"
	raftLeaderHeader     = "Raft Leader"
	raftTermHeader       = "Raft Term"
	appliedIndexHeader   = "Applied Index"
	committedIndexHeader = "Committed Index"
)

func prettyPrintStatus(
	outputStream io.Writer, errorStream io.Writer, stores []*NodeInfo, allFields bool,
) error {
	const noValue = "N/A"

	filterErrorsAndPrintField := func(n *NodeInfo, fieldFn func(n *NodeInfo) interface{}, errorTags ...string) string {
		for _, tag := range errorTags {
			if err := n.getError(tag); err != nil {
				return noValue
			}
		}
		return fmt.Sprint(fieldFn(n))
	}

	valueFn := map[string]func(n *NodeInfo) string{
		raftIDHeader:   func(n *NodeInfo) string { return n.ID },
		raftAddrHeader: func(n *NodeInfo) string { return kronosutil.NodeAddrToString(n.RaftAddr) },
		grpcAddrHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} { return kronosutil.NodeAddrToString(n.GRPCAddr) }, grpcAddrErrTag)
		},
		serverStatusHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.ServerStatus
			}, grpcAddrErrTag, statusErrTag)
		},
		oracleAddrHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return kronosutil.NodeAddrToString(n.OracleState.Oracle)
			}, grpcAddrErrTag, statusErrTag)
		},
		oracleIDHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.OracleState.Id
			}, grpcAddrErrTag, statusErrTag)
		},
		timeCapHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.OracleState.TimeCap
			}, grpcAddrErrTag, statusErrTag)
		},
		deltaHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return time.Duration(n.Delta)
			}, grpcAddrErrTag, statusErrTag)
		},
		timeHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.Time
			}, grpcAddrErrTag, timeErrTag)
		},
		raftLeaderHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.RaftLeader
			}, statusErrTag)
		},
		raftTermHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.RaftTerm
			}, statusErrTag)
		},
		appliedIndexHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.AppliedIndex
			}, statusErrTag)
		},
		committedIndexHeader: func(n *NodeInfo) string {
			return filterErrorsAndPrintField(n, func(n *NodeInfo) interface{} {
				return n.CommittedIndex
			}, statusErrTag)
		},
	}

	tw := tabwriter.NewWriter(
		outputStream,
		2,   /* minWidth */
		2,   /* tabWidth */
		2,   /* padding */
		' ', /* padChar */
		0,   /* flags */
	)

	var fields []string
	if allFields {
		fields = []string{
			raftIDHeader,
			raftAddrHeader,
			grpcAddrHeader,
			serverStatusHeader,
			oracleAddrHeader,
			oracleIDHeader,
			timeCapHeader,
			deltaHeader,
			timeHeader,
			raftLeaderHeader,
			raftTermHeader,
			appliedIndexHeader,
			committedIndexHeader,
		}
	} else {
		fields = []string{
			raftIDHeader,
			grpcAddrHeader,
			serverStatusHeader,
			oracleAddrHeader,
			deltaHeader,
		}
	}

	// generate format
	repeat := func(s string, n int) (rep []string) {
		for ; n > 0; n-- {
			rep = append(rep, s)
		}
		return
	}
	// format is %s\t%s\t....%s\n where %s comes len(fields) times
	format := strings.Join(repeat("%s", len(fields)), "\t") + "\n"

	row := make([]interface{}, len(fields))

	// write Headers
	for i := range fields {
		row[i] = fields[i]
	}
	fmt.Fprintf(tw, format, row...)

	// write Statuses
	for _, store := range stores {
		for i, field := range fields {
			row[i] = valueFn[field](store)
		}
		fmt.Fprintf(tw, format, row...)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	// write Errors
	tags := []string{grpcAddrErrTag, statusErrTag, timeErrTag}
	for _, store := range stores {
		raftAddr := kronosutil.NodeAddrToString(store.RaftAddr)
		for _, tag := range tags {
			if err := store.getError(tag); err != nil {
				fmt.Fprintf(
					errorStream,
					"%s Error: Couldn't fetch kronos %s due to err: %s\n",
					raftAddr,
					tag,
					err,
				)
			}
		}
	}
	return nil
}
