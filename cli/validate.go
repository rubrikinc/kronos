package cli

import (
	"context"
	"flag"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

const (
	maxNodeFailuresFlag         = "max-node-failures"
	fractionFailuresAllowedFlag = "fraction-failures-allowed"
	maxDiffFlag                 = "max-diff"
	durationFlag                = "duration"
	timeBetweenValidationFlag   = "time-between-validation"
)

var validateCtx struct {
	maxNodeFailures         int
	fractionFailuresAllowed float64
	maxDiff                 time.Duration
	duration                time.Duration
	timeout                 time.Duration
	timeBetweenValidation   time.Duration
}

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "validate time on kronos nodes.",
	Long: `
Takes a comma-separated list of hosts and periodically validates that time
difference between them is within threshold.
`,
	Run: func(cmd *cobra.Command, args []string) {
		runValidate()
	},
}

func init() {
	addHostsFlags(validateCmd)

	validateCmd.Flags().DurationVar(
		&validateCtx.maxDiff,
		maxDiffFlag,
		50*time.Millisecond,
		"Maximum delta allowed between time on nodes",
	)
	validateCmd.Flags().IntVar(
		&validateCtx.maxNodeFailures,
		maxNodeFailuresFlag,
		0,
		"Maximum number of nodes on which failure to return time can be tolerated in each validation.",
	)
	validateCmd.Flags().Float64Var(
		&validateCtx.fractionFailuresAllowed,
		fractionFailuresAllowedFlag,
		0.0,
		"Fraction of total validation failures tolerated.",
	)
	validateCmd.Flags().DurationVar(
		&validateCtx.duration,
		durationFlag,
		2*time.Minute,
		"Duration to run validation for",
	)
	validateCmd.Flags().DurationVar(
		&validateCtx.timeout,
		timeoutFlag,
		100*time.Millisecond,
		"timeout for query to fetch kronos time run during periodic validation",
	)
	validateCmd.Flags().DurationVar(
		&validateCtx.timeBetweenValidation,
		timeBetweenValidationFlag,
		time.Second,
		"Time to wait between validations",
	)
}

func runValidate() {
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), validateCtx.timeout)
	defer cancel()
	nodes, err := fetchRaftNodes(ctx)
	if err != nil {
		log.Fatal(ctx, err)
	}
	t := time.NewTicker(validateCtx.timeBetweenValidation)
	numFailuresTolerated := int(
		float64(validateCtx.duration/validateCtx.timeBetweenValidation) *
			validateCtx.fractionFailuresAllowed,
	)
	numFailures := 0
	stop := time.NewTimer(validateCtx.duration)
	for {
		select {
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), validateCtx.timeout)
			stores := nodeInfoFetcher{time: true}.fetch(ctx, nodes)
			timesOnNodes := make(map[string]int64)
			var concatenatedErrors string
			numNodeFailures := 0
			for _, store := range stores {
				raftAddr := kronosutil.NodeAddrToString(store.RaftAddr)
				if store.Err != nil {
					concatenatedErrors = concatenatedErrors +
						errors.Wrap(store.Err, raftAddr).Error() + "\n"
					numNodeFailures++
					continue
				}
				timesOnNodes[raftAddr] = store.Time
			}
			validateDatapoints := func() error {
				if numNodeFailures > validateCtx.maxNodeFailures {
					return errors.Errorf(
						"%d nodes failed to return time, which is more than maxFailuresAllowed=%d\n%s",
						numNodeFailures,
						validateCtx.maxNodeFailures,
						concatenatedErrors,
					)
				}
				return kronosutil.ValidateTimeInConsensus(
					ctx,
					validateCtx.maxDiff,
					timesOnNodes,
				)
			}
			if err := validateDatapoints(); err != nil {
				numFailures++
				log.Error(ctx, err)
			}
			if numFailures > numFailuresTolerated {
				log.Fatalf(
					ctx,
					"%d failures encountered, which is more than numFailuresTolerated(%d)",
					numFailures,
					numFailuresTolerated,
				)
			}
			cancel()
		case <-stop.C:
			return
		}
	}
}
