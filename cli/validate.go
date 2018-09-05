package cli

import (
	"context"
	"flag"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/scaledata/kronos/kronosutil"
	"github.com/scaledata/kronos/kronosutil/log"
)

const (
	maxFailuresFlag           = "max-failures"
	maxDiffFlag               = "max-diff"
	durationFlag              = "duration"
	timeBetweenValidationFlag = "time-between-validation"
)

var validateCtx struct {
	maxFailures           int
	maxDiff               time.Duration
	duration              time.Duration
	timeout               time.Duration
	timeBetweenValidation time.Duration
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
		&validateCtx.maxFailures,
		maxFailuresFlag,
		0,
		"Maximum number of nodes on which failure to return time can be tolerated in each validation.",
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
		50*time.Millisecond,
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
	stop := time.NewTimer(validateCtx.duration)
	for {
		select {
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), validateCtx.timeout)
			stores := nodeInfoFetcher{time: true}.fetch(ctx, nodes)
			timesOnNodes := make(map[string]int64)
			var concatenatedErrors string
			numErrors := 0
			for _, store := range stores {
				raftAddr := kronosutil.NodeAddrToString(store.RaftAddr)
				if store.Err != nil {
					concatenatedErrors = concatenatedErrors +
						errors.Wrap(store.Err, raftAddr).Error() + "\n"
					numErrors++
					continue
				}
				timesOnNodes[raftAddr] = store.Time
			}
			if numErrors > validateCtx.maxFailures {
				log.Fatalf(
					ctx,
					"%d nodes failed to return time, which is more than maxFailuresAllowed=%d\n%s",
					numErrors,
					validateCtx.maxFailures,
					concatenatedErrors,
				)
			}
			if err := kronosutil.ValidateTimeInConsensus(ctx, validateCtx.maxDiff, timesOnNodes); err != nil {
				log.Fatal(ctx, err)
			}
			cancel()
		case <-stop.C:
			return
		}
	}
}
