// +build acceptance

package acceptance

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/rubrikinc/kronos/syncutil"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/acceptance/chaos"
	"github.com/rubrikinc/kronos/acceptance/cluster"
	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/kronosutil/log"
)

const (
	chaosInterval = 5 * time.Second
	// defaultValidationThreshold is the threshold describing the maximum
	// difference allowed between times on the nodes in the cluster.
	defaultValidationThreshold = 50 * time.Millisecond
	// loggingPeriod denotes the number of validations after which the times are
	// logged.
	loggingPeriod = 100
	numNodes      = 8
	testDuration  = 5 * time.Minute
	// validationInterval denotes the interval between two validations.
	validationInterval = 100 * time.Millisecond
	// validationTimeInChaos denotes the time for which validation runs before
	// chaos policy is recovered. We want to run 50 validations per chaos
	// injection.
	validationTimeInChaos = 50 * validationInterval
)

type timeValidator struct {
	syncutil.RWMutex
	validationThreshold time.Duration
}

func newTimeValidator(validationThreshold time.Duration) *timeValidator {
	tv := new(timeValidator)
	tv.validationThreshold = validationThreshold
	return tv
}

// runPolicy runs the given policy
func runPolicy(
	ctx context.Context, tc *cluster.TestCluster, validator *timeValidator, policy chaos.Policy,
) error {
	// injection
	if err := func() error {
		validator.Lock()
		defer validator.Unlock()
		log.Infof(ctx, "Injecting %v", policy)
		thresholdDuringPolicy, err := policy.Inject(ctx, tc)
		if err != nil {
			log.Errorf(
				ctx,
				"Failed to inject failure policy %v, error: %v",
				policy,
				err,
			)
			return err
		}
		validator.validationThreshold += thresholdDuringPolicy
		return nil
	}(); err != nil {
		return err
	}

	time.Sleep(validationTimeInChaos)

	// recovery
	err := func() error {
		validator.Lock()
		defer validator.Unlock()
		log.Infof(ctx, "Recovering from %v", policy)
		err := policy.Recover(ctx, tc)
		if err != nil {
			log.Errorf(
				ctx,
				"Failed to recover from failure policy %v, error: %v",
				policy,
				err,
			)
			return err
		}
		validator.validationThreshold = defaultValidationThreshold
		return nil
	}()
	return err
}

func TestKronosChaos(t *testing.T) {
	ctx := context.TODO()
	fs := afero.NewOsFs()

	tc, err := cluster.NewCluster(
		ctx,
		cluster.ClusterConfig{
			Fs:                       fs,
			NumNodes:                 numNodes,
			ManageOracleTickInterval: manageOracleTickInterval,
			RaftSnapCount:            5,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer kronosutil.CloseWithErrorLog(ctx, tc)
	log.Info(ctx, "Initialized Test Cluster.")

	time.Sleep(3 * chaosInterval)
	nodeIdxToTimes, nodeIdxToUptimes, err := tc.ValidateTimeInConsensus(
		ctx,
		50*time.Millisecond,
		false, /*checkOnlyRunningNodes*/
	)
	log.Infof(ctx, "Initial relative time on nodes: %v", nodeIdxToTimes.Relative(), nodeIdxToUptimes.Relative())
	if err != nil {
		t.Fatal(err)
	}

	chaosPolicies := []chaos.Policy{
		&chaos.StopAnySubsetPolicy{MaxNodeFailuresAllowed: (numNodes - 1) / 2},
		&chaos.StopOraclePolicy{},
		&chaos.RestartOraclePolicy{},
		&chaos.RestartAllPolicy{},
		&chaos.DriftAllPolicy{},
		&chaos.DriftOraclePolicy{},
		&chaos.AddRemovePolicy{},
	}

	validator := newTimeValidator(defaultValidationThreshold)

	stopChaosCh := make(chan struct{})
	wg := sync.WaitGroup{}

	randSeed := time.Now().UnixNano()
	log.Infof(ctx, "Random seed: %d", randSeed)
	t.Logf("Random seed: %d", randSeed)
	rand.Seed(randSeed)

	wg.Add(1)
	injectChaosPeriodically := func() {
		defer wg.Done()
		for {
			ticker := time.NewTicker(chaosInterval)
			select {
			case <-ticker.C:
				idx := rand.Intn(len(chaosPolicies))
				log.Infof(ctx, "Injecting %v", chaosPolicies[idx])
				err := runPolicy(ctx, tc, validator, chaosPolicies[idx])
				assert.Nil(t, err)
			case <-stopChaosCh:
				return
			}
		}
	}

	go injectChaosPeriodically()

	lastNodeIdxToTimes, lastNodeIdxToUptimes, err := tc.ValidateTimeInConsensus(
		ctx,
		validator.validationThreshold,
		true, /*checkOnlyRunningNodes*/
	)
	lastActualTime := time.Now()
	assert.Nil(t, err)
	var timeElapsedOnNodes map[int]time.Duration
	var uptimeElapsedOnNodes map[int]time.Duration
	start := time.Now()
	validationCount := 0
	for time.Since(start) < testDuration {
		func() {
			validator.RLock()
			defer validator.RUnlock()
			timeoutCtx, cancelFunc := context.WithTimeout(ctx, validationInterval)
			defer cancelFunc()
			// validate time is in consensus on all the nodes
			nodeIdxToTimes, nodeIdxToUptimes, err = tc.ValidateTimeInConsensus(
				timeoutCtx,
				validator.validationThreshold,
				true, /*checkOnlyRunningNodes*/
			)
			currActualTime := time.Now()
			if err != nil {
				log.Errorf(
					ctx,
					"Time validation failed, node times: %v, error: %v",
					nodeIdxToTimes.Relative(),
					err,
				)
			}

			assert.NoError(t, err)
			// validationCount is incremented here avoid running the progress check
			// for first run.
			validationCount++
			if validationCount%loggingPeriod == 0 {
				// validate time has progressed with around the same flow as real time on
				// oracle.
				// Progress test fails on jenkins due to the virtual environment.
				// Disabling the progress check to deflake the build. We should run this
				// locally to check the progress of cluster.
				// The below validation is a littly flaky right now so it is disabled.
				if false {
					timeElapsedOnNodes = nodeIdxToTimes.Since(lastNodeIdxToTimes)
					expectedDuration := currActualTime.Sub(lastActualTime)
					log.Infof(ctx, "Expected time elapsed: %v", expectedDuration)
					for _, timeElapsed := range timeElapsedOnNodes {
						assert.InDelta(
							t,
							float64(expectedDuration),
							float64(timeElapsed),
							0.5*float64(validationInterval*loggingPeriod),
						)
					}

					uptimeElapsedOnNodes = nodeIdxToUptimes.Since(lastNodeIdxToUptimes)
					for _, uptimeElapsed := range uptimeElapsedOnNodes {
						assert.InDelta(
							t,
							float64(expectedDuration),
							float64(uptimeElapsed),
							0.5*float64(validationInterval*loggingPeriod),
						)
					}
				}
				log.Infof(
					ctx,
					"Completed %d validations over %v, relative time: %v, time elapsed on nodes: %v, uptime elapsed on nodes",
					validationCount,
					time.Since(start),
					nodeIdxToTimes.Relative(),
					timeElapsedOnNodes,
					uptimeElapsedOnNodes,
				)
				lastNodeIdxToTimes = nodeIdxToTimes
				lastNodeIdxToUptimes = nodeIdxToUptimes
				lastActualTime = currActualTime
			}
		}()
		time.Sleep(validationInterval)
	}
	close(stopChaosCh)
	wg.Wait()
}
