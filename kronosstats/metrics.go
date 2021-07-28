package kronosstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// KronosMetrics is used to record metrics of Kronos
type KronosMetrics struct {
	// Delta is the offset between local time with kronos time.
	Delta *metric.Gauge
	// UptimeDelta is the offset between local time with kronos uptime.
	UptimeDelta *metric.Gauge
	// IsOracle is 1 if the current server is the oracle, otherwise 0.
	IsOracle *metric.Gauge
	// OverthrowAttemptCount is the number of oracle overthrow attempts.
	OverthrowAttemptCount *metric.Counter
	// RTT is the histogram of RTT of oracle time queries.
	RTT *metric.Histogram
	// SyncSuccessCount is the number of successful time syncs with the oracle.
	SyncSuccessCount *metric.Counter
	// SyncFailureCount is the number of failed time syncs with the oracle.
	SyncFailureCount *metric.Counter
	// TimeCap is an upper bound to kronos time.
	TimeCap *metric.Gauge
	// TimeCap is an upper bound to kronos uptime.
	UptimeCap *metric.Gauge
}

// NewMetrics returns KronosMetrics which can be used to record metrics
func NewMetrics() *KronosMetrics {
	return &KronosMetrics{
		// Percentile values of RTT is over last 1 minute
		Delta:                 metric.NewGauge(MetaKronosDelta),
		UptimeDelta:           metric.NewGauge(MetaKronosUptimeDelta),
		IsOracle:              metric.NewGauge(MetaKronosIsOracle),
		OverthrowAttemptCount: metric.NewCounter(MetaKronosOverthrowCounter),
		RTT:                   metric.NewLatency(MetaKronosRTT, time.Minute),
		SyncFailureCount:      metric.NewCounter(MetaKronosSyncFailure),
		SyncSuccessCount:      metric.NewCounter(MetaKronosSyncSuccess),
		TimeCap:               metric.NewGauge(MetaKronosTimeCap),
		UptimeCap:             metric.NewGauge(MetaKronosUptimeCap),
	}
}
