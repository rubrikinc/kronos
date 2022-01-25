package kronosstats

type Gauge struct {
	Metadata
	value *int64
	fn    func() int64
}

func (g Gauge) Inc(i int64)  {

}

func (g Gauge) RecordValue(i int64)  {

}

func (g Gauge) Update(i int64)  {

}

// NewGauge creates a Gauge.
func NewGauge(metadata Metadata) *Gauge {
	return &Gauge{metadata, new(int64), nil}
}

// KronosMetrics is used to record metrics of Kronos
type KronosMetrics struct {
	// Delta is the offset between local time with kronos time.
	Delta *Gauge
	// UptimeDelta is the offset between local time with kronos uptime.
	UptimeDelta *Gauge
	// IsOracle is 1 if the current server is the oracle, otherwise 0.
	IsOracle *Gauge
	// OverthrowAttemptCount is the number of oracle overthrow attempts.
	OverthrowAttemptCount *Gauge
	// RTT is the histogram of RTT of oracle time queries.
	RTT *Gauge
	// SyncSuccessCount is the number of successful time syncs with the oracle.
	SyncSuccessCount *Gauge
	// SyncFailureCount is the number of failed time syncs with the oracle.
	SyncFailureCount *Gauge
	// TimeCap is an upper bound to kronos time.
	TimeCap *Gauge
	// TimeCap is an upper bound to kronos uptime.
	UptimeCap *Gauge
}

// NewMetrics returns KronosMetrics which can be used to record metrics
func NewMetrics() *KronosMetrics {
	return &KronosMetrics{
		// Percentile values of RTT is over last 1 minute
		Delta:                 NewGauge(MetaKronosDelta),
		UptimeDelta:           NewGauge(MetaKronosUptimeDelta),
		IsOracle:              NewGauge(MetaKronosIsOracle),
		OverthrowAttemptCount: NewGauge(MetaKronosOverthrowCounter),
		RTT:                   NewGauge(MetaKronosRTT),
		SyncFailureCount:      NewGauge(MetaKronosSyncFailure),
		SyncSuccessCount:      NewGauge(MetaKronosSyncSuccess),
		TimeCap:               NewGauge(MetaKronosTimeCap),
		UptimeCap:             NewGauge(MetaKronosUptimeCap),
	}
}
