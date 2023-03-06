package kronosstats

type Gauge interface {
	Value() int64
	Update(int64)
}

type Counter interface {
	Inc(int64)
}

type Histogram interface {
	RecordValue(int64)
}

// KronosMetrics is used to record metrics of Kronos
type KronosMetrics struct {
	// Delta is the offset between local time with kronos time.
	Delta Gauge
	// UptimeDelta is the offset between local time with kronos uptime.
	UptimeDelta Gauge
	// IsOracle is 1 if the current server is the oracle, otherwise 0.
	IsOracle Gauge
	// OverthrowAttemptCount is the number of oracle overthrow attempts.
	OverthrowAttemptCount Counter
	// RTT is the histogram of RTT of oracle time queries.
	RTT Histogram
	// SyncSuccessCount is the number of successful time syncs with the oracle.
	SyncSuccessCount Counter
	// SyncFailureCount is the number of failed time syncs with the oracle.
	SyncFailureCount Counter
	// TimeCap is an upper bound to kronos time.
	TimeCap Gauge
	// TimeCap is an upper bound to kronos uptime.
	UptimeCap Gauge
}
