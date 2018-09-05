package kronosstats

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Fully-qualified names for metrics.
var (
	MetaKronosDelta = metric.Metadata{
		Name: "kronos.delta",
		Help: "Offset between local time and kronos time"}
	MetaKronosIsOracle = metric.Metadata{
		Name: "kronos.oracle",
		Help: "1 if the current node is the oracle, 0 otherwise"}
	MetaKronosOverthrowCounter = metric.Metadata{
		Name: "kronos.overthrow.count",
		Help: "Number of oracle overthrow attempts"}
	MetaKronosRTT = metric.Metadata{
		Name: "kronos.rtt",
		Help: "RTT of Kronos RPCs"}
	MetaKronosSyncSuccess = metric.Metadata{
		Name: "kronos.sync.success.count",
		Help: "Number of successful syncs with the oracle"}
	MetaKronosSyncFailure = metric.Metadata{
		Name: "kronos.sync.failure.count",
		Help: "Number of failed syncs with the oracle"}
	MetaKronosTimeCap = metric.Metadata{
		Name: "kronos.timecap",
		Help: "Time cap of Kronos"}
)
