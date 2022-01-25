package kronosstats

type LabelPair struct {
	Name                 *string  `protobuf:"bytes,1,opt,name=name" json:"name,omitempty"`
	Value                *string  `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
	//XXX_NoUnkeyedLiteral struct{} `json:"-"`
	//XXX_sizecache        int32    `json:"-"`
}

type Metadata struct {
	Name                 string         `protobuf:"bytes,1,req,name=name" json:"name"`
	Help                 string         `protobuf:"bytes,2,req,name=help" json:"help"`
	Measurement          string         `protobuf:"bytes,3,req,name=measurement" json:"measurement"`
	Unit                 int32           `protobuf:"varint,4,req,name=unit,enum=cockroach.util.metric.Unit" json:"unit"`
//	MetricType           _go.MetricType `protobuf:"varint,5,opt,name=metricType,enum=io.prometheus.client.MetricType" json:"metricType"`
	Labels               []*LabelPair   `protobuf:"bytes,6,rep,name=labels" json:"labels,omitempty"`
//	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
//	XXX_sizecache        int32          `json:"-"`
}

// Fully-qualified names for metrics.
var (
	MetaKronosDelta = Metadata{
		Name: "kronos.delta",
		Help: "Offset between local time and kronos time"}
	MetaKronosUptimeDelta = Metadata{
		Name: "kronos.uptimedelta",
		Help: "Offset between local uptime and kronos uptime"}
	MetaKronosIsOracle = Metadata{
		Name: "kronos.oracle",
		Help: "1 if the current node is the oracle, 0 otherwise"}
	MetaKronosOverthrowCounter = Metadata{
		Name: "kronos.overthrow.count",
		Help: "Number of oracle overthrow attempts"}
	MetaKronosRTT = Metadata{
		Name: "kronos.rtt",
		Help: "RTT of Kronos RPCs"}
	MetaKronosSyncSuccess = Metadata{
		Name: "kronos.sync.success.count",
		Help: "Number of successful syncs with the oracle"}
	MetaKronosSyncFailure = Metadata{
		Name: "kronos.sync.failure.count",
		Help: "Number of failed syncs with the oracle"}
	MetaKronosTimeCap = Metadata{
		Name: "kronos.timecap",
		Help: "Time cap of Kronos"}
	MetaKronosUptimeCap = Metadata{
		Name: "kronos.uptimecap",
		Help: "Time cap of Kronos Uptime"}
)
