package cli

import (
	"bytes"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/rubrikinc/kronos/kronosutil"
	"github.com/rubrikinc/kronos/pb"
)

func stringToNodeAddr(addr string) *kronospb.NodeAddr {
	a, err := kronosutil.NodeAddr(addr)
	if err != nil {
		panic(err)
	}
	return a
}

func TestPrettyPrintStatus(t *testing.T) {
	er := errors.New("timeout")
	testCases := []struct {
		name           string
		stores         []*NodeInfo
		allFields      bool
		inOutputStream string
		inErrorStream  string
	}{
		{
			name: "all fields: nodeinfo error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					Err: errMap{
						grpcAddrErrTag: er,
					},
				},
			},
			allFields:     true,
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos grpc-addr due to err: timeout\n",
			inOutputStream: `Raft ID  Raft Address    GRPC Address  Server Status  Oracle Address  Oracle Id  Time Cap  Delta  Time  Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766  N/A           N/A            N/A             N/A        N/A       N/A    N/A   false        0          0              0
`,
		},
		{
			name: "some fields: nodeinfo error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					Err:      errMap{grpcAddrErrTag: er},
				},
			},
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos grpc-addr due to err: timeout\n",
			inOutputStream: `Raft ID  GRPC Address  Server Status  Oracle Address  Delta
1        N/A           N/A            N/A             N/A
`,
		},
		{
			name: "all fields: single node",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Time:  1534084075586595055,
				},
			},
			allFields: true,
			inOutputStream: `Raft ID  Raft Address    GRPC Address    Server Status  Oracle Address  Oracle Id  Time Cap             Delta  Time                 Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766  localhost:5767  INITIALIZED    localhost:5766  43         1534084077586595055  0s     1534084075586595055  false        0          0              0
`,
		},
		{
			name: "some fields: single node",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Time:  1534084075586595055,
				},
			},
			inOutputStream: `Raft ID  GRPC Address    Server Status  Oracle Address  Delta
1        localhost:5767  INITIALIZED    localhost:5766  0s
`,
		},
		{
			name: "all fields: status error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					GRPCAddr: stringToNodeAddr("localhost:5767"),
					Time:     1534084075586595055,
					Err:      errMap{statusErrTag: er},
				},
			},
			allFields:     true,
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos status due to err: timeout\n",
			inOutputStream: `Raft ID  Raft Address    GRPC Address    Server Status  Oracle Address  Oracle Id  Time Cap  Delta  Time                 Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766  localhost:5767  N/A            N/A             N/A        N/A       N/A    1534084075586595055  N/A          N/A        N/A            N/A
`,
		},
		{
			name: "some fields: status error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					GRPCAddr: stringToNodeAddr("localhost:5767"),
					Time:     1534084075586595055,
					Err:      errMap{statusErrTag: er},
				},
			},
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos status due to err: timeout\n",
			inOutputStream: `Raft ID  GRPC Address    Server Status  Oracle Address  Delta
1        localhost:5767  N/A            N/A             N/A
`,
		},
		{
			name: "all fields: time error",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Err:   errMap{timeErrTag: er},
				},
			},
			allFields:     true,
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos time due to err: timeout\n",
			inOutputStream: `Raft ID  Raft Address    GRPC Address    Server Status  Oracle Address  Oracle Id  Time Cap             Delta  Time  Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766  localhost:5767  INITIALIZED    localhost:5766  43         1534084077586595055  0s     N/A   false        0          0              0
`,
		},
		{
			name: "some fields: time error",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Err:   errMap{timeErrTag: er},
				},
			},
			inErrorStream: "localhost:5766 Error: Couldn't fetch kronos time due to err: timeout\n",
			inOutputStream: `Raft ID  GRPC Address    Server Status  Oracle Address  Delta
1        localhost:5767  INITIALIZED    localhost:5766  0s
`,
		},
		{
			name: "all fields: status and time error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					GRPCAddr: stringToNodeAddr("localhost:5767"),
					Err:      errMap{statusErrTag: er, timeErrTag: er},
				},
			},
			allFields: true,
			inErrorStream: `localhost:5766 Error: Couldn't fetch kronos status due to err: timeout
localhost:5766 Error: Couldn't fetch kronos time due to err: timeout
`,
			inOutputStream: `Raft ID  Raft Address    GRPC Address    Server Status  Oracle Address  Oracle Id  Time Cap  Delta  Time  Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766  localhost:5767  N/A            N/A             N/A        N/A       N/A    N/A   N/A          N/A        N/A            N/A
`,
		},
		{
			name: "some fields: status and time error",
			stores: []*NodeInfo{
				{
					ID:       "1",
					RaftAddr: stringToNodeAddr("localhost:5766"),
					GRPCAddr: stringToNodeAddr("localhost:5767"),
					Err:      errMap{statusErrTag: er, timeErrTag: er},
				},
			},
			inErrorStream: `localhost:5766 Error: Couldn't fetch kronos status due to err: timeout
localhost:5766 Error: Couldn't fetch kronos time due to err: timeout
`,
			inOutputStream: `Raft ID  GRPC Address    Server Status  Oracle Address  Delta
1        localhost:5767  N/A            N/A             N/A
`,
		},
		{
			name: "all fields: multiple nodes",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Time:  1534084075586595055,
				},
				{
					ID:           "2",
					RaftAddr:     stringToNodeAddr("remotehost:5766"),
					GRPCAddr:     stringToNodeAddr("remotehost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Time:  1534084075586595055,
				},
			},
			allFields: true,
			inOutputStream: `Raft ID  Raft Address     GRPC Address     Server Status  Oracle Address  Oracle Id  Time Cap             Delta  Time                 Raft Leader  Raft Term  Applied Index  Committed Index
1        localhost:5766   localhost:5767   INITIALIZED    localhost:5766  43         1534084077586595055  0s     1534084075586595055  false        0          0              0
2        remotehost:5766  remotehost:5767  INITIALIZED    localhost:5766  43         1534084077586595055  0s     1534084075586595055  false        0          0              0
`,
		},
		{
			name: "some fields: multiple nodes",
			stores: []*NodeInfo{
				{
					ID:           "1",
					RaftAddr:     stringToNodeAddr("localhost:5766"),
					GRPCAddr:     stringToNodeAddr("localhost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 0,
					Time:  1534084075586595055,
				},
				{
					ID:           "2",
					RaftAddr:     stringToNodeAddr("remotehost:5766"),
					GRPCAddr:     stringToNodeAddr("remotehost:5767"),
					ServerStatus: kronospb.ServerStatus_INITIALIZED,
					OracleState: &kronospb.OracleState{
						Id:      43,
						TimeCap: 1534084077586595055,
						Oracle:  stringToNodeAddr("localhost:5766"),
					},
					Delta: 39710,
					Time:  1534084075586595055,
				},
			},
			inOutputStream: `Raft ID  GRPC Address     Server Status  Oracle Address  Delta
1        localhost:5767   INITIALIZED    localhost:5766  0s
2        remotehost:5767  INITIALIZED    localhost:5766  39.71µs
`,
		},
	}
	a := assert.New(t)
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var outS bytes.Buffer
			var errS bytes.Buffer
			a.NoError(prettyPrintStatus(&outS, &errS, tc.stores, tc.allFields))
			a.Equal(tc.inOutputStream, outS.String())
			a.Equal(tc.inErrorStream, errS.String())
		})
	}
}
