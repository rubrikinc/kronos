## Kronos

Kronos is a distributed service / library which can be used to provide 
synchronized time in a cluster. 

It provides an API to query "kronos time", which would be nearly same on all 
kronos nodes in the cluster. It does not update system time.

- [What is Kronos?](#what-is-kronos)
- [Design](#design)
- [Server Operation](#server-operation)
- [Usage](#usage)


## What is kronos
Kronos is a fault tolerant service / library which can be used to have 
synchronized time in a cluster. The time provided by this service may be 
different from system time, but it will be nearly the same on all nodes of the 
cluster.

In a cluster each node has a local system clock which it uses for time. 
There can be clock skews between nodes. Individual nodes can also have clock
jumps. Some services require a "global" timestamp for ordering of events, 
and having a global view of time is difficult in a cluster. 


Kronos solves above problems and has the following properties:
- It always serves monotonic time.
- It is immune to large jumps in system time (Go v1.9 onwards).
- It has the same rate of flow as real time.
- It returns nearly the same time on all the nodes (within a few ms).
- It is fault tolerant and works in the presence of node failures
   - It requires only a quorum of nodes to be up.
- It supports addition / removal of nodes from the cluster.

## Design
- **Time is always monotonic**  
This is easy to ensure for a running service by storing the last served 
timestamp in memory and flat-lining if a backward jump is detected.
To ensure monotonic time across restart, an upper bound to time is periodically 
persisted (like current time +10s). This time cap is used to ensure monotonicity
across restarts.

- **It is immune to large jumps in system time**  
  **It has the same rate of flow as real time**  
Kronos uses CLOCK_MONOTONIC along with CLOCK_REALTIME. 
CLOCK_REALTIME represents the machine's best-guess as to the current wall-clock, 
time-of-day time.  
CLOCK_MONOTONIC represents the absolute elapsed wall-clock time since some 
arbitrary, fixed point in the past. It isn't affected by changes in the system 
time-of-day clock.  
CLOCK_MONOTONIC is used to compute progress of time from a fixed reference 
picked from CLOCK_REALTIME.  
Time used in kronos is given by   
`local_time = (start_time + time.Since(start_time))`  
`start_time` is a fixed point of reference taken from CLOCK_REALTIME.  
`time.Since(start_time)` is the monotonic time since `start_time`. 
Time used this way is immune to large system jumps and has the same rate of 
flow as real time.  
This is supported Go 1.9 onwards
https://github.com/golang/proposal/blob/master/design/12914-monotonic.md

- **Returns nearly the same time on all the nodes (within a few ms)**  
**It is fault tolerant and works in the presence of node failures**  
**It supports addition / removal of nodes from the cluster**  
Kronos elects a single "oracle" from within the cluster. This oracle is a node 
with which all nodes in the cluster periodically sync time. Periodic time 
synchronization suffices since each node is immune to large system time jumps. 
RPCs are used to synchronize time with the oracle.
Kronos time on each node is given as `local_time + delta`, where delta is the 
time difference between kronos time this node and kronos time of the oracle. The 
delta is updated during every synchronization with the oracle. 
The oracle is elected using raft (distributed consensus protocol). Raft provides
a mechanism to manage a distributed state machine. Kronos stores the identity of
the current oracle and an upper bound to time (for safety reasons) in this state 
machine.  
Raft is failure tolerant and can continue to work as long as a quorum of nodes 
are healthy. It also supports addition and removal of nodes to the cluster.


## Server operation
When a kronos server starts up, it first tries to initialize itself to find out 
the current kronos time.  
It does the following till it is initialized
1. If there is no current oracle
   1. Propose self as oracle
2. If self is the current oracle
   1. Wait to see if another initialized node becomes the oracle. Initialized 
      nodes have the correct kronos time and should get preference to become 
      the oracle
      1. If no nodes becomes the oracle
         1. If a backward jump is detected (since this node has no delta 
             information)
            1. Continue time from time cap
         2. Propose self as oracle
3. If self is not the oracle, sync with the current oracle
   1. If the the current oracle is down 
      1. Propose self as oracle

When proposing self as oracle, the server proposes itself as the new oracle and 
a new time cap to the state machine managed by raft. Raft guarantees 
serializability so CAS (compare and set) operations are used to manage this 
state machine.  
Once the server is initialized, it operates as follows  
1. If self is the current oracle
   1. Continue proposing self as oracle
2. If self is not the oracle, sync with the current oracle
   1. If the the current oracle is down
      1. Propose self as oracle

**Syncing time with the oracle**  
Time synchronization with the oracle happens over RPCs. Kronos has a limit on 
the RTT allowed for this RPC for it to be considered valid. The goal is to keep 
the kronos time of the follower in sync with kronos time of the oracle.
![Time sync with oracle](docs/media/kronos_sync.png?raw=true "Time sync with oracle")

Let's say a follower requests the oracle for its kronos time at t<sub>s</sub> 
and receives a response at t<sub>e</sub> (where t<sub>s</sub> is the kronos time 
of the follower), and the oracle responds with t<sub>o</sub>.  
Time needs to be adjusted taking RTT into account. The true oracle time will lie 
between t<sub>o</sub> and t<sub>o</sub> + RTT

If t<sub>f</sub> does not lie within this interval, delta is adjusted so that it
falls into this interval (kronos time on follower is given as local_time + delta
with oracle).

**Cluster operations**  
Addition and removal of nodes entail addition and removal of nodes from the raft
cluster. A kronos node maintains metadata about itself and its view of the
cluster locally in a file.  
Each kronos server is passed a list of kronos seed hosts which it can use to 
initialize itself.  
When a new kronos node is started, it assigns itself a NodeID and requests the 
seed hosts to add this new node to the raft cluster. Once this request succeeds,
it joins the raft cluster and continues its usual operations.  
Seed hosts are not necessary for subsequent restarts of a node since each node 
has a local view of the cluster, but can be used in order to ramp up a node 
which has been down for a long time. 


## Usage
Kronos can be used as a standalone service or embedded in another service
as a library.

Kronos requires Go version 1.9+ for immunity against large clock jumps.

**Install**
- Install Go version 1.9
- Run `go get github.com/rubrikinc/kronos`
  This will clone kronos in `$GOPATH/src/github.com/rubrikinc/kronos`

**Build/Test**
- Build kronos: `make build` creates a kronos binary.
- Run unit tests: `make test`
- Run acceptance tests: `make acceptance`

`kronos help` can be used to get information on subcommands.

**Starting a kronos server**  
The below are example commands to create a kronos cluster
- Log and data directory have to be created
- The IPs used are 127.0.0.1. For multi node clusters, the IP of different nodes
  can be used.
```
Server 1
./kronos \
  --log-dir ./log0\
  start\
  --advertise-host 127.0.0.1\
  --raft-port 5766\
  --grpc-port 5767\
  --pprof-addr :5768\
  --data-dir ./data0\
  --seed-hosts 127.0.0.1:5766,127.0.0.1:6766
  
Server 2
./kronos \
  --log-dir ./log1\
  start\
  --advertise-host 127.0.0.1\
  --raft-port 6766\
  --grpc-port 6767\
  --pprof-addr :6768\
  --data-dir ./data1\
  --seed-hosts 127.0.0.1:5766,127.0.0.1:6766
  
Server 3
./kronos \
  --log-dir ./log2\
  start\
  --advertise-host 127.0.0.1\
  --raft-port 7766\
  --grpc-port 7767\
  --pprof-addr :7768\
  --data-dir ./data2\
  --seed-hosts 127.0.0.1:5766,127.0.0.1:6766
```
Kronos servers discover each other based on seed hosts (which is the raft 
address of some nodes of the cluster)

Time (In Unix Nanos) can be queried using `kronos time` and status can be 
queried using `kronos status`. If the default ports (5766, 5767) are not being 
used, they need to be passed as arguments to time / status commands. For example 
```
./kronos time --grpc-addr=127.0.0.1:5767
1536315057173673605

./kronos status --raft-addr 127.0.0.1:5766 --all                                                                                                                                kronos ✚ ✱ ◼
Raft ID           Raft Address    GRPC Address    Server Status  Oracle Address  Oracle Id  Time Cap             Delta  Time
a7041e2396dec1e3  127.0.0.1:5766  127.0.0.1:5767  INITIALIZED    127.0.0.1:5767  599        1536315306176477839  0s     1536315246559974475
b2300f5e4ece5750  127.0.0.1:7766  127.0.0.1:7767  INITIALIZED    127.0.0.1:5767  599        1536315306176477839  0s     1536315246560100423
c56aec267a57d604  127.0.0.1:6766  127.0.0.1:6767  INITIALIZED    127.0.0.1:5767  599        1536315306176477839  0s     1536315246560047515
```

**Removing a node from the cluster**  
A node can be removed using
`./kronos cluster remove <Raft ID>` where Raft ID is the id in `kronos status`

**Embedding kronos**  
A kronos server can be started in a go application by using
```
import(
	"github.com/rubrikinc/kronos"
	"github.com/rubrikinc/kronos/oracle"
	"github.com/rubrikinc/kronos/pb"
	kronosserver "github.com/rubrikinc/kronos/server"
)

  if err := kronos.Initialize(ctx, kronosserver.Config{
    Clock:                    tm.NewMonotonicClockWithOffset(int64(serverCfg.ClockOffset)),
    OracleTimeCapDelta:       kronosserver.DefaultOracleTimeCapDelta,
    ManageOracleTickInterval: 3 * time.Second,
    RaftConfig: &oracle.RaftConfig{
      CertsDir: certsDir,
      DataDir:  kronosDir,
      GRPCHostPort: &kronospb.NodeAddr{
        Host: advertiseHost,
        Port: serverCfg.KronosGRPCPort,
      },
      RaftHostPort: &kronospb.NodeAddr{
        Host: advertiseHost,
        Port: serverCfg.KronosRaftPort,
      },
      SeedHosts: seedHosts,
    },
  }); err != nil {
    return err
  }

  // call kronos.Stop() in shutdown
  // call kronos.Now() for kronos time
```

**TLS Support**  
Kronos supports TLS. The certificates directory can be passed using --certs-dir
or set using the environment variable `KRONOS_CERTS_DIR`.
The CA and node certs can be created using something similar to 
https://www.cockroachlabs.com/docs/stable/create-security-certificates.html
