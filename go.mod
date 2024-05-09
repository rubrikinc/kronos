module github.com/rubrikinc/kronos

go 1.19

require (
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f
	github.com/gogo/protobuf v1.3.2
	github.com/pkg/errors v0.9.1
	github.com/rubrikinc/failure-test-utils v0.0.0-20240509175610-a4013588db1c
	github.com/spf13/afero v1.1.2
	github.com/spf13/cobra v1.1.1
	github.com/stretchr/testify v1.8.4
	go.uber.org/atomic v1.10.0
	golang.org/x/sync v0.0.0-20220513210516-0976fa681c29
	google.golang.org/grpc v1.32.0
)

require (
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5
	github.com/sasha-s/go-deadlock v0.2.0
	github.com/sirupsen/logrus v1.8.1
	go.etcd.io/etcd/pkg/v3 v3.5.0-alpha.0.0.20210320072418-e51c697ec6e8
	go.etcd.io/etcd/raft/v3 v3.5.0-alpha.0.0.20210320072418-e51c697ec6e8
	go.etcd.io/etcd/server/v3 v3.5.0-alpha.0.0.20210320072418-e51c697ec6e8
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.11.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/etcd/api/v3 v3.5.0-alpha.0.0.20210320072418-e51c697ec6e8 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	go.uber.org/zap v1.17.0 // indirect
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	google.golang.org/genproto v0.0.0-20200513103714-09dca8ec2884 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Using cockroachdb fork of etcd to play well with cockroachdb

replace go.etcd.io/etcd/api/v3 => github.com/cockroachdb/etcd/api/v3 v3.0.0-20230718104326-dbe02a550754

replace go.etcd.io/etcd/server/v3 => github.com/cockroachdb/etcd/server/v3 v3.0.0-20230718104326-dbe02a550754

replace go.etcd.io/etcd/pkg/v3 => github.com/cockroachdb/etcd/pkg/v3 v3.0.0-20230718104326-dbe02a550754

replace go.etcd.io/etcd/raft/v3 => github.com/cockroachdb/etcd/raft/v3 v3.0.0-20230718104326-dbe02a550754
