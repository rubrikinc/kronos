package oracle

import (
	"context"
	"fmt"

	"github.com/scaledata/etcd/raft"

	"github.com/rubrikinc/kronos/kronosutil/log"
)

// init sets the raft logger for kronos/etcd/raft. This is different from
// pkg/storage/raft.go as this updates the logger of the raft used by Kronos
func init() {
	raft.SetLogger(&raftLogger{ctx: context.Background()})
}

// raftLogger is used as a logger for etcd/raft so that its logs are a part of
// kronos logs
type raftLogger struct {
	ctx context.Context
}

func (r *raftLogger) Debug(v ...interface{}) {
	log.InfofDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Debugf(format string, v ...interface{}) {
	log.InfofDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Info(v ...interface{}) {
	log.InfofDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Infof(format string, v ...interface{}) {
	log.InfofDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Warning(v ...interface{}) {
	log.WarningfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Warningf(format string, v ...interface{}) {
	log.WarningfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Error(v ...interface{}) {
	log.ErrorfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Errorf(format string, v ...interface{}) {
	log.ErrorfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Fatal(v ...interface{}) {
	log.FatalfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Fatalf(format string, v ...interface{}) {
	log.FatalfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	log.ErrorfDepth(r.ctx, 1, s)
	panic(s)
}

func (r *raftLogger) Panicf(format string, v ...interface{}) {
	log.ErrorfDepth(r.ctx, 1, format, v...)
	panic(fmt.Sprintf(format, v...))
}
