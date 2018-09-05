package log

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type simpleLogger struct {
}

func (s *simpleLogger) Info(ctx context.Context, args ...interface{}) {
	log.Info(ctx, args...)
}

func (s *simpleLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	log.Infof(ctx, format, args...)
}

func (s *simpleLogger) InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	log.InfofDepth(ctx, depth, format, args...)
}

func (s *simpleLogger) Warning(ctx context.Context, args ...interface{}) {
	log.Warning(ctx, args...)
}

func (s *simpleLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	log.Warningf(ctx, format, args...)
}

func (s *simpleLogger) WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	log.WarningfDepth(ctx, depth, format, args...)
}

func (s *simpleLogger) Error(ctx context.Context, args ...interface{}) {
	log.Error(ctx, args...)
}

func (s *simpleLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	log.Errorf(ctx, format, args...)
}

func (s *simpleLogger) ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	log.ErrorfDepth(ctx, depth, format, args...)
}

func (s *simpleLogger) Fatal(ctx context.Context, args ...interface{}) {
	log.Fatal(ctx, args...)
}

func (s *simpleLogger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	log.Fatalf(ctx, format, args...)
}

func (s *simpleLogger) FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	log.FatalfDepth(ctx, depth, format, args...)
}

func (s *simpleLogger) V(level int32) bool {
	return log.V(level)
}

func (s *simpleLogger) WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return log.WithLogTag(ctx, name, value)
}

func (s *simpleLogger) Flush() {
}
