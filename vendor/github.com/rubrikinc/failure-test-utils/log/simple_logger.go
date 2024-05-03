package log

import (
	"context"

	log "github.com/sirupsen/logrus"
)

type simpleLogger struct {
}

func (s *simpleLogger) Info(ctx context.Context, args ...interface{}) {
	args = append(args, ctx)
	log.Println(args...)
}

func (s *simpleLogger) Infof(ctx context.Context, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) Warning(ctx context.Context, args ...interface{}) {
	args = append(args, ctx)
	log.Println(args...)
}

func (s *simpleLogger) Warningf(ctx context.Context, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) Error(ctx context.Context, args ...interface{}) {
	args = append(args, ctx)
	log.Println(args...)
}

func (s *simpleLogger) Errorf(ctx context.Context, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Printf(format, args...)
}

func (s *simpleLogger) Fatal(ctx context.Context, args ...interface{}) {
	args = append(args, ctx)
	log.Fatal(args...)
}

func (s *simpleLogger) Fatalf(ctx context.Context, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Fatalf(format, args...)
}

func (s *simpleLogger) FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	args = append(args, ctx)
	log.Fatalf(format, args...)
}

func (s *simpleLogger) V(level int32) bool {
	log.SetLevel(log.TraceLevel)
	return true
}

func (s *simpleLogger) WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	log.WithField(name, value)
	return ctx
}

func (s *simpleLogger) Flush() {
}
