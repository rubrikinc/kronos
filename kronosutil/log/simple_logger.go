package log

import (
	"context"
	"go.uber.org/zap/zapcore"

	log "github.com/sirupsen/logrus"
)

type simpleLogger struct {
}

func (s *simpleLogger) Enabled(level zapcore.Level) bool {
	return true
}

func (s *simpleLogger) With(fields []zapcore.Field) zapcore.Core {
	return s
}

func (s *simpleLogger) Check(entry zapcore.Entry, entry2 *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return entry2.AddCore(entry, s)
}

func (s *simpleLogger) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	ctx := context.Background()
	for _, f := range fields {
		ctx = s.WithLogTag(ctx, f.Key, f.Interface)
	}
	switch entry.Level {
	case zapcore.DebugLevel, zapcore.InfoLevel:
		s.Infof(ctx, "%s - %s", entry.Caller.TrimmedPath(), entry.Message)
	case zapcore.WarnLevel:
		s.Warningf(ctx, "%s - %s", entry.Caller.TrimmedPath(), entry.Message)
	case zapcore.ErrorLevel:
		s.Errorf(ctx, "%s - %s", entry.Caller.TrimmedPath(), entry.Message)
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		s.Fatalf(ctx, "%s - %s", entry.Caller.TrimmedPath(), entry.Message)
	default:
		s.Infof(ctx, "%s - %s", entry.Caller.TrimmedPath(), entry.Message)
	}
	return nil
}

func (s *simpleLogger) Sync() error {
	s.Flush()
	return nil
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
