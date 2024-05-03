package log

import (
	"context"
	"sync"
)

type kronosLogger struct {
	sync.Mutex
	l Logger
}

var logger = &kronosLogger{l: &simpleLogger{}}

// Logger can be be used to override the logger used by kronos
type Logger interface {
	// Info log
	Info(ctx context.Context, args ...interface{})
	// Infof log
	Infof(ctx context.Context, format string, args ...interface{})
	// InfofDepth log
	InfofDepth(ctx context.Context, depth int, format string, args ...interface{})
	// Warning log
	Warning(ctx context.Context, args ...interface{})
	// Warningf log
	Warningf(ctx context.Context, format string, args ...interface{})
	// WarningfDepth log
	WarningfDepth(ctx context.Context, depth int, format string, args ...interface{})
	// Error log
	Error(ctx context.Context, args ...interface{})
	// Errorf log
	Errorf(ctx context.Context, format string, args ...interface{})
	// ErrorfDepth log
	ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{})
	// Fatal log
	Fatal(ctx context.Context, args ...interface{})
	// Fatalf log
	Fatalf(ctx context.Context, format string, args ...interface{})
	// FatalfDepth log
	FatalfDepth(ctx context.Context, depth int, format string, args ...interface{})
	// V returns whether the given verbosity should be logged
	V(level int32) bool
	// WithLogTag returns a context with a log tag
	WithLogTag(ctx context.Context, name string, value interface{}) context.Context
	// Flush the logger
	Flush()
}

// SetLogger is used to override the kronos logger. It should be called
// before initializing kronos
func SetLogger(l Logger) {
	logger.Lock()
	defer logger.Unlock()
	logger.l = l
}

// Info log
func Info(ctx context.Context, args ...interface{}) {
	logger.l.InfofDepth(ctx, 2, "", args...)
}

// Infof log
func Infof(ctx context.Context, format string, args ...interface{}) {
	logger.l.InfofDepth(ctx, 2, format, args...)
}

// InfofDepth log
func InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logger.l.InfofDepth(ctx, depth+2, format, args...)
}

// Warning log
func Warning(ctx context.Context, args ...interface{}) {
	logger.l.WarningfDepth(ctx, 2, "", args...)
}

// Warningf log
func Warningf(ctx context.Context, format string, args ...interface{}) {
	logger.l.WarningfDepth(ctx, 2, format, args...)
}

// WarningfDepth log
func WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logger.l.WarningfDepth(ctx, depth+2, format, args...)
}

// Error log
func Error(ctx context.Context, args ...interface{}) {
	logger.l.ErrorfDepth(ctx, 2, "", args...)
}

// Errorf log
func Errorf(ctx context.Context, format string, args ...interface{}) {
	logger.l.ErrorfDepth(ctx, 2, format, args...)
}

// ErrorfDepth log
func ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logger.l.ErrorfDepth(ctx, depth+2, format, args...)
}

// Fatal log
func Fatal(ctx context.Context, args ...interface{}) {
	logger.l.FatalfDepth(ctx, 2, "", args...)
}

func Fatalf(ctx context.Context, format string, args ...interface{}) {
	logger.l.FatalfDepth(ctx, 2, format, args...)
}

// FatalfDepth log
func FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	logger.l.FatalfDepth(ctx, depth, format, args...)
}

// V returns whether the given verbosity should be logged
func V(level int32) bool {
	return logger.l.V(level)
}

// WithLogTag returns a context with a log tag
func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return logger.l.WithLogTag(ctx, name, value)
}

// Flush the logger
func Flush() {
	logger.l.Flush()
}
