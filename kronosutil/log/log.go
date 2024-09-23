package log

import (
	"context"
	"sync/atomic"
	"unsafe"

	"go.uber.org/zap/zapcore"
)

type kronosLogger struct {
	logPtr unsafe.Pointer
}

var defaultLogger Logger = &simpleLogger{}
var logger = &kronosLogger{logPtr: unsafe.Pointer(&defaultLogger)}

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
	// Core implements zapcore.Core needed for logging in transport, storage and other packages
	zapcore.Core
}

func extractLoggerAndRun(f func(Logger)) {
	loggerPtr := (*Logger)(atomic.LoadPointer(&logger.logPtr))
	if loggerPtr == nil {
		return
	}
	f(*loggerPtr)
}

// SetLogger is used to override the kronos logger. It should be called
// before initializing kronos
func SetLogger(l Logger) {
	atomic.StorePointer(&logger.logPtr, unsafe.Pointer(&l))
}

func Getlogger() Logger {
	p := atomic.LoadPointer(&logger.logPtr)
	if p != nil {
		return *(*Logger)(p)
	}
	return nil
}

// Info log
func Info(ctx context.Context, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Info(ctx, args...)
	})
}

// Infof log
func Infof(ctx context.Context, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Infof(ctx, format, args...)
	})
}

// InfofDepth log
func InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.InfofDepth(ctx, depth+1, format, args...)
	})
}

// Warning log
func Warning(ctx context.Context, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Warning(ctx, args...)
	})
}

// Warningf log
func Warningf(ctx context.Context, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Warningf(ctx, format, args...)
	})
}

// WarningfDepth log
func WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.WarningfDepth(ctx, depth+1, format, args...)
	})
}

// Error log
func Error(ctx context.Context, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Error(ctx, args...)
	})
}

// Errorf log
func Errorf(ctx context.Context, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Errorf(ctx, format, args...)
	})
}

// ErrorfDepth log
func ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.ErrorfDepth(ctx, depth+1, format, args...)
	})
}

// Fatal log
func Fatal(ctx context.Context, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Fatal(ctx, args...)
	})
}

// Fatalf log
func Fatalf(ctx context.Context, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.Fatalf(ctx, format, args...)
	})
}

// FatalfDepth log
func FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
	extractLoggerAndRun(func(l Logger) {
		l.FatalfDepth(ctx, depth+1, format, args...)
	})
}

// V returns whether the given verbosity should be logged
func V(level int32) bool {
	var res bool
	extractLoggerAndRun(func(l Logger) {
		res = l.V(level)
	})
	return res
}

// WithLogTag returns a context with a log tag
func WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	var resCtx context.Context
	extractLoggerAndRun(func(l Logger) {
		resCtx = l.WithLogTag(ctx, name, value)
	})
	return resCtx
}

// Flush the logger
func Flush() {
	extractLoggerAndRun(func(l Logger) {
		l.Flush()
	})
}
