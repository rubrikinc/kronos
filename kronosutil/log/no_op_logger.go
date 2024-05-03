package log

import "context"

type noOpLogger struct {
}

func (n noOpLogger) Info(ctx context.Context, args ...interface{}) {

}

func (n noOpLogger) Infof(ctx context.Context, format string, args ...interface{}) {

}

func (n noOpLogger) InfofDepth(ctx context.Context, depth int, format string, args ...interface{}) {

}

func (n noOpLogger) Warning(ctx context.Context, args ...interface{}) {

}

func (n noOpLogger) Warningf(ctx context.Context, format string, args ...interface{}) {

}

func (n noOpLogger) WarningfDepth(ctx context.Context, depth int, format string, args ...interface{}) {

}

func (n noOpLogger) Error(ctx context.Context, args ...interface{}) {

}

func (n noOpLogger) Errorf(ctx context.Context, format string, args ...interface{}) {

}

func (n noOpLogger) ErrorfDepth(ctx context.Context, depth int, format string, args ...interface{}) {

}

func (n noOpLogger) Fatal(ctx context.Context, args ...interface{}) {

}

func (n noOpLogger) Fatalf(ctx context.Context, format string, args ...interface{}) {
}

func (n noOpLogger) FatalfDepth(ctx context.Context, depth int, format string, args ...interface{}) {
}

func (n noOpLogger) V(level int32) bool {
	return false
}

func (n noOpLogger) WithLogTag(ctx context.Context, name string, value interface{}) context.Context {
	return ctx
}

func (n noOpLogger) Flush() {
}

var NoOplogger = &noOpLogger{}
