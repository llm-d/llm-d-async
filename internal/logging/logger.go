package logging

import (
	"flag"

	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	DEFAULT = 2
	VERBOSE = 3
	DEBUG   = 4
	TRACE   = 5
)

// InitLogging initializes the controller-runtime logger with zap backend.
func InitLogging(opts *zap.Options, logVerbosity int) {
	// Unless -zap-log-level is explicitly set, use -v
	useV := true
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "zap-log-level" {
			useV = false
		}
	})
	if useV {
		// See https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/log/zap#Options.Level
		lvl := -1 * (logVerbosity)
		if lvl < -128 {
			lvl = -128
		}
		opts.Level = uberzap.NewAtomicLevelAt(zapcore.Level(int8(lvl))) // #nosec G115 -- clamped above
	}

	logger := zap.New(zap.UseFlagOptions(opts), zap.RawZapOpts(uberzap.AddCaller()))
	ctrl.SetLogger(logger)
}

// Sync flushes any buffered log entries.
func Sync() error {
	logger := ctrl.Log.WithName("logger-sync").GetSink()
	if syncer, ok := logger.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return nil
}
