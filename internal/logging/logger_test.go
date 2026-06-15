package logging

import (
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func TestInitLogging_defaultVerbosity(t *testing.T) {
	opts := zap.Options{Development: true}
	InitLogging(&opts, DEFAULT)

	logger := ctrl.Log.WithName("test-default")
	if logger.GetSink() == nil {
		t.Error("expected logger sink to be initialized")
	}
}

func TestInitLogging_verbosityLevels(t *testing.T) {
	levels := []struct {
		name      string
		verbosity int
	}{
		{"DEFAULT", DEFAULT},
		{"VERBOSE", VERBOSE},
		{"DEBUG", DEBUG},
		{"TRACE", TRACE},
	}
	for _, tt := range levels {
		t.Run(tt.name, func(t *testing.T) {
			opts := zap.Options{Development: true}
			InitLogging(&opts, tt.verbosity)

			logger := ctrl.Log.WithName("test-" + tt.name)
			if logger.GetSink() == nil {
				t.Error("expected logger sink to be initialized")
			}
		})
	}
}

func TestInitLogging_highVerbosityClamped(t *testing.T) {
	opts := zap.Options{Development: true}
	InitLogging(&opts, 200)

	logger := ctrl.Log.WithName("test-clamped")
	if logger.GetSink() == nil {
		t.Error("expected logger sink to be initialized after clamping")
	}
}

func TestSync_doesNotPanic(t *testing.T) {
	opts := zap.Options{Development: true}
	InitLogging(&opts, DEFAULT)

	// Sync may return benign errors on some platforms (e.g. stderr sinks);
	// we only verify it doesn't panic.
	_ = Sync()
}
