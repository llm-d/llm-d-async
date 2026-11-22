package pubsub

import (
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
)

const registryTimeout = 5 * time.Second

func TestDeliveryRegistry_TokensAreUnique(t *testing.T) {
	var reg deliveryRegistry
	seen := make(map[uint64]bool)
	for range 3 {
		token, _, closeDelivery := reg.open()
		t.Cleanup(closeDelivery)
		if token == 0 {
			t.Fatal("open returned the unset token 0")
		}
		if seen[token] {
			t.Fatalf("token %d issued twice", token)
		}
		seen[token] = true
	}
}

func TestDeliveryRegistry_SignalReachesOnlyItsDelivery(t *testing.T) {
	var reg deliveryRegistry
	tokenA, chA, closeA := reg.open()
	tokenB, chB, closeB := reg.open()
	defer closeB()

	// The first delivery finishes before the second one is signalled.
	reg.signal(logr.Discard(), tokenA, true)
	closeA()
	reg.signal(logr.Discard(), tokenB, false)

	if got := <-chA; !got {
		t.Error("delivery A received false, want true")
	}
	select {
	case got := <-chB:
		if got {
			t.Error("delivery B received true, want false")
		}
	case <-time.After(registryTimeout):
		t.Fatal("delivery B never received its signal")
	}
}

// A retry and a result can both signal one delivery. The second signal must
// return without blocking and must not replace the first outcome.
func TestDeliveryRegistry_SecondSignalDoesNotBlock(t *testing.T) {
	var reg deliveryRegistry
	token, ch, closeDelivery := reg.open()
	defer closeDelivery()

	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{})

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		reg.signal(logger, token, false)
		reg.signal(logger, token, true)
	}()
	select {
	case <-returned:
	case <-time.After(registryTimeout):
		t.Fatal("second signal blocked")
	}

	if got := <-ch; got {
		t.Error("outcome = true, want the first signal (false)")
	}
	if len(logged) != 1 || !strings.Contains(logged[0], "Duplicate signal dropped") {
		t.Errorf("logged %v, want one \"Duplicate signal dropped\" error", logged)
	}
}

func TestDeliveryRegistry_UnknownTokenLogsError(t *testing.T) {
	var reg deliveryRegistry
	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{})

	reg.signal(logger, 42, true)
	token, _, closeDelivery := reg.open()
	closeDelivery()
	reg.signal(logger, token, true)

	if len(logged) != 2 {
		t.Fatalf("logged %d entries, want 2: %v", len(logged), logged)
	}
	for _, entry := range logged {
		if !strings.Contains(entry, "No waiting delivery for signal") {
			t.Errorf("log entry %q does not name the missing delivery", entry)
		}
	}
}

// A result without a token, such as one built by a gate, is reported rather
// than delivered to another request.
func TestDeliveryRegistry_UnsetTokenLogsError(t *testing.T) {
	var reg deliveryRegistry
	_, ch, closeDelivery := reg.open()
	defer closeDelivery()
	var logged []string
	logger := funcr.New(func(_, args string) { logged = append(logged, args) }, funcr.Options{})

	reg.signal(logger, 0, true)

	if len(logged) != 1 {
		t.Fatalf("logged %v, want one error", logged)
	}
	select {
	case got := <-ch:
		t.Fatalf("a registered delivery received %v for the unset token", got)
	default:
	}
}
