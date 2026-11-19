package plugins

import (
	"context"
	"encoding/json"
	"testing"
)

var _ Plugin = (*stubPlugin)(nil)

type stubPlugin struct {
	name string
}

func (s *stubPlugin) TypedName() TypedName { return TypedName{Type: "stub", Name: s.name} }

func TestTypedName_String(t *testing.T) {
	tn := TypedName{Type: "multipart", Name: "whisper"}
	if got, want := tn.String(), "whisper/multipart"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestRegistry_RegisterAndLookup(t *testing.T) {
	// Use a unique type so we don't collide with other tests/packages.
	const typ = "plugins_test.stub"
	called := false
	if err := Register(typ, func(name string, _ json.RawMessage, _ Handle) (Plugin, error) {
		called = true
		return &stubPlugin{name: name}, nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	t.Cleanup(func() { delete(registry, typ) })

	factory, ok := Lookup(typ)
	if !ok {
		t.Fatalf("type %q not found after Register", typ)
	}
	p, err := factory("inst", nil, NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	if !called {
		t.Error("factory was not invoked")
	}
	if p.TypedName().Name != "inst" {
		t.Errorf("plugin name = %q, want %q", p.TypedName().Name, "inst")
	}
}

func TestRegistry_DuplicateRegistrationFails(t *testing.T) {
	const typ = "plugins_test.dup"
	factory := func(name string, _ json.RawMessage, _ Handle) (Plugin, error) {
		return &stubPlugin{name: name}, nil
	}
	if err := Register(typ, factory); err != nil {
		t.Fatalf("first Register returned error: %v", err)
	}
	t.Cleanup(func() { delete(registry, typ) })

	if err := Register(typ, factory); err == nil {
		t.Error("expected error registering duplicate type, got nil")
	}
}

func TestHandle_AddAndGet(t *testing.T) {
	h := NewHandle(context.Background())
	if h.Context() == nil {
		t.Error("Context() returned nil")
	}
	if h.Plugin("missing") != nil {
		t.Error("Plugin(missing) should be nil")
	}

	p := &stubPlugin{name: "a"}
	h.AddPlugin("a", p)
	if h.Plugin("a") != p {
		t.Error("Plugin(a) did not return the added plugin")
	}
}
