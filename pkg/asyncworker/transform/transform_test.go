package transform

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/llm-d/llm-d-async/pkg/plugins"
)

var _ RequestTransform = (*fakeTransform)(nil)

// fakeTransform is a configurable RequestTransform for tests.
type fakeTransform struct {
	name        string
	validateErr error
	handled     bool
	body        []byte
	contentType string
	transformer error

	validateCalls *int
	transformLog  *[]string
}

func (f *fakeTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "fake", Name: f.name}
}

func (f *fakeTransform) Validate(_ []byte, _ map[string]string, _ int64) error {
	if f.validateCalls != nil {
		*f.validateCalls++
	}
	return f.validateErr
}

func (f *fakeTransform) Transform(_ []byte, _ map[string]string) ([]byte, string, bool, error) {
	if f.transformLog != nil {
		*f.transformLog = append(*f.transformLog, f.name)
	}
	if f.transformer != nil {
		return nil, "", false, f.transformer
	}
	if !f.handled {
		return nil, "", false, nil
	}
	return f.body, f.contentType, true, nil
}

func TestChain_NilIsNoOp(t *testing.T) {
	var c *Chain
	if c.Len() != 0 {
		t.Errorf("nil chain Len = %d, want 0", c.Len())
	}
	if err := c.Validate([]byte(`{}`), nil, 0); err != nil {
		t.Errorf("nil chain Validate err = %v, want nil", err)
	}
	body, ct, handled, err := c.Apply([]byte(`{}`), nil)
	if handled || err != nil || body != nil || ct != "" {
		t.Errorf("nil chain Apply = (%q,%q,%v,%v), want all zero/false/nil", body, ct, handled, err)
	}
}

func TestChain_Validate_ReturnsFirstError(t *testing.T) {
	wantErr := errors.New("boom")
	var firstCalls, secondCalls int
	c := NewChain([]RequestTransform{
		&fakeTransform{name: "first", validateErr: wantErr, validateCalls: &firstCalls},
		&fakeTransform{name: "second", validateCalls: &secondCalls},
	})
	if err := c.Validate([]byte(`{}`), nil, 0); !errors.Is(err, wantErr) {
		t.Errorf("Validate err = %v, want %v", err, wantErr)
	}
	if firstCalls != 1 {
		t.Errorf("first Validate called %d times, want 1", firstCalls)
	}
	if secondCalls != 0 {
		t.Errorf("second Validate called %d times, want 0 (short-circuit)", secondCalls)
	}
}

func TestChain_Apply_FirstHandledWinsAndShortCircuits(t *testing.T) {
	var log []string
	c := NewChain([]RequestTransform{
		&fakeTransform{name: "skip", handled: false, transformLog: &log},
		&fakeTransform{name: "win", handled: true, body: []byte("WIN"), contentType: "multipart/form-data; boundary=x", transformLog: &log},
		&fakeTransform{name: "never", handled: true, body: []byte("NEVER"), transformLog: &log},
	})
	body, ct, handled, err := c.Apply([]byte(`{}`), nil)
	if err != nil || !handled {
		t.Fatalf("Apply = handled %v err %v, want handled true err nil", handled, err)
	}
	if string(body) != "WIN" || ct != "multipart/form-data; boundary=x" {
		t.Errorf("Apply body=%q ct=%q, want WIN / multipart...", body, ct)
	}
	want := []string{"skip", "win"}
	if len(log) != len(want) || log[0] != want[0] || log[1] != want[1] {
		t.Errorf("transform call order = %v, want %v (never should not run)", log, want)
	}
}

func TestChain_Apply_NoneHandled(t *testing.T) {
	c := NewChain([]RequestTransform{&fakeTransform{name: "a"}, &fakeTransform{name: "b"}})
	_, _, handled, err := c.Apply([]byte(`{}`), nil)
	if handled || err != nil {
		t.Errorf("Apply = handled %v err %v, want false/nil", handled, err)
	}
}

func TestChain_Apply_ErrorShortCircuits(t *testing.T) {
	wantErr := errors.New("transform failed")
	c := NewChain([]RequestTransform{&fakeTransform{name: "a", transformer: wantErr}})
	_, _, handled, err := c.Apply([]byte(`{}`), nil)
	if handled {
		t.Error("handled should be false on error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
}

// registerFake registers a factory under typ that yields a handled transform,
// and cleans it up after the test.
func registerFake(t *testing.T, typ string) {
	t.Helper()
	if err := plugins.Register(typ, func(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
		return &fakeTransform{name: name, handled: true, body: []byte("x")}, nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	t.Cleanup(func() { plugins.Unregister(typ) })
}

func TestLoadConfigAndBuildChain(t *testing.T) {
	const typ = "transform_test.multipart"
	registerFake(t, typ)

	dir := t.TempDir()
	path := filepath.Join(dir, "transforms.json")
	cfg := `{"requestTransforms":[{"name":"whisper","type":"transform_test.multipart","parameters":{"provider":"whisper"}}]}`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	specs := loaded.RequestTransforms
	if len(specs) != 1 || specs[0].Name != "whisper" || specs[0].Type != typ {
		t.Fatalf("unexpected specs: %+v", specs)
	}

	handle := plugins.NewHandle(context.Background())
	chain, err := BuildChain(specs, handle)
	if err != nil {
		t.Fatalf("BuildChain: %v", err)
	}
	if chain.Len() != 1 {
		t.Errorf("chain Len = %d, want 1", chain.Len())
	}
	if handle.Plugin("whisper") == nil {
		t.Error("plugin 'whisper' not registered on handle")
	}
}

func TestLoadConfig_RejectsUnknownTopLevelFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "transforms.json")
	// responseTransforms is not supported yet (#259); a typo or premature use
	// must fail loudly rather than be silently dropped.
	cfg := `{"requestTransforms":[],"responseTransforms":[]}`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadConfig(path); err == nil {
		t.Error("LoadConfig with unknown top-level field = nil error, want error")
	}
}

func TestBuildChain_Errors(t *testing.T) {
	const typ = "transform_test.ok"
	registerFake(t, typ)

	tests := []struct {
		name  string
		specs []PluginSpec
	}{
		{"empty name", []PluginSpec{{Type: typ}}},
		{"missing type", []PluginSpec{{Name: "a"}}},
		{"unregistered type", []PluginSpec{{Name: "a", Type: "nope.nope"}}},
		{"duplicate name", []PluginSpec{{Name: "a", Type: typ}, {Name: "a", Type: typ}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := BuildChain(tc.specs, plugins.NewHandle(context.Background())); err == nil {
				t.Errorf("BuildChain(%s) = nil error, want error", tc.name)
			}
		})
	}
}

var _ plugins.Plugin = nonTransformPlugin{}

// nonTransformPlugin implements plugins.Plugin but not RequestTransform.
type nonTransformPlugin struct{}

func (nonTransformPlugin) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "nontransform", Name: "n"}
}

func TestBuildChain_NotARequestTransform(t *testing.T) {
	const typ = "transform_test.nontransform"
	if err := plugins.Register(typ, func(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
		return nonTransformPlugin{}, nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	t.Cleanup(func() { plugins.Unregister(typ) })

	_, err := BuildChain([]PluginSpec{{Name: "n", Type: typ}}, plugins.NewHandle(context.Background()))
	if err == nil {
		t.Fatal("expected error for plugin that does not implement RequestTransform")
	}
}

func TestBuildChain_Empty(t *testing.T) {
	chain, err := BuildChain(nil, plugins.NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("BuildChain(nil): %v", err)
	}
	if chain.Len() != 0 {
		t.Errorf("empty chain Len = %d, want 0", chain.Len())
	}
}
