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

var _ ResponseTransform = (*fakeResponseTransform)(nil)

// fakeResponseTransform is a configurable ResponseTransform for tests.
type fakeResponseTransform struct {
	name         string
	handled      bool
	body         []byte
	contentType  string
	err          error
	transformLog *[]string
}

func (f *fakeResponseTransform) TypedName() plugins.TypedName {
	return plugins.TypedName{Type: "fake-response", Name: f.name}
}

func (f *fakeResponseTransform) Transform(_ context.Context, _ []byte, _ map[string]string) ([]byte, string, bool, error) {
	if f.transformLog != nil {
		*f.transformLog = append(*f.transformLog, f.name)
	}
	if f.err != nil {
		return nil, "", false, f.err
	}
	if !f.handled {
		return nil, "", false, nil
	}
	return f.body, f.contentType, true, nil
}

func TestChain_ApplyResponse_NilIsNoOp(t *testing.T) {
	var c *Chain
	if c.ResponseLen() != 0 {
		t.Errorf("nil chain ResponseLen = %d, want 0", c.ResponseLen())
	}
	body, contentType, handled, err := c.ApplyResponse(context.Background(), []byte(`{}`), nil)
	if handled || err != nil || body != nil || contentType != "" {
		t.Errorf("nil chain ApplyResponse = (%q,%q,%v,%v), want nil/\"\"/false/nil", body, contentType, handled, err)
	}
}

func TestChain_ApplyResponse_FirstHandledWinsAndShortCircuits(t *testing.T) {
	var log []string
	c := NewChain(nil,
		&fakeResponseTransform{name: "skip", transformLog: &log},
		&fakeResponseTransform{name: "win", handled: true, body: []byte("WIN"), contentType: "text/plain", transformLog: &log},
		&fakeResponseTransform{name: "never", handled: true, body: []byte("NEVER"), transformLog: &log},
	)
	body, contentType, handled, err := c.ApplyResponse(context.Background(), []byte(`{}`), nil)
	if err != nil || !handled {
		t.Fatalf("ApplyResponse = handled %v err %v, want handled true err nil", handled, err)
	}
	if string(body) != "WIN" {
		t.Errorf("ApplyResponse body = %q, want WIN", body)
	}
	if contentType != "text/plain" {
		t.Errorf("ApplyResponse contentType = %q, want text/plain", contentType)
	}
	want := []string{"skip", "win"}
	if len(log) != len(want) || log[0] != want[0] || log[1] != want[1] {
		t.Errorf("transform call order = %v, want %v (never should not run)", log, want)
	}
}

func TestChain_ApplyResponse_NoneHandled(t *testing.T) {
	c := NewChain(nil, &fakeResponseTransform{name: "a"}, &fakeResponseTransform{name: "b"})
	_, _, handled, err := c.ApplyResponse(context.Background(), []byte(`{}`), nil)
	if handled || err != nil {
		t.Errorf("ApplyResponse = handled %v err %v, want false/nil", handled, err)
	}
}

func TestChain_ApplyResponse_ErrorShortCircuits(t *testing.T) {
	wantErr := errors.New("response transform failed")
	var log []string
	c := NewChain(nil,
		&fakeResponseTransform{name: "a", err: wantErr, transformLog: &log},
		&fakeResponseTransform{name: "b", handled: true, transformLog: &log},
	)
	_, _, handled, err := c.ApplyResponse(context.Background(), []byte(`{}`), nil)
	if handled {
		t.Error("handled should be false on error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("err = %v, want %v", err, wantErr)
	}
	if len(log) != 1 {
		t.Errorf("transform call order = %v, want only [a]", log)
	}
}

// registerFakeResponse registers a factory under typ that yields a handled
// response transform, and cleans it up after the test.
func registerFakeResponse(t *testing.T, typ string) {
	t.Helper()
	if err := plugins.Register(typ, func(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
		return &fakeResponseTransform{name: name, handled: true, body: []byte("x")}, nil
	}); err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	t.Cleanup(func() { plugins.Unregister(typ) })
}

func TestLoadConfigAndBuildChain_BothDirections(t *testing.T) {
	const reqTyp = "transform_test.both.request"
	const respTyp = "transform_test.both.response"
	registerFake(t, reqTyp)
	registerFakeResponse(t, respTyp)

	path := filepath.Join(t.TempDir(), "transforms.json")
	cfg := `{"requestTransforms":[{"name":"req","type":"transform_test.both.request"}],` +
		`"responseTransforms":[{"name":"resp","type":"transform_test.both.response"}]}`
	if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	handle := plugins.NewHandle(context.Background())
	chain, err := BuildChainFromConfig(loaded, handle)
	if err != nil {
		t.Fatalf("BuildChainFromConfig: %v", err)
	}
	if chain.Len() != 1 || chain.ResponseLen() != 1 {
		t.Errorf("chain Len = %d ResponseLen = %d, want 1/1", chain.Len(), chain.ResponseLen())
	}
	if handle.Plugin("resp") == nil {
		t.Error("plugin 'resp' not registered on handle")
	}
}

func TestBuildChainFromConfig_DuplicateNameAcrossDirections(t *testing.T) {
	const reqTyp = "transform_test.dup.request"
	const respTyp = "transform_test.dup.response"
	registerFake(t, reqTyp)
	registerFakeResponse(t, respTyp)

	_, err := BuildChainFromConfig(Config{
		RequestTransforms:  []PluginSpec{{Name: "same", Type: reqTyp}},
		ResponseTransforms: []PluginSpec{{Name: "same", Type: respTyp}},
	}, plugins.NewHandle(context.Background()))
	if err == nil {
		t.Fatal("expected error for a plugin name reused across directions")
	}
}

func TestBuildChainFromConfig_NotAResponseTransform(t *testing.T) {
	// registerFake yields a RequestTransform, which is not a ResponseTransform.
	const typ = "transform_test.notresponse"
	registerFake(t, typ)

	_, err := BuildChainFromConfig(Config{
		ResponseTransforms: []PluginSpec{{Name: "n", Type: typ}},
	}, plugins.NewHandle(context.Background()))
	if err == nil {
		t.Fatal("expected error for plugin that does not implement ResponseTransform")
	}
}

func TestBuildChainFromConfig_Empty(t *testing.T) {
	chain, err := BuildChainFromConfig(Config{}, plugins.NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("BuildChainFromConfig(empty): %v", err)
	}
	if chain.Len() != 0 || chain.ResponseLen() != 0 {
		t.Errorf("empty chain Len = %d ResponseLen = %d, want 0/0", chain.Len(), chain.ResponseLen())
	}
}
