/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package otel

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInitTracerDisabledWhenNoEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	ctx := logr.NewContext(context.Background(), logr.Discard())
	shutdown, err := InitTracer(ctx)
	if err != nil {
		t.Fatalf("InitTracer returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown function")
	}
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
}

func TestStartSpanCreatesSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background()) //nolint:errcheck
	otel.SetTracerProvider(tp)

	ctx := context.Background()
	_, span := StartSpan(ctx, "test-span")
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "test-span" {
		t.Errorf("expected span name 'test-span', got %q", spans[0].Name)
	}
}

func TestSetAttrAddsAttributes(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background()) //nolint:errcheck
	otel.SetTracerProvider(tp)

	ctx := context.Background()
	ctx, span := StartSpan(ctx, "attr-span")
	SetAttr(ctx, attribute.String("key", "value"))
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	found := false
	for _, attr := range spans[0].Attributes {
		if string(attr.Key) == "key" && attr.Value.AsString() == "value" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected attribute key=value not found on span")
	}
}

func TestDetachedContextCreatesLinkedSpan(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	defer tp.Shutdown(context.Background()) //nolint:errcheck
	otel.SetTracerProvider(tp)

	ctx := logr.NewContext(context.Background(), logr.Discard())
	ctx, parentSpan := StartSpan(ctx, "parent")
	parentSC := parentSpan.SpanContext()

	_, childSpan := DetachedContext(ctx, "detached")
	childSpan.End()
	parentSpan.End()

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// Find the detached span and verify it has a link to the parent
	for _, s := range spans {
		if s.Name == "detached" {
			if len(s.Links) == 0 {
				t.Fatal("detached span has no links")
			}
			if s.Links[0].SpanContext.TraceID() != parentSC.TraceID() {
				t.Error("detached span link does not reference parent trace")
			}
			// Detached span should be on a different trace
			if s.SpanContext.TraceID() == parentSC.TraceID() {
				t.Error("detached span should be on a different trace than parent")
			}
			return
		}
	}
	t.Fatal("detached span not found")
}
