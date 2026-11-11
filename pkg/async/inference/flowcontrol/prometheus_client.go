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

package flowcontrol

import (
	"fmt"
	"net/http"

	promconfig "github.com/prometheus/common/config"
)

// NewPrometheusRoundTripper returns the HTTP transport Prometheus gates use to
// reach an authenticated, TLS-terminated Prometheus API such as OpenShift's
// Thanos Querier. caFile adds a PEM CA bundle for verifying the server and is
// reloaded when its contents change; bearerTokenFile is read on every request,
// so rotated tokens (for example projected service account tokens) are picked
// up without a restart. With both empty it returns nil, which keeps the
// Prometheus client's default transport.
func NewPrometheusRoundTripper(caFile, bearerTokenFile string) (http.RoundTripper, error) {
	if caFile == "" && bearerTokenFile == "" {
		return nil, nil
	}
	cfg := promconfig.DefaultHTTPClientConfig
	cfg.TLSConfig.CAFile = caFile
	if bearerTokenFile != "" {
		cfg.Authorization = &promconfig.Authorization{Type: "Bearer", CredentialsFile: bearerTokenFile}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Prometheus client config: %w", err)
	}
	rt, err := promconfig.NewRoundTripperFromConfig(cfg, "llm-d-async-prometheus-gates")
	if err != nil {
		return nil, fmt.Errorf("failed to build Prometheus client transport: %w", err)
	}
	return rt, nil
}
