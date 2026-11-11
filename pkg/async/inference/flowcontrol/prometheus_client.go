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
	"net"
	"net/http"
	"os"
	"time"

	promconfig "github.com/prometheus/common/config"
)

// NewPrometheusRoundTripper returns the HTTP transport Prometheus gates use to
// reach an authenticated, TLS-terminated Prometheus API such as OpenShift's
// Thanos Querier. caFile adds a PEM CA bundle for verifying the server and is
// reloaded when its contents change; bearerTokenFile is read on every request,
// so rotated tokens (for example projected service account tokens) are picked
// up without a restart. Both files must be readable when this is called. With
// both empty it returns nil, which keeps the Prometheus client's default
// transport.
//
// Like that default transport, the returned one honors the proxy environment
// variables and bounds connection attempts to 30 seconds.
func NewPrometheusRoundTripper(caFile, bearerTokenFile string) (http.RoundTripper, error) {
	if caFile == "" && bearerTokenFile == "" {
		return nil, nil
	}
	cfg := promconfig.DefaultHTTPClientConfig
	cfg.ProxyFromEnvironment = true
	cfg.TLSConfig.CAFile = caFile
	if bearerTokenFile != "" {
		// The library only reads the token on the first request; check it
		// now so a wrong path fails at startup instead of closing the gates.
		if _, err := os.ReadFile(bearerTokenFile); err != nil {
			return nil, fmt.Errorf("failed to read Prometheus bearer token file: %w", err)
		}
		cfg.Authorization = &promconfig.Authorization{Type: "Bearer", CredentialsFile: bearerTokenFile}
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Prometheus client config: %w", err)
	}
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	rt, err := promconfig.NewRoundTripperFromConfig(cfg, "llm-d-async-prometheus-gates",
		promconfig.WithDialContextFunc(dialer.DialContext))
	if err != nil {
		return nil, fmt.Errorf("failed to build Prometheus client transport: %w", err)
	}
	return rt, nil
}
