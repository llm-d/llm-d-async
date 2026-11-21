package server

import (
	"fmt"
	"net/http"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/rest"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// metricsPath is where controller-runtime's metrics server serves the registry.
const metricsPath = "/metrics"

// openMetricsFilterProvider returns the metrics server's FilterProvider.
//
// Trace exemplars only exist in the OpenMetrics format. controller-runtime v0.22
// builds its /metrics handler without EnableOpenMetrics and has no option to
// change that, so the filter serves /metrics from a handler that negotiates
// OpenMetrics instead; scrapers that don't ask for it still get the classic
// format. controller-runtime runs the filter over every handler it mounts, so
// other paths go to the handler it built. With auth on, the authentication and
// authorization filter wraps the result as before.
//
// controller-runtime v0.25.2 adds Options.HandlerOpts; once this module is on
// it, setting EnableOpenMetrics there replaces this filter.
func openMetricsFilterProvider(authEnabled bool) func(*rest.Config, *http.Client) (metricsserver.Filter, error) {
	return func(c *rest.Config, httpClient *http.Client) (metricsserver.Filter, error) {
		var authFilter metricsserver.Filter
		if authEnabled {
			var err error
			if authFilter, err = filters.WithAuthenticationAndAuthorization(c, httpClient); err != nil {
				return nil, fmt.Errorf("failed to create the metrics auth filter: %w", err)
			}
		}
		openMetrics := promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{
			ErrorHandling:     promhttp.HTTPErrorOnError,
			EnableOpenMetrics: true,
		})
		return func(log logr.Logger, next http.Handler) (http.Handler, error) {
			var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != metricsPath {
					next.ServeHTTP(w, r)
					return
				}
				openMetrics.ServeHTTP(w, r)
			})
			if authFilter == nil {
				return handler, nil
			}
			return authFilter(log, handler)
		}, nil
	}
}
