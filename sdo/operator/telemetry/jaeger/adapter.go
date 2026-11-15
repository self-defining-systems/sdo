// Package jaeger reads traces from a Jaeger query service (the HTTP API of
// jaeger-query, as served by the all-in-one image). It implements
// telemetry.TraceReader; detectors never query it.
package jaeger

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"sdo.dev/operator/telemetry"
)

// ID is the adapter name bindings use.
const ID = "jaeger"

// Options are the adapter's strictly decoded options. There are none yet.
type Options struct{}

// Register adds the Jaeger adapter to r.
func Register(r *telemetry.Registry) error {
	return telemetry.Register(r, telemetry.Adapter[Options]{
		Name:    ID,
		Kinds:   []telemetry.Kind{telemetry.KindTraces},
		Dialect: "Jaeger query HTTP API (/api/services, /api/traces)",
		Validate: func(_ Options, endpoint telemetry.Endpoint) error {
			return validateEndpoint(endpoint.URL)
		},
		Open: func(_ Options, endpoint telemetry.Endpoint, env telemetry.Env) (telemetry.Client, error) {
			if err := validateEndpoint(endpoint.URL); err != nil {
				return nil, err
			}
			return Client{BaseURL: endpoint.URL, HTTPClient: env.HTTP}, nil
		},
	})
}

func validateEndpoint(address string) error {
	if address == "" {
		return errors.New("jaeger endpoint URL is required")
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("jaeger endpoint %q must be an http or https URL", address)
	}
	return nil
}

// Query refuses: a trace source answers trace reads (sdo traces), not
// detector queries.
func (Client) Query(context.Context, telemetry.Query, time.Time) (telemetry.Result, error) {
	return telemetry.Result{}, errors.New("a jaeger source serves trace reads (sdo traces), not detector queries")
}
