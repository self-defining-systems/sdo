package prometheus

import (
	"errors"
	"fmt"
	"net/url"

	"sdo.dev/operator/telemetry"
)

// ID is the adapter name bindings use.
const ID = "prometheus"

// Options are the adapter's strictly decoded options. There are none yet.
type Options struct{}

// Register adds the Prometheus adapter to r.
func Register(r *telemetry.Registry) error {
	return telemetry.Register(r, telemetry.Adapter[Options]{
		Name:    ID,
		Kinds:   []telemetry.Kind{telemetry.KindMetrics},
		Dialect: "PromQL (Prometheus HTTP API v1)",
		Caps:    []telemetry.Capability{telemetry.CapAlerts, telemetry.CapRange},
		Validate: func(_ Options, endpoint telemetry.Endpoint) error {
			return validateEndpoint(endpoint.URL)
		},
		Open: open,
	})
}

func validateEndpoint(address string) error {
	if address == "" {
		return errors.New("prometheus endpoint URL is required")
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("prometheus endpoint %q must be an http or https URL", address)
	}
	return nil
}

func open(_ Options, endpoint telemetry.Endpoint, env telemetry.Env) (telemetry.Client, error) {
	if err := validateEndpoint(endpoint.URL); err != nil {
		return nil, err
	}
	client := Client{BaseURL: endpoint.URL, HTTPClient: env.HTTP}
	if endpoint.Credential != "" {
		if env.ReadCredential == nil {
			return nil, fmt.Errorf("credential %q is named but the environment grants none", endpoint.Credential)
		}
		name := endpoint.Credential
		client.Token = func() (string, error) { return env.ReadCredential(name) }
	}
	return client, nil
}
