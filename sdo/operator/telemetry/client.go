package telemetry

import (
	"context"
	"net/http"
	"time"

	"sdo.dev/operator/sdk/metrics"
)

// Client answers queries for one bound source. Implementations must honor ctx
// and must be safe for concurrent use. The runtime also enforces QueryTimeout
// around every call, so a client that ignores ctx cannot stall a cycle.
type Client interface {
	Query(ctx context.Context, q Query, at time.Time) (Result, error)
}

// AlertReader is the optional interface of a client whose source advertises
// CapAlerts.
type AlertReader interface {
	Alerts(ctx context.Context) ([]metrics.Alert, error)
}

// Endpoint is where a source lives. Credential names an environment-granted
// credential; it is never a value.
type Endpoint struct {
	URL        string
	Credential string
}

// HTTPDoer is the part of *http.Client adapters need.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Env is what Open may use from the process. Only the runtime constructs a
// non-zero Env; Validate never receives one.
type Env struct {
	// HTTP is the transport adapters use for network calls. It is injected so
	// the package holds no ambient network client; the runtime supplies one.
	HTTP HTTPDoer
	// ReadCredential returns the current value of a granted credential, read
	// per request so rotation needs no restart.
	ReadCredential func(name string) (string, error)
}
