package core

import (
	"fmt"
	"math"
	"strconv"

	"k8s.io/client-go/rest"
)

const (
	// DefaultKubernetesClientQPS and DefaultKubernetesClientBurst replace
	// client-go's QPS 5 / burst 10, which spaced controller steps 400 ms apart
	// under event bursts. Informers use watches, so this budget covers state
	// writes, responder Jobs, lease renewal, and validator reads.
	DefaultKubernetesClientQPS   float32 = 50
	DefaultKubernetesClientBurst int     = 100

	KubernetesClientQPSEnv   = "SDO_KUBE_API_QPS"
	KubernetesClientBurstEnv = "SDO_KUBE_API_BURST"
)

// applyClientRateLimits sets the client-side rate limit from the environment,
// falling back to the controller defaults. Invalid values are rejected rather
// than silently replaced so a misconfigured controller fails at startup.
func applyClientRateLimits(config *rest.Config, lookupEnv func(string) (string, bool)) error {
	qps := DefaultKubernetesClientQPS
	if raw, ok := lookupEnv(KubernetesClientQPSEnv); ok {
		parsed, err := strconv.ParseFloat(raw, 32)
		if err != nil || parsed <= 0 || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
			return fmt.Errorf("%s must be a positive finite number, got %q", KubernetesClientQPSEnv, raw)
		}
		qps = float32(parsed)
	}
	burst := DefaultKubernetesClientBurst
	if raw, ok := lookupEnv(KubernetesClientBurstEnv); ok {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return fmt.Errorf("%s must be a positive integer, got %q", KubernetesClientBurstEnv, raw)
		}
		burst = parsed
	}
	config.QPS = qps
	config.Burst = burst
	return nil
}
