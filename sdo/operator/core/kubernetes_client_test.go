package core

import (
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestClientRateLimitsDefaultAboveClientGoDefaults(t *testing.T) {
	config := &rest.Config{}
	if err := applyClientRateLimits(config, lookupFrom(nil)); err != nil {
		t.Fatalf("apply defaults: %v", err)
	}
	if config.QPS != DefaultKubernetesClientQPS || config.Burst != DefaultKubernetesClientBurst {
		t.Fatalf("got qps=%v burst=%d, want %v/%d",
			config.QPS, config.Burst, DefaultKubernetesClientQPS, DefaultKubernetesClientBurst)
	}
	if DefaultKubernetesClientQPS <= rest.DefaultQPS || DefaultKubernetesClientBurst <= rest.DefaultBurst {
		t.Fatal("controller defaults must exceed client-go's QPS 5 / burst 10")
	}
}

func TestClientRateLimitsHonorEnvironmentOverrides(t *testing.T) {
	config := &rest.Config{}
	err := applyClientRateLimits(config, lookupFrom(map[string]string{
		KubernetesClientQPSEnv: "12.5", KubernetesClientBurstEnv: "25",
	}))
	if err != nil {
		t.Fatalf("apply overrides: %v", err)
	}
	if config.QPS != 12.5 || config.Burst != 25 {
		t.Fatalf("got qps=%v burst=%d, want 12.5/25", config.QPS, config.Burst)
	}
}

func TestClientRateLimitsRejectInvalidEnvironment(t *testing.T) {
	for _, test := range []struct {
		name   string
		values map[string]string
	}{
		{"non-numeric qps", map[string]string{KubernetesClientQPSEnv: "fast"}},
		{"zero qps", map[string]string{KubernetesClientQPSEnv: "0"}},
		{"negative qps", map[string]string{KubernetesClientQPSEnv: "-1"}},
		{"infinite qps", map[string]string{KubernetesClientQPSEnv: "Inf"}},
		{"non-numeric burst", map[string]string{KubernetesClientBurstEnv: "many"}},
		{"zero burst", map[string]string{KubernetesClientBurstEnv: "0"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := applyClientRateLimits(&rest.Config{}, lookupFrom(test.values))
			if err == nil || !strings.Contains(err.Error(), "SDO_KUBE_API_") {
				t.Fatalf("expected actionable error naming the variable, got %v", err)
			}
		})
	}
}
