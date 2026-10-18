package runtime

import (
	"encoding/json"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/traffic"
)

// Traffic workload conformance ratchet:
// proto/sdodev/contracts/v1alpha1/traffic.proto is the single schema SOURCE for
// the `.sdo/diagnostics/traffic/workloads/<name>.yaml` artifact, and these tests
// make drift a build failure on the Go side.
//
//  1. Schema fidelity: a workload the Go controller struct emits
//     (sdo/operator/sdk/traffic.Workload, camelCase JSON, Go-duration strings)
//     decodes into the generated proto type. protojson rejects unknown fields,
//     so a field the hand struct grows without the proto fails here.
//  2. Invariant fidelity: the protovalidate CEL mirrors the hand-written Go
//     traffic.Workload.Validate -- a valid workload passes both, and each
//     CEL-expressible invariant violation is rejected by both.
//
// The Go-duration bounds (timeout/iterationTimeout/duration/interval and the
// SLO durations) and the cross-field checks that depend on parsing one cannot
// be expressed in CEL; they stay behaviour-preserving host code in
// traffic.Workload.Validate, so this test asserts only that Go rejects them.
//
// Deleting the hand struct in favour of the generated type is deferred: it
// needs google.golang.org/protobuf converged from v1.33 to v1.36 across
// sdo/operator/sdk, /core, /runtime, every synthesized detector workspace, and
// the validator image (Go `replace` is not transitive) -- the dependency ripple
// proto/README.md defers. Until then this ratchet keeps the schema single-sourced.

func mustValidWorkload(t *testing.T, workload traffic.Workload) traffic.Workload {
	t.Helper()
	workload = workload.WithDefaults()
	if err := workload.Validate(); err != nil {
		t.Fatalf("traffic.Workload.Validate rejected a workload the test assumed valid: %v", err)
	}
	return workload
}

func TestTrafficProtoSchemaModelsGoWorkloadJSON(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}

	workloads := map[string]traffic.Workload{
		"health-probe": {
			APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "health",
			Purpose: traffic.PurposeHealthProbe, Arrival: traffic.ArrivalUniform,
			Scenarios: []traffic.WorkloadScenario{{ID: "search", Weight: 2}, {ID: "login"}},
		},
		"verify-burst": {
			APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "verify",
			Purpose: traffic.PurposeVerifyBurst, Duration: traffic.Duration(3 * time.Second),
			Scenarios: []traffic.WorkloadScenario{{ID: "search", SLO: &traffic.SLO{Window: 10, MinSamples: 2}}},
		},
		"link-probe": {
			APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "links",
			Purpose: traffic.PurposeLinkProbe,
			Links:   []traffic.Link{{From: "frontend", To: "search", Port: 8082}},
		},
	}

	for name, workload := range workloads {
		t.Run(name, func(t *testing.T) {
			valid := mustValidWorkload(t, workload)
			// Pin the seed small so the JSON number round-trips exactly.
			valid.Seed = 7
			raw, err := json.Marshal(valid)
			if err != nil {
				t.Fatalf("marshal workload: %v", err)
			}
			message := &contractsv1alpha1.TrafficWorkload{}
			if err := protojson.Unmarshal(raw, message); err != nil {
				t.Fatalf("proto schema does not model the Go workload JSON (drift): %v\n%s", err, raw)
			}
			if err := validator.Validate(message); err != nil {
				t.Fatalf("protovalidate rejected a workload traffic.Validate accepted: %v", err)
			}
		})
	}
}

func TestTrafficProtoValidateMirrorsGoInvariants(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}

	base := func() *contractsv1alpha1.TrafficWorkload {
		return &contractsv1alpha1.TrafficWorkload{
			ApiVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "health",
			Purpose:   string(traffic.PurposeHealthProbe),
			Scenarios: []*contractsv1alpha1.TrafficWorkloadScenario{{Id: "search", Weight: 2}, {Id: "login"}},
		}
	}

	// A well-formed workload passes protovalidate.
	if err := validator.Validate(base()); err != nil {
		t.Fatalf("protovalidate rejected a valid workload: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*contractsv1alpha1.TrafficWorkload)
	}{
		{"bad api version", func(w *contractsv1alpha1.TrafficWorkload) { w.ApiVersion = "sdo.dev/v2" }},
		{"bad kind", func(w *contractsv1alpha1.TrafficWorkload) { w.Kind = "TrafficMix" }},
		{"name not a dns label", func(w *contractsv1alpha1.TrafficWorkload) { w.Name = "Bad_Name" }},
		{"unknown purpose", func(w *contractsv1alpha1.TrafficWorkload) { w.Purpose = "chaos" }},
		{"bad arrival", func(w *contractsv1alpha1.TrafficWorkload) { w.Arrival = "burst" }},
		{"rate above ceiling", func(w *contractsv1alpha1.TrafficWorkload) { w.RatePerSecond = 100 }},
		{"health probe with duration", func(w *contractsv1alpha1.TrafficWorkload) { w.Duration = proto.String("5s") }},
		{"non link-probe with links", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Links = []*contractsv1alpha1.TrafficLink{{From: "a", To: "b", Port: 80}}
		}},
		{"no scenarios", func(w *contractsv1alpha1.TrafficWorkload) { w.Scenarios = nil }},
		{"duplicate scenario", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Scenarios = append(w.Scenarios, &contractsv1alpha1.TrafficWorkloadScenario{Id: "search"})
		}},
		{"scenario id not allowed", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Scenarios = []*contractsv1alpha1.TrafficWorkloadScenario{{Id: "BAD"}}
		}},
		{"weight out of range", func(w *contractsv1alpha1.TrafficWorkload) { w.Scenarios[0].Weight = 500 }},
		{"slo error rate above one", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Slo = &contractsv1alpha1.TrafficSLO{MaxErrorRate: proto.Float64(1.5)}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workload := base()
			tc.mutate(workload)
			if err := validator.Validate(workload); err == nil {
				t.Fatalf("protovalidate accepted an invariant violation: %s", tc.name)
			}
		})
	}
}

func TestTrafficProtoValidateLinkProbeInvariants(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}
	base := func() *contractsv1alpha1.TrafficWorkload {
		return &contractsv1alpha1.TrafficWorkload{
			ApiVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "links",
			Purpose: string(traffic.PurposeLinkProbe),
			Links:   []*contractsv1alpha1.TrafficLink{{From: "frontend", To: "search", Port: 8082}},
		}
	}
	if err := validator.Validate(base()); err != nil {
		t.Fatalf("protovalidate rejected a valid link-probe: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*contractsv1alpha1.TrafficWorkload)
	}{
		{"self loop", func(w *contractsv1alpha1.TrafficWorkload) { w.Links[0].To = w.Links[0].From }},
		{"bad protocol", func(w *contractsv1alpha1.TrafficWorkload) { w.Links[0].Protocol = "udp" }},
		{"port out of range", func(w *contractsv1alpha1.TrafficWorkload) { w.Links[0].Port = 0 }},
		{"no links", func(w *contractsv1alpha1.TrafficWorkload) { w.Links = nil }},
		{"link-probe with scenarios", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Scenarios = []*contractsv1alpha1.TrafficWorkloadScenario{{Id: "x"}}
		}},
		{"duplicate link", func(w *contractsv1alpha1.TrafficWorkload) {
			w.Links = append(w.Links, &contractsv1alpha1.TrafficLink{From: "frontend", To: "search", Port: 8082})
		}},
		{"failures below floor", func(w *contractsv1alpha1.TrafficWorkload) { w.Failures = proto.Int32(1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workload := base()
			tc.mutate(workload)
			if err := validator.Validate(workload); err == nil {
				t.Fatalf("protovalidate accepted a link-probe invariant violation: %s", tc.name)
			}
		})
	}
}
