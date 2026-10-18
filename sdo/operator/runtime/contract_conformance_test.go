package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse time %q: %v", value, err)
	}
	return parsed
}

// Stage 2a conformance ratchet: the proto
// messages are the single schema SOURCE for the controller<->responder tree,
// and these tests make drift a build failure.
//
//  1. Schema fidelity: every golden fixture the Go controller writes and the
//     Python Pydantic models parse (tests/fixtures/sdo/contracts/go/, pinned by
//     TestGoContractFixtures) decodes into the generated proto type. protojson
//     rejects unknown fields, so a field either hand-written definition adds
//     without the proto fails here.
//  2. Invariant fidelity: the protovalidate CEL mirrors the hand-written
//     Pydantic validators and Go Validate/ValidateFor — valid fixtures pass,
//     and each invariant violation is rejected.
//
// This stage does not change the wire or the fenced verify_diagnosis path; the
// cutover that replaces the Pydantic/Go structs with these types is Stage 2b.

func mustProtoJSON(t *testing.T, name string, message proto.Message) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(goContractFixtureDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if err := protojson.Unmarshal(raw, message); err != nil {
		t.Fatalf("proto schema does not model %s (drift): %v", name, err)
	}
}

func TestProtoSchemaParsesGoldenFixtures(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}

	request := &contractsv1alpha1.IncidentRequest{}
	mustProtoJSON(t, "incident_request.json", request)
	if err := validator.Validate(request); err != nil {
		t.Fatalf("incident_request.json fails protovalidate: %v", err)
	}

	for _, name := range []string{"incident_view_empty.json", "incident_view_open.json", "incident_view_populated.json"} {
		view := &contractsv1alpha1.IncidentView{}
		mustProtoJSON(t, name, view)
		if err := validator.Validate(view); err != nil {
			t.Fatalf("%s fails protovalidate: %v", name, err)
		}
	}

	for _, name := range []string{"closure_repaired.json", "closure_own_edit.json"} {
		closure := &contractsv1alpha1.IncidentClosure{}
		mustProtoJSON(t, name, closure)
		if err := validator.Validate(closure); err != nil {
			t.Fatalf("%s fails protovalidate: %v", name, err)
		}
	}
}

func TestProtoValidateMirrorsHandInvariants(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("new validator: %v", err)
	}
	early := timestamppb.New(mustTime(t, "2026-01-01T00:00:00Z"))
	late := timestamppb.New(mustTime(t, "2026-01-01T00:01:00Z"))

	cases := []struct {
		name    string
		message proto.Message
	}{
		{
			// TimingMetrics.validate_order / IncidentResult timing check.
			name:    "timing completed before started",
			message: &contractsv1alpha1.TimingMetrics{StartedAt: late, CompletedAt: early},
		},
		{
			// RepairActionReceipt.validate_order.
			name: "repair action completed before started",
			message: &contractsv1alpha1.RepairActionReceipt{
				ActionId: "a", Kind: "kubectl", Target: "x", Summary: "s", Details: "d",
				StartedAt: late, CompletedAt: early,
			},
		},
		{
			// DetectorTimelineEntry._ordered_timestamps.
			name: "timeline last_seen before first_activated",
			message: &contractsv1alpha1.DetectorTimelineEntry{
				DetectorId: "d", Fingerprint: "f", Relation: "no_incident", Activations: 1,
				FirstActivatedAt: late, LastSeenAt: early,
			},
		},
		{
			// UsageMetrics.validate_cached_input: reasoning must not exceed output.
			name:    "usage reasoning exceeds output",
			message: &contractsv1alpha1.UsageMetrics{OutputTokens: 1, ReasoningOutputTokens: 2},
		},
		{
			// IncidentResult.validate_repair_action_ids: ids must be unique.
			name: "duplicate repair action ids",
			message: &contractsv1alpha1.IncidentResult{
				SchemaVersion: "sdo.dev/v1alpha1", IncidentId: "i", Status: "completed",
				Usage:  &contractsv1alpha1.UsageMetrics{},
				Timing: &contractsv1alpha1.TimingMetrics{StartedAt: early, CompletedAt: late},
				RepairActions: []*contractsv1alpha1.RepairActionReceipt{
					{ActionId: "dup", Kind: "k", Target: "t", Summary: "s", Details: "d", StartedAt: early, CompletedAt: late},
					{ActionId: "dup", Kind: "k", Target: "t", Summary: "s", Details: "d", StartedAt: early, CompletedAt: late},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validator.Validate(tc.message); err == nil {
				t.Fatalf("protovalidate accepted an invariant violation: %s", tc.name)
			}
		})
	}

	// A well-formed IncidentResult with unique ids and ordered timing passes.
	ok := &contractsv1alpha1.IncidentResult{
		SchemaVersion: "sdo.dev/v1alpha1", IncidentId: "i", Status: "completed",
		Usage:  &contractsv1alpha1.UsageMetrics{InputTokens: 10, OutputTokens: 4, ReasoningOutputTokens: 1},
		Timing: &contractsv1alpha1.TimingMetrics{StartedAt: early, CompletedAt: late},
		RepairActions: []*contractsv1alpha1.RepairActionReceipt{
			{ActionId: "one", Kind: "k", Target: "t", Summary: "s", Details: "d", StartedAt: early, CompletedAt: late},
			{ActionId: "two", Kind: "k", Target: "t", Summary: "s", Details: "d", StartedAt: early, CompletedAt: late},
		},
	}
	if err := validator.Validate(ok); err != nil {
		t.Fatalf("protovalidate rejected a valid IncidentResult: %v", err)
	}
}
