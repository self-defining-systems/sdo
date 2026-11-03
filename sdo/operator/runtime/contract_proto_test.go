package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Stage 1 vertical slice of the proto single-source-of-truth track
// prove ObjectRef end-to-end through
// protojson before migrating the full message tree in Stage 2.
//
// The proto schema governs the schema, not the encoding: contracts stay
// snake_case protojson on the wire (UseProtoNames), so the generated encoding
// is byte-for-byte the legacy hand-written sdk.ObjectRef JSON. The Go side here
// writes tests/fixtures/sdo/contracts/proto/object_ref.json; the Python side
// (tests/unit/sdo/contracts/test_proto_contract_roundtrip.py) decodes it with
// the generated Python type, re-encodes, and asserts the same bytes — the
// Go-encode -> Python-decode -> re-encode -> identical round trip.

var protoContractFixtureDir = filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "contracts", "proto")

// protoContractJSON is the single wire convention for every SDO contract
// message: protojson with proto (snake_case) field names, zero-valued fields
// omitted. protojson deliberately randomizes interior whitespace, so we
// normalize through encoding/json — which sorts object keys — to a stable,
// indented, byte-comparable canonical form shared across Go and Python.
func protoContractJSON(t *testing.T, message proto.Message) []byte {
	t.Helper()
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		t.Fatalf("protojson marshal: %v", err)
	}
	return canonicalJSON(t, raw)
}

func canonicalJSON(t *testing.T, raw []byte) []byte {
	t.Helper()
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("normalize json: %v", err)
	}
	out, err := json.MarshalIndent(generic, "", "  ")
	if err != nil {
		t.Fatalf("encode canonical json: %v", err)
	}
	return append(out, '\n')
}

func checkProtoContractFixture(t *testing.T, name string, encoded []byte) {
	t.Helper()
	path := filepath.Join(protoContractFixtureDir, name)
	if *updateGoContractFixturesFlag {
		if err := os.MkdirAll(protoContractFixtureDir, 0o755); err != nil {
			t.Fatalf("create fixture dir: %v", err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return
	}
	golden, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s (regenerate with %s): %v", path, updateGoContractFixtures, err)
	}
	if !bytes.Equal(golden, encoded) {
		t.Fatalf("%s differs from the Go protojson encoding; regenerate with %s and rerun the Python contract tests.\n"+
			"got:\n%s", path, updateGoContractFixtures, encoded)
	}
}

func TestProtoObjectRefContract(t *testing.T) {
	ref := &contractsv1alpha1.ObjectRef{
		ApiVersion: "apps/v1",
		Kind:       "Deployment",
		Namespace:  "hotel-reservation",
		Name:       "frontend",
	}
	encoded := protoContractJSON(t, ref)
	checkProtoContractFixture(t, "object_ref.json", encoded)

	// Round trip within Go: decode the canonical bytes and re-encode; the proto
	// type must reproduce them exactly.
	var decoded contractsv1alpha1.ObjectRef
	if err := protojson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("protojson unmarshal: %v", err)
	}
	if reencoded := protoContractJSON(t, &decoded); !bytes.Equal(encoded, reencoded) {
		t.Fatalf("ObjectRef did not round-trip:\n got: %s\nwant: %s", reencoded, encoded)
	}

	// Wire compatibility: the proto encoding is byte-identical to the legacy
	// hand-written sdk.ObjectRef JSON, so Stage 2 can swap the source of truth
	// without changing a single byte on the wire.
	legacy := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "hotel-reservation", Name: "frontend"}
	legacyRaw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy ObjectRef: %v", err)
	}
	if legacyJSON := canonicalJSON(t, legacyRaw); !bytes.Equal(encoded, legacyJSON) {
		t.Fatalf("proto encoding diverges from legacy sdk.ObjectRef JSON:\nproto:  %s\nlegacy: %s", encoded, legacyJSON)
	}

	// Empty optional fields (api_version, namespace) are omitted, matching the
	// legacy struct's `omitempty`.
	minimal := protoContractJSON(t, &contractsv1alpha1.ObjectRef{Kind: "Service", Name: "api"})
	if bytes.Contains(minimal, []byte("api_version")) || bytes.Contains(minimal, []byte("namespace")) {
		t.Fatalf("empty optional fields must be omitted, got: %s", minimal)
	}
}

// A runtime closure and a plan application must map to their protobuf
// contracts: these two cases pin the Go side of the wire parity.
func TestProtoOutcomeDeriverAcceptsRuntimeClosure(t *testing.T) {
	now := lifecycleio.Now().UTC()
	finding := sdk.Finding{DetectorID: "health", RuleID: "health", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "fault", Evidence: "evidence",
		PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Name: "health"}, Fingerprint: "health"}
	closure := IncidentClosure{
		Request: IncidentRequest{
			SchemaVersion: ProtocolSchemaVersion, Application: "app", Namespace: "default", IncidentID: "inc-proto",
			Findings:        []sdk.Finding{finding},
			DetectorHistory: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now, Status: DetectorEvaluationFiring}}, SourceCommit: "base", DeployedCommit: "base",
			RepositoryWorktree: "/proposal", RepositoryBaseCommit: "base",
			ResponseDeadline: now.Add(time.Minute), CancellationToken: "token", RepairPolicy: "commit",
		},
		Result: &IncidentResult{
			SchemaVersion: ProtocolSchemaVersion, IncidentID: "inc-proto", Status: IncidentCompleted,
			Timing: TimingMetrics{StartedAt: now, CompletedAt: now.Add(time.Second)},
		},
		FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", EvaluatedAt: now.Add(time.Second), Status: DetectorEvaluationClear}},
		DetectedAt:          now, DispatchedAt: now, ResponderCompletedAt: now.Add(time.Second), VerifiedAt: now.Add(time.Second),
		Frozen: true,
	}
	derived, err := (ProtoOutcomeDeriver{}).Derive(context.Background(), closure, "", "test", "test")
	if err != nil {
		t.Fatal(err)
	}
	if derived.Record.GetIncidentId() != "inc-proto" {
		t.Fatalf("wrong outcome: %+v", derived.Record)
	}
}

func TestPlanApplicationResultMapsToProto(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	plan := program.Plan{Actions: []program.Action{program.SetField{Value: program.FromParam("fixture")}}}
	bound := program.BindPlanValues(plan, program.ValueInputs{Spec: program.ProgramSpec{Params: []program.ParamSpec{{Name: "fixture", ValueKind: program.IntValue, Default: 2}}}})
	value := bound.Actions[0].(program.SetField).Value
	action := program.SetField{Ref: ref, Path: "spec.replicas", Value: value, Reason: "restore capacity"}
	result := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{action}}, Applied: true, ActionsApplied: []program.Action{action}, VerificationPassed: true, VerificationCompleted: true}
	encoded, err := result.Proto()
	if err != nil || !encoded.GetApplied() || encoded.GetDryRun() || encoded.GetVerification().GetResult() != contractsv1alpha1.Verdict_RESULT_NO_MATCH || len(encoded.GetPlan().GetActions()) != 1 || encoded.GetPlan().GetActions()[0].GetSetField().GetValue().GetParameter() != "fixture" {
		t.Fatalf("proto application = %+v, err = %v", encoded, err)
	}
}
