package runtime

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
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
	if os.Getenv(updateGoContractFixtures) == "1" {
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
		t.Fatalf("read %s (regenerate with %s=1): %v", path, updateGoContractFixtures, err)
	}
	if !bytes.Equal(golden, encoded) {
		t.Fatalf("%s differs from the Go protojson encoding; regenerate with %s=1 and rerun the Python contract tests.\n"+
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
