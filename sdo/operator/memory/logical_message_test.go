package memory

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"sdo.dev/operator/memory/layout"
)

// Every string reaches its logical form: scalars, list items, map values and
// map keys, at any depth.
func TestLogicalizeMessageRewritesEveryString(t *testing.T) {
	message, err := structpb.NewStruct(map[string]any{
		"note":                   "wrote .sdo/deployment/patches/0001-fix.patch",
		".sdo/outcomes/abc.json": "read",
		"nested":                 map[string]any{"items": []any{"see .sdo/playbooks/raise-limit/README.md", 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	LogicalizeMessage(message)
	text := protojson.Format(message)
	if layout.ContainsPhysicalPath(text) {
		t.Fatalf("physical memory path survived: %s", text)
	}
	got := message.AsMap()
	if got["note"] != "wrote memory:deployment/patches/0001-fix.patch" || got["memory:outcomes/abc.json"] != "read" {
		t.Fatalf("unexpected rewrite: %s", text)
	}
	if items := got["nested"].(map[string]any)["items"].([]any); items[0] != "see playbook:raise-limit" || items[1] != float64(3) {
		t.Fatalf("unexpected nested rewrite: %v", items)
	}
}
