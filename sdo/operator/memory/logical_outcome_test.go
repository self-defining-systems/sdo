package memory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/layout"
)

const physicalProse = "see .sdo/playbooks/fix/README.md and .sdo/lib/kube/client.go"

// textField is one string field of OutcomeRecord reached through singular or
// repeated message fields: the path the sweep sets.
type textField []protoreflect.FieldDescriptor

func (f textField) String() string {
	names := make([]string, len(f))
	for i, field := range f {
		names[i] = string(field.Name())
	}
	return strings.Join(names, ".")
}

func textFields(message protoreflect.MessageDescriptor, prefix textField, seen map[protoreflect.FullName]bool) []textField {
	if seen[message.FullName()] || len(prefix) > 5 {
		return nil
	}
	seen[message.FullName()] = true
	defer delete(seen, message.FullName())
	var fields []textField
	for i := 0; i < message.Fields().Len(); i++ {
		field := message.Fields().Get(i)
		path := append(append(textField(nil), prefix...), field)
		switch {
		case field.IsMap():
		case field.Kind() == protoreflect.StringKind:
			fields = append(fields, path)
		case field.Kind() == protoreflect.MessageKind && !strings.HasPrefix(string(field.Message().FullName()), "google.protobuf."):
			fields = append(fields, textFields(field.Message(), path, seen)...)
		}
	}
	return fields
}

// setText sets the field at path to text. Every message the path creates is
// first filled with plain values, so required siblings do not hide the field.
func setText(message protoreflect.Message, path textField, text string) {
	field := path[0]
	if len(path) == 1 {
		if field.IsList() {
			list := message.Mutable(field).List()
			list.Truncate(0)
			list.Append(protoreflect.ValueOfString(text))
		} else {
			message.Set(field, protoreflect.ValueOfString(text))
		}
		return
	}
	if field.IsList() {
		list := message.Mutable(field).List()
		if list.Len() == 0 {
			element := list.NewElement()
			fill(element.Message(), 0)
			list.Append(element)
		}
		setText(list.Get(0).Message(), path[1:], text)
		return
	}
	if !message.Has(field) {
		fill(message.Mutable(field).Message(), 0)
	}
	setText(message.Mutable(field).Message(), path[1:], text)
}

// fill sets the fields a message's validation rules require, with the
// smallest values those rules accept.
func fill(message protoreflect.Message, depth int) {
	fields := message.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
		value := func() (protoreflect.Value, bool) {
			switch field.Kind() {
			case protoreflect.StringKind:
				if in := rules.GetString().GetIn(); len(in) > 0 {
					return protoreflect.ValueOfString(in[0]), true
				}
				return protoreflect.ValueOfString("plain"), rules.GetString().GetMinLen() > 0 || field.IsList()
			case protoreflect.Uint32Kind:
				return protoreflect.ValueOfUint32(1), rules.GetUint32().GetGte() > 0 || rules.GetUint32().HasGt()
			case protoreflect.Int32Kind:
				return protoreflect.ValueOfInt32(1), rules.GetInt32().GetGte() > 0 || rules.GetInt32().HasGt()
			case protoreflect.MessageKind:
				if depth >= 4 {
					return protoreflect.Value{}, false
				}
				element := message.NewField(field)
				if field.IsList() {
					element = message.Mutable(field).List().NewElement()
				}
				if !strings.HasPrefix(string(field.Message().FullName()), "google.protobuf.") {
					fill(element.Message(), depth+1)
				}
				return element, rules.GetRequired() || field.IsList()
			}
			return protoreflect.Value{}, false
		}
		switch {
		case field.IsMap() || field.ContainingOneof() != nil:
		case field.IsList():
			for range rules.GetRepeated().GetMinItems() {
				if item, ok := value(); ok {
					message.Mutable(field).List().Append(item)
				}
			}
		default:
			if item, ok := value(); ok {
				message.Set(field, item)
			}
		}
	}
}

func baseOutcome(id string) *contracts.OutcomeRecord {
	return &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}
}

// Class guard: every text field an outcome can hold, now or after a schema
// change, is stored and read without a physical memory path, whether it was
// written through the store or accepted before the store rewrote prose.
func TestEveryOutcomeTextFieldIsStoredAndReadLogically(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewJSONStore(root)
	covered := map[string]bool{}
	for _, path := range textFields(baseOutcome("").ProtoReflect().Descriptor(), nil, map[protoreflect.FullName]bool{}) {
		if path.String() == "incident_id" {
			continue // the store's key, set by every case
		}
		plain := baseOutcome("plain")
		setText(plain.ProtoReflect(), path, "plain text")
		if wire.Validate(plain) != nil {
			continue // a constrained field (enum-like value, identifier) cannot hold prose
		}
		written := baseOutcome(path.String())
		setText(written.ProtoReflect(), path, physicalProse)
		if _, err := store.Put(ctx, written); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		older := baseOutcome("older-" + path.String())
		setText(older.ProtoReflect(), path, physicalProse)
		document, err := wire.MarshalFull(older)
		if err != nil {
			t.Fatal(err)
		}
		olderFile := filepath.Join(root, layout.OutcomePath(older.IncidentId))
		if err := os.WriteFile(olderFile, document, 0o644); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{written.IncidentId, older.IncidentId} {
			outcome, err := store.Get(ctx, id)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			shown, err := wire.Marshal(outcome.Record)
			if err != nil {
				t.Fatal(err)
			}
			if layout.ContainsPhysicalPath(string(shown)) || !strings.Contains(string(shown), "playbook:fix") {
				t.Fatalf("%s read as %s", path, shown)
			}
		}
		stored, err := os.ReadFile(filepath.Join(root, layout.OutcomePath(written.IncidentId)))
		if err != nil {
			t.Fatal(err)
		}
		if layout.ContainsPhysicalPath(string(stored)) {
			t.Fatalf("%s stored as %s", path, stored)
		}
		covered[path.String()] = true
	}
	for _, path := range []string{"sdk_gaps.scratch_script", "sdk_gaps.reason", "reflection_rejections.diagnostics.path", "reflection_rejections.diagnostics.message", "source_check_notes", "confirmed_root_causes.summary"} {
		if !covered[path] {
			t.Errorf("sweep did not cover %s (covered %d fields)", path, len(covered))
		}
	}
	// The sweep reached 432 fields when written; a much smaller number means
	// the construction stopped reaching nested prose, not that the schema shrank.
	if len(covered) < 300 {
		t.Errorf("sweep covered only %d text fields", len(covered))
	}
	listed, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2*len(covered) {
		t.Fatalf("listed %d outcomes, want %d", len(listed), 2*len(covered))
	}
}

// Reflection annotations are agent prose appended after the outcome is
// stored. A retried annotation is the same logical gap and is kept once.
func TestReflectionAnnotationsAreStoredLogicallyAndOnce(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := NewJSONStore(root)
	if _, err := store.Put(ctx, baseOutcome("i")); err != nil {
		t.Fatal(err)
	}
	gap := &contracts.SdkGap{FaultClass: "drift", Primitive: "references", Reason: "saved", ScratchScript: physicalProse}
	rejection := &contracts.ProposalRejection{Attempt: 1, Diagnostics: []*contracts.MemoryDiagnostic{{Rule: "text.physical-path", Path: ".sdo/playbooks/fix/README.md", Message: "bad"}}}
	for range 2 {
		if _, err := store.AppendSDKGaps(ctx, "i", []*contracts.SdkGap{proto.Clone(gap).(*contracts.SdkGap)}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.AppendReflectionRejections(ctx, "i", []*contracts.ProposalRejection{proto.Clone(rejection).(*contracts.ProposalRejection)}); err != nil {
			t.Fatal(err)
		}
	}
	outcome, err := store.Get(ctx, "i")
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Record.SdkGaps) != 1 || len(outcome.Record.ReflectionRejections) != 1 || outcome.Record.SdkGaps[0].ScratchScript != "see playbook:fix and memory:lib/kube/client.go" || outcome.Record.ReflectionRejections[0].Diagnostics[0].Path != "playbook:fix" {
		t.Fatalf("annotations = %v / %v", outcome.Record.SdkGaps, outcome.Record.ReflectionRejections)
	}
	stored, err := os.ReadFile(filepath.Join(root, layout.OutcomePath("i")))
	if err != nil {
		t.Fatal(err)
	}
	if layout.ContainsPhysicalPath(string(stored)) {
		t.Fatalf("annotations stored as %s", stored)
	}
}
