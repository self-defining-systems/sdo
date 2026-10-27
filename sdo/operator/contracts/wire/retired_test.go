package wire

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const storedValuesSnapshot = "testdata/stored_values.json"

// Messages that are stored across builds: accepted-memory outcomes and
// durable controller state.
func storedRoots() []protoreflect.MessageDescriptor {
	return []protoreflect.MessageDescriptor{
		(&contracts.OutcomeRecord{}).ProtoReflect().Descriptor(),
		(&contracts.ControllerState{}).ProtoReflect().Descriptor(),
		(&contracts.BrokerLedger{}).ProtoReflect().Descriptor(),
	}
}

// currentStoredValues lists every value a stored string or enum field accepts
// today, keyed "<message>.<field>", across the messages reachable from the
// stored roots.
func currentStoredValues() map[string][]string {
	values := map[string][]string{}
	seen := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor)
	walk = func(message protoreflect.MessageDescriptor) {
		if seen[message.FullName()] || strings.HasPrefix(string(message.FullName()), "google.protobuf.") {
			return
		}
		seen[message.FullName()] = true
		for i := 0; i < message.Fields().Len(); i++ {
			field := message.Fields().Get(i)
			key := string(field.FullName())
			switch {
			case field.IsMap() && field.MapValue().Message() != nil:
				walk(field.MapValue().Message())
			case field.Message() != nil:
				walk(field.Message())
			case field.Kind() == protoreflect.EnumKind:
				for j := 0; j < field.Enum().Values().Len(); j++ {
					values[key] = append(values[key], string(field.Enum().Values().Get(j).Name()))
				}
			case field.Kind() == protoreflect.StringKind:
				rules, _ := proto.GetExtension(field.Options(), validate.E_Field).(*validate.FieldRules)
				allowed := rules.GetString().GetIn()
				if items := rules.GetRepeated().GetItems().GetString(); items != nil {
					allowed = items.GetIn()
				}
				if len(allowed) > 0 {
					values[key] = append(values[key], allowed...)
				}
			}
		}
	}
	for _, root := range storedRoots() {
		walk(root)
	}
	for key := range values {
		sort.Strings(values[key])
	}
	return values
}

func readSnapshot(t *testing.T) map[string][]string {
	t.Helper()
	data, err := os.ReadFile(storedValuesSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := map[string][]string{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// The snapshot holds every value a stored field has ever allowed, so removing
// or renaming one is a visible diff here. Set SDO_UPDATE_STORED_VALUES=1 to add
// newly allowed values to it; it never forgets a value.
func TestRetiredTablesCoverEverySchemaRemoval(t *testing.T) {
	current := currentStoredValues()
	snapshot := readSnapshot(t)
	if os.Getenv("SDO_UPDATE_STORED_VALUES") == "1" {
		for key, values := range current {
			for _, value := range values {
				if !contains(snapshot[key], value) {
					snapshot[key] = append(snapshot[key], value)
				}
			}
			sort.Strings(snapshot[key])
		}
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(storedValuesSnapshot, append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	for key, values := range snapshot {
		now, exists := current[key]
		if !exists {
			continue
		}
		for _, value := range values {
			if contains(now, value) {
				continue
			}
			successor, retired := retiredValues[key][value]
			if !retired || !contains(now, successor) {
				t.Errorf("%s no longer accepts %q, which earlier builds stored: add %q to retiredValues in retired.go mapping it to a value %s accepts", key, value, value, key)
			}
		}
	}
	for key, values := range current {
		for _, value := range values {
			if !contains(snapshot[key], value) {
				t.Errorf("%s accepts %q, which %s does not record: run SDO_UPDATE_STORED_VALUES=1 go test ./wire -run RetiredTables", key, value, storedValuesSnapshot)
			}
		}
	}
}

func TestRetiredTableEntriesAreLive(t *testing.T) {
	current := currentStoredValues()
	snapshot := readSnapshot(t)
	for key, retiredToSuccessor := range retiredValues {
		for value, successor := range retiredToSuccessor {
			if !contains(current[key], successor) {
				t.Errorf("%s retired %q maps to %q, which the schema does not accept", key, value, successor)
			}
			if contains(current[key], value) {
				t.Errorf("%s still accepts %q: drop its retiredValues entry", key, value)
			}
			if !contains(snapshot[key], value) {
				t.Errorf("%s retired %q is missing from %s", key, value, storedValuesSnapshot)
			}
		}
	}
	for oldKey, successor := range retiredFieldNames {
		cut := strings.LastIndex(oldKey, ".")
		message, oldName := oldKey[:cut], oldKey[cut+1:]
		descriptor := findMessage(t, message)
		if descriptor.Fields().ByName(protoreflect.Name(oldName)) != nil {
			t.Errorf("%s still has field %s: drop its retiredFieldNames entry", message, oldName)
		}
		if descriptor.Fields().ByName(protoreflect.Name(successor)) == nil {
			t.Errorf("%s has no field %s, the successor of %s", message, successor, oldName)
		}
	}
}

func findMessage(t *testing.T, name string) protoreflect.MessageDescriptor {
	t.Helper()
	var found protoreflect.MessageDescriptor
	seen := map[protoreflect.FullName]bool{}
	var walk func(protoreflect.MessageDescriptor)
	walk = func(message protoreflect.MessageDescriptor) {
		if found != nil || seen[message.FullName()] {
			return
		}
		seen[message.FullName()] = true
		if string(message.FullName()) == name {
			found = message
			return
		}
		for i := 0; i < message.Fields().Len(); i++ {
			field := message.Fields().Get(i)
			if field.IsMap() && field.MapValue().Message() != nil {
				walk(field.MapValue().Message())
			} else if field.Message() != nil {
				walk(field.Message())
			}
		}
	}
	for _, root := range storedRoots() {
		walk(root)
	}
	if found == nil {
		t.Fatalf("retired table names %s, which no stored message contains", name)
	}
	return found
}

// Every retired value is mapped wherever its field occurs in a document.
func TestEveryRetiredValueIsMapped(t *testing.T) {
	for key, retiredToSuccessor := range retiredValues {
		cut := strings.LastIndex(key, ".")
		message, field := key[:cut], key[cut+1:]
		descriptor := findMessage(t, message)
		for value, successor := range retiredToSuccessor {
			document, _ := json.Marshal(map[string]string{field: value})
			migrated, err := migrateRetired(document, descriptor)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]string
			if err := json.Unmarshal(migrated, &got); err != nil || got[field] != successor {
				t.Errorf("%s=%q migrated to %s (%v), want %q", key, value, migrated, err, successor)
			}
		}
	}
}

func TestRetiredFieldAndValueMigrationUnderDecode(t *testing.T) {
	document := []byte(`{"schema_version":1,"incident_id":"i","classification":"success","reflection_eligibility":{"version":1,"evidence_gap_count":2,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}`)
	if err := UnmarshalLenient(document, new(contracts.OutcomeRecord)); err == nil {
		t.Fatal("plain lenient decode accepted a retired value; the migration would be untested")
	}
	var record contracts.OutcomeRecord
	if err := UnmarshalLenientMigrating(document, &record); err != nil {
		t.Fatal(err)
	}
	eligibility := record.GetReflectionEligibility()
	if eligibility.GetUnassigned()[0].GetReason() != "no_recorded_repair" || eligibility.GetNoRepairCount() != 2 {
		t.Fatalf("not migrated: %v", &record)
	}
}

func TestDurableStateMigratesRetiredLedgerFieldName(t *testing.T) {
	var ledger contracts.BrokerLedger
	if err := UnmarshalDurable([]byte(`{"reflection_evidence_gap_count":3}`), &ledger); err != nil {
		t.Fatal(err)
	}
	if ledger.GetReflectionNoRepairCount() != 3 {
		t.Fatalf("count lost: %v", &ledger)
	}
}
