package wire

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	storedValuesSnapshot = "testdata/stored_values.json"
	storedFieldsSnapshot = "testdata/stored_fields.json"
)

// Messages that are stored across builds: accepted-memory outcomes, durable
// controller state and the frozen or journaled messages read back on restart.
func storedRoots() []protoreflect.MessageDescriptor {
	return []protoreflect.MessageDescriptor{
		(&contracts.OutcomeRecord{}).ProtoReflect().Descriptor(),
		(&contracts.ControllerState{}).ProtoReflect().Descriptor(),
		(&contracts.BrokerLedger{}).ProtoReflect().Descriptor(),
		(&contracts.AgentTask{}).ProtoReflect().Descriptor(),
		(&contracts.ProposalRejectionLog{}).ProtoReflect().Descriptor(),
		(&contracts.RepairActionReceipt{}).ProtoReflect().Descriptor(),
	}
}

// currentStored lists every field and every value a stored string or enum
// field accepts today across the messages reachable from the stored roots.
// Values are keyed "<message>.<field>"; fields are those same names, sorted.
func currentStored() (values map[string][]string, fieldNames []string) {
	values = map[string][]string{}
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
			fieldNames = append(fieldNames, key)
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
	sort.Strings(fieldNames)
	return values, fieldNames
}

func currentStoredValues() map[string][]string {
	values, _ := currentStored()
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

func readFieldSnapshot(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(storedFieldsSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	var fields []string
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func contains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// The snapshots hold every field and every value a stored message has ever
// had, so removing or renaming one is a visible failure here until the retired
// tables map it. Set SDO_UPDATE_STORED_VALUES=1 to add newly present fields and
// values to the snapshots; it never forgets one and never excuses a removal.
func TestRetiredTablesCoverEverySchemaRemoval(t *testing.T) {
	current, currentFields := currentStored()
	snapshot := readSnapshot(t)
	snapshotFields := readFieldSnapshot(t)

	for _, problem := range unmappedRemovals(snapshotFields, currentFields, snapshot, current) {
		t.Error(problem)
	}
	if t.Failed() {
		return
	}

	if os.Getenv("SDO_UPDATE_STORED_VALUES") == "1" {
		for key, values := range current {
			for _, value := range values {
				if !contains(snapshot[key], value) {
					snapshot[key] = append(snapshot[key], value)
				}
			}
			sort.Strings(snapshot[key])
		}
		for _, field := range currentFields {
			if !contains(snapshotFields, field) {
				snapshotFields = append(snapshotFields, field)
			}
		}
		sort.Strings(snapshotFields)
		writeJSONFile(t, storedValuesSnapshot, snapshot)
		writeJSONFile(t, storedFieldsSnapshot, snapshotFields)
		return
	}
	for key, values := range current {
		for _, value := range values {
			if !contains(snapshot[key], value) {
				t.Errorf("%s accepts %q, which %s does not record: run SDO_UPDATE_STORED_VALUES=1 go test ./contracts/wire -run RetiredTables", key, value, storedValuesSnapshot)
			}
		}
	}
	for _, field := range currentFields {
		if !contains(snapshotFields, field) {
			t.Errorf("field %s is not in %s: run SDO_UPDATE_STORED_VALUES=1 go test ./contracts/wire -run RetiredTables", field, storedFieldsSnapshot)
		}
	}
}

// unmappedRemovals lists the fields and values the snapshots record that the
// schema no longer has and the retired tables do not map. A field that
// disappeared, with or without its allowed-value list, is never skipped.
func unmappedRemovals(snapshotFields, currentFields []string, snapshot, current map[string][]string) []string {
	var problems []string
	for _, field := range snapshotFields {
		if contains(currentFields, field) {
			continue
		}
		if _, retired := retiredFieldNames[field]; !retired {
			problems = append(problems, fmt.Sprintf("field %s no longer exists, but earlier builds stored it: add %q to retiredFieldNames in retired.go mapping it to its successor field (or \"\" when its data is dropped)", field, field))
		}
	}
	for key, values := range snapshot {
		if _, exists := current[key]; !exists {
			// A field that stays but stopped listing values accepts anything,
			// a superset; a field that is gone is covered by the loop above.
			continue
		}
		for _, value := range values {
			if contains(current[key], value) {
				continue
			}
			successor, retired := retiredValues[key][value]
			if !retired || !contains(current[key], successor) {
				problems = append(problems, fmt.Sprintf("%s no longer accepts %q, which earlier builds stored: add %q to retiredValues in retired.go mapping it to a value %s accepts", key, value, value, key))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func TestUnmappedRemovalsCatchesARenamedValueBearingField(t *testing.T) {
	const old = "sdodev.contracts.v1alpha1.Synthetic.kind"
	snapshot := map[string][]string{old: {"a"}}
	fields := []string{old}
	if problems := unmappedRemovals(fields, []string{"sdodev.contracts.v1alpha1.Synthetic.type"}, snapshot, map[string][]string{"sdodev.contracts.v1alpha1.Synthetic.type": {"a"}}); len(problems) != 1 || !strings.Contains(problems[0], old) {
		t.Fatalf("renamed field with allowed values not reported: %v", problems)
	}
	if problems := unmappedRemovals(fields, []string{"sdodev.contracts.v1alpha1.Synthetic.type"}, nil, nil); len(problems) != 1 {
		t.Fatalf("renamed field without allowed values not reported: %v", problems)
	}
	retiredFieldNames[old] = "type"
	defer delete(retiredFieldNames, old)
	if problems := unmappedRemovals(fields, []string{"sdodev.contracts.v1alpha1.Synthetic.type"}, snapshot, map[string][]string{"sdodev.contracts.v1alpha1.Synthetic.type": {"a"}}); len(problems) != 0 {
		t.Fatalf("mapped rename still reported: %v", problems)
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
		if !contains(readFieldSnapshot(t), oldKey) {
			t.Errorf("retired field %s is missing from %s", oldKey, storedFieldsSnapshot)
		}
		if successor != "" && descriptor.Fields().ByName(protoreflect.Name(successor)) == nil {
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

func TestRetiredFieldAndValueMigrationInsideAny(t *testing.T) {
	inner := []byte(`{"outcome":{"schema_version":1,"incident_id":"i","classification":"success","reflection_eligibility":{"version":1,"evidence_gap_count":2,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}}`)
	document := []byte(`{"schema_version":"sdo.dev/v1alpha1","task_id":"t","agent_id":"a","task_type":"reflection","request":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.ReflectionRequest",` + string(inner[1:]) + `}`)
	var task contracts.AgentTask
	if err := Unmarshal(document, &task); err == nil {
		t.Fatal("plain decode accepted a retired field inside an Any; the migration would be untested")
	}
	if err := UnmarshalDurable(document, &task); err != nil {
		t.Fatal(err)
	}
	var request contracts.ReflectionRequest
	if err := task.GetRequest().UnmarshalTo(&request); err != nil {
		t.Fatal(err)
	}
	eligibility := request.GetOutcome().GetReflectionEligibility()
	if eligibility.GetNoRepairCount() != 2 || eligibility.GetUnassigned()[0].GetReason() != "no_recorded_repair" {
		t.Fatalf("not migrated inside Any: %v", &request)
	}
}

func TestRetiredFieldWithEmptySuccessorIsDropped(t *testing.T) {
	const old = "sdodev.contracts.v1alpha1.BrokerLedger.synthetic_removed"
	retiredFieldNames[old] = ""
	defer delete(retiredFieldNames, old)
	var ledger contracts.BrokerLedger
	if err := UnmarshalDurable([]byte(`{"synthetic_removed":3,"reflection_no_repair_count":1}`), &ledger); err != nil {
		t.Fatal(err)
	}
	if ledger.GetReflectionNoRepairCount() != 1 {
		t.Fatalf("other fields lost: %v", &ledger)
	}
}
