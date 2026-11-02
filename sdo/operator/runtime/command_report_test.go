package runtime

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestCommandReportSchemaMatchesGeneratedAgentFile(t *testing.T) {
	data, err := os.ReadFile("../../agents/responder/command_report_schema.json")
	if err != nil {
		t.Fatal(err)
	}
	var actual map[string]string
	if err := json.Unmarshal(data, &actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, CommandReportSchema()) {
		t.Fatal("regenerate using go run ./cmd/tool-report-schema")
	}
	encoded, err := json.Marshal(CommandReport{})
	if err != nil {
		t.Fatal(err)
	}
	var report map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &report); err != nil {
		t.Fatal(err)
	}
	if len(report) != len(actual) {
		t.Fatalf("schema=%v report=%v", actual, report)
	}
	for key := range report {
		if actual[key] == "" {
			t.Fatalf("undocumented key %s", key)
		}
	}
}
