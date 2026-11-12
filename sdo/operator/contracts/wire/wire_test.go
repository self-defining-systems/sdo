package wire_test

import (
	"regexp"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func validResult() *contracts.AgentResult {
	return &contracts.AgentResult{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "t", Status: "completed"}
}

func TestMarshalValidatesBeforeEncoding(t *testing.T) {
	if _, err := wire.Marshal(validResult()); err != nil {
		t.Fatalf("valid result rejected: %v", err)
	}
	bad := validResult()
	bad.Status = "bogus"
	if _, err := wire.Marshal(bad); err == nil || !strings.Contains(err.Error(), "status") {
		t.Fatalf("invalid status encoded: %v", err)
	}
	if _, err := wire.Canonical(bad); err == nil {
		t.Fatal("canonical encoding skipped validation")
	}
}

func TestUnmarshalIsStrictAndValidated(t *testing.T) {
	if err := wire.Unmarshal([]byte(`{"schema_version":"sdo.dev/v1alpha1","task_id":"t","status":"failed"}`), &contracts.AgentResult{}); err != nil {
		t.Fatalf("valid document rejected: %v", err)
	}
	for name, document := range map[string]string{
		"unknown field": `{"schema_version":"sdo.dev/v1alpha1","task_id":"t","status":"failed","extra":1}`,
		"bad status":    `{"schema_version":"sdo.dev/v1alpha1","task_id":"t","status":"paused"}`,
		"bad version":   `{"schema_version":"v2","task_id":"t","status":"failed"}`,
	} {
		if err := wire.Unmarshal([]byte(document), &contracts.AgentResult{}); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := wire.UnmarshalLenient([]byte(`{"schema_version":"sdo.dev/v1alpha1","task_id":"t","status":"failed","extra":1}`), &contracts.AgentResult{}); err != nil {
		t.Fatalf("lenient decode rejected unknown field: %v", err)
	}
}

func TestIDPatternsRejectPhysicalPaths(t *testing.T) {
	pattern := regexp.MustCompile(wire.StringPattern(&contracts.Finding{}, "playbook_ids"))
	if pattern.MatchString(".sdo/playbooks/x/README.md") {
		t.Fatal("physical playbook path accepted as a playbook ID")
	}
	if !pattern.MatchString("restart-pod") {
		t.Fatal("logical playbook ID rejected")
	}
}

func TestRuleReaders(t *testing.T) {
	if got := wire.StringConst(&contracts.AgentTask{}, "schema_version"); got != "sdo.dev/v1alpha1" {
		t.Fatalf("schema version = %q", got)
	}
	if got := wire.StringIn(&contracts.AgentResult{}, "status"); len(got) != 3 {
		t.Fatalf("status values = %v", got)
	}
	if got := wire.StringPattern(&contracts.Finding{}, "playbook_ids"); got == "" {
		t.Fatal("empty item pattern")
	}
}
