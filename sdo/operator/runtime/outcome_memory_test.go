package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
)

func recordOutcomes(t *testing.T, repository, lines string) {
	t.Helper()
	store := memory.NewJSONStore(repository)
	for _, line := range strings.Split(lines, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var document map[string]any
		if err := json.Unmarshal([]byte(line), &document); err != nil {
			t.Fatal(err)
		}
		document["schema_version"] = 1
		data, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		record := new(contracts.OutcomeRecord)
		if err := protojson.Unmarshal(data, record); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRelevantOutcomeEvidenceSelectsCompactVerifiedMatch(t *testing.T) {
	repository := t.TempDir()
	outcome := `{"incident_id":"old","source_commit":"abc","classification":"success","findings":[{"detector_id":"health","rule_id":"missing","status":"active","severity":"critical","summary":"missing","evidence":"missing","primary_resource":{"kind":"ConfigMap","name":"cfg"},"fingerprint":"missing:cfg"}],"confirmed_root_causes":[{"summary":"ConfigMap absent","resources":[{"kind":"ConfigMap","name":"cfg"}]}],"applied_playbooks":[".sdo/playbooks/missing.md"],"repair_actions":[{"action_id":"apply","kind":"ConfigMap","target":"ns/cfg","summary":"restored manifest","details":"applied source","started_at":"2026-01-01T00:00:00Z","completed_at":"2026-01-01T00:00:01Z","success":true,"reversible":true}]}` + "\n"
	recordOutcomes(t, repository, outcome)
	findings := []sdk.Finding{{DetectorID: "health", RuleID: "missing", Fingerprint: "missing:cfg", PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Name: "cfg"}}}

	got := relevantOutcomeEvidence(repository, findings, "abc", nil)

	if len(got) != 1 || got[0].MatchReason != "exact-fingerprint" || !got[0].ExactSourceMatch {
		t.Fatalf("unexpected relevant outcome: %#v", got)
	}
	if len(got[0].RepairActionSummaries) != 1 || got[0].RepairActionSummaries[0] != "restored manifest" {
		t.Fatalf("expected compact successful repair evidence, got %#v", got[0])
	}
}

func TestRelevantOutcomeEvidenceIgnoresFailuresAndUnrelatedFindings(t *testing.T) {
	repository := t.TempDir()
	lines := "{\"incident_id\":\"failed\",\"classification\":\"failed\"}\n" +
		"{\"incident_id\":\"other\",\"source_commit\":\"abc\",\"classification\":\"success\",\"findings\":[{\"detector_id\":\"other\",\"rule_id\":\"other\",\"primary_resource\":{\"kind\":\"Pod\"}}]}\n"
	recordOutcomes(t, repository, lines)

	got := relevantOutcomeEvidence(repository, []sdk.Finding{{DetectorID: "health", RuleID: "missing", PrimaryResource: sdk.ObjectRef{Kind: "ConfigMap", Name: "cfg"}}}, "abc", nil)
	if len(got) != 0 {
		t.Fatalf("expected no relevant outcomes, got %#v", got)
	}
}

// A detector learned from an incident fires on its first match, often before
// the health detectors that opened the original incident. Its repeat then
// shares no fingerprint with the prior outcome, and the warm path was lost
// on timing. The learned detector's origin, on the same resource, links them.
func TestALearnedDetectorLinksItsRepeatToTheIncidentItWasLearnedFrom(t *testing.T) {
	repository := t.TempDir()
	outcome := `{"incident_id":"first","source_commit":"abc","classification":"success","findings":[{"detector_id":"health-objective","rule_id":"network-policy-total-isolation","primary_resource":{"kind":"NetworkPolicy","namespace":"app","name":"deny-all"},"fingerprint":"health-objective/deny-all"}],"applied_playbooks":[]}` + "\n"
	recordOutcomes(t, repository, outcome)
	origins := map[string]string{"deny-all-isolation": "first"}
	repeat := []sdk.Finding{{
		DetectorID: "deny-all-isolation", RuleID: "total-isolation", Fingerprint: "deny-all-isolation/deny-all",
		PrimaryResource: sdk.ObjectRef{Kind: "NetworkPolicy", Namespace: "app", Name: "deny-all"},
	}}

	got := relevantOutcomeEvidence(repository, repeat, "abc", origins)
	if len(got) != 1 || got[0].IncidentID != "first" || got[0].MatchReason != "learned-detector-origin" {
		t.Fatalf("the learned detector's repeat must match its originating outcome: %#v", got)
	}

	variant := []sdk.Finding{{
		DetectorID: "deny-all-isolation", RuleID: "total-isolation", Fingerprint: "deny-all-isolation/other",
		PrimaryResource: sdk.ObjectRef{Kind: "NetworkPolicy", Namespace: "app", Name: "other"},
	}}
	if got := relevantOutcomeEvidence(repository, variant, "abc", origins); len(got) != 0 {
		t.Fatalf("the learned detector on another resource is not a repeat: %#v", got)
	}
}
