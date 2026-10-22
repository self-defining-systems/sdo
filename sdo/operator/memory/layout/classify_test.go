package layout

import (
	"strings"
	"testing"
)

func TestClassify(t *testing.T) {
	outcome := OutcomePath("incident-1")
	cases := []struct {
		path string
		kind Kind
		id   string
		ok   bool
	}{
		{".sdo/schema-version", KindSchemaVersion, "", true},
		{".sdo/goal.md", KindGoal, "", true},
		{".sdo/arch.md", KindArchitecture, "", true},
		{".sdo/diagnostics/go.mod", KindDiagnosticsModule, "go.mod", true},
		{".sdo/diagnostics/go.sum", KindDiagnosticsModule, "go.sum", true},
		{".sdo/diagnostics/manifest.yaml", KindLegacyDetectorManifest, "", true},
		{".sdo/diagnostics/detectors/health/objective/manifest.yaml", KindHealthDetectorManifest, "objective", true},
		{".sdo/diagnostics/detectors/incidents/cache.v2/manifest.yaml", KindIncidentDetectorManifest, "cache.v2", true},
		{".sdo/diagnostics/detectors/health/objective/detector.go", KindHealthDetectorSource, "objective", true},
		{".sdo/diagnostics/detectors/incidents/cache/testdata/a.json", KindIncidentDetectorSource, "cache", true},
		{".sdo/diagnostics/traffic/generators/main.go", KindTrafficGenerator, "application", true},
		{".sdo/diagnostics/traffic/generators/incident/main.go", KindTrafficGenerator, "incident", true},
		{".sdo/diagnostics/traffic/workloads/incident-x.yaml", KindTrafficWorkload, "incident-x", true},
		{".sdo/playbooks/README.md", KindPlaybookIndex, "", true},
		{".sdo/playbooks/cache-pressure/README.md", KindPlaybook, "cache-pressure", true},
		{".sdo/playbooks/cache-pressure/scripts/fix.sh", KindPlaybookScript, "cache-pressure/fix.sh", true},
		{".sdo/lifecycle-provenance.yaml", KindLifecycleProvenance, "", true},
		{".sdo/outcomes.jsonl", KindLegacyOutcomes, "", true},
		{outcome, KindOutcome, strings.TrimSuffix(strings.TrimPrefix(outcome, ".sdo/outcomes/"), ".json"), true},
		{"README.md", "", "", false},
		{".sdo", "", "", false},
		{".sdo/unknown", "", "", false},
		{".sdo/diagnostics/detectors/health/Bad_ID/manifest.yaml", "", "", false},
		{".sdo/diagnostics/detectors/other/x/manifest.yaml", "", "", false},
		{".sdo/diagnostics/detectors/health/x", "", "", false},
		{".sdo/playbooks/Bad/README.md", "", "", false},
		{".sdo/playbooks/p/notes.md", "", "", false},
		{".sdo/playbooks/p/scripts/nested/x.sh", "", "", false},
		{".sdo/playbooks/p/scripts/x.py", "", "", false},
		{".sdo/outcomes/not-a-hash.json", "", "", false},
		{".sdo/../x", "", "", false},
		{".sdo//goal.md", "", "", false},
		{".sdo-runtime/x", "", "", false},
	}
	for _, c := range cases {
		kind, id, ok := Classify(c.path)
		if kind != c.kind || id != c.id || ok != c.ok {
			t.Errorf("Classify(%q) = (%q, %q, %v), want (%q, %q, %v)", c.path, kind, id, ok, c.kind, c.id, c.ok)
		}
	}
}

func TestPathBuildersRoundTripThroughClassify(t *testing.T) {
	for _, class := range []Class{ClassHealth, ClassIncident} {
		kind, id, ok := Classify(DetectorManifestPath(class, "d1"))
		if !ok || id != "d1" || kind != class.ManifestKind() {
			t.Errorf("manifest path for %s classified as (%q, %q, %v)", class, kind, id, ok)
		}
		kind, id, ok = Classify(DetectorDir(class, "d1") + "/detector.go")
		if !ok || id != "d1" || kind != class.SourceKind() {
			t.Errorf("source path for %s classified as (%q, %q, %v)", class, kind, id, ok)
		}
	}
	path, err := PlaybookPath("a-b")
	if err != nil {
		t.Fatal(err)
	}
	if kind, id, ok := Classify(path); !ok || kind != KindPlaybook || id != "a-b" {
		t.Errorf("playbook path classified as (%q, %q, %v)", kind, id, ok)
	}
	if _, err := PlaybookPath("../x"); err == nil {
		t.Error("unsafe playbook ID was accepted")
	}
}

func TestPlaybookReferences(t *testing.T) {
	for reference, want := range map[string]string{
		"cache-pressure": "cache-pressure",
		".sdo/playbooks/cache-pressure/README.md": "cache-pressure",
		".sdo/playbooks/cache-pressure.md":        "cache-pressure",
	} {
		if got, err := PlaybookID(reference); err != nil || got != want {
			t.Errorf("PlaybookID(%q) = %q, %v", reference, got, err)
		}
	}
	for _, bad := range []string{"../../outside", ".sdo/playbooks/A/README.md", ".sdo/playbooks/a/b/README.md", ""} {
		if _, err := PlaybookID(bad); err == nil {
			t.Errorf("PlaybookID(%q) succeeded", bad)
		}
	}
	if name, err := PlaybookScriptName("p", ".sdo/playbooks/p/scripts/fix.sh"); err != nil || name != "fix.sh" {
		t.Errorf("script name = %q, %v", name, err)
	}
	if _, err := PlaybookScriptName("p", ".sdo/playbooks/q/scripts/fix.sh"); err == nil {
		t.Error("script from another playbook was accepted")
	}
}

func TestArtifactIDAndRollout(t *testing.T) {
	if id, err := ArtifactID(DetectorDir(ClassIncident, "d1") + "/detector.go"); err != nil || id != "d1" {
		t.Errorf("ArtifactID = %q, %v", id, err)
	}
	if _, err := ArtifactID(".sdo/goal.md"); err == nil {
		t.Error("goal is not a proposable artifact")
	}
	if !RequiresControllerRollout([]string{DetectorManifestPath(ClassIncident, "d1")}) {
		t.Error("incident detector manifest edit requires rollout")
	}
	if RequiresControllerRollout([]string{DetectorManifestPath(ClassHealth, "d1"), ".sdo/playbooks/p/README.md"}) {
		t.Error("health detector and playbook edits do not require rollout")
	}
}

func TestRequiresValidatorJob(t *testing.T) {
	outcome := OutcomePath("incident-1")
	cases := []struct {
		name  string
		paths []string
		want  bool
	}{
		{"prose and outcome only", []string{".sdo/goal.md", ".sdo/arch.md", ".sdo/playbooks/README.md", ".sdo/playbooks/cache/README.md", ".sdo/playbooks/cache/scripts/restart.sh", outcome}, false},
		{"program source", []string{".sdo/playbooks/cache/diagnose.go"}, true},
		{"library", []string{".sdo/lib/shared/helper.go"}, true},
		{"incident detector", []string{".sdo/diagnostics/detectors/incidents/cache/detector.go"}, true},
		{"health detector", []string{".sdo/diagnostics/detectors/health/objective/manifest.yaml"}, true},
		{"traffic generator", []string{".sdo/diagnostics/traffic/generators/main.go"}, true},
		{"diagnostics module", []string{".sdo/diagnostics/go.mod"}, true},
		{"new corpus case is replayed by the Job", []string{".sdo/corpus/cache/open.json"}, true},
		{"application source", []string{"app/main.go"}, true},
		{"prose plus one executable path", []string{".sdo/arch.md", ".sdo/playbooks/cache/mitigate.go"}, true},
		{"no change", nil, false},
	}
	for _, tc := range cases {
		if got := RequiresValidatorJob(tc.paths); got != tc.want {
			t.Errorf("%s: RequiresValidatorJob(%v) = %v, want %v", tc.name, tc.paths, got, tc.want)
		}
	}
}
