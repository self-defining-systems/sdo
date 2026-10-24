package validate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDuplicateIDDoesNotChangeOwnershipOfAcceptedDetector(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	applyCase(t, root, "duplicate-id")
	want := []Diagnostic{{
		Rule:    RuleDuplicateID,
		Path:    ".sdo/diagnostics/detectors/incidents/health/manifest.yaml",
		Message: `detector ID "health" is already used by .sdo/diagnostics/detectors/health/health/manifest.yaml`,
	}}
	got := diagnose(t, Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Layers: Static})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("diagnostics = %v, want %v", got, want)
	}
}

func TestOwnershipChecksEveryRegistrationWithDuplicateID(t *testing.T) {
	for _, test := range []struct {
		name  string
		actor Actor
		phase Phase
		edit  func(*testing.T, string)
		want  []Diagnostic
	}{
		{
			name:  "responder adds protected health detector and rewrites incident provenance",
			actor: Responder, phase: ReflectionPhase,
			edit: func(t *testing.T, root string) {
				manifest, err := os.ReadFile(filepath.Join(root, healthManifest))
				if err != nil {
					t.Fatal(err)
				}
				write(t, root, ".sdo/diagnostics/detectors/health/cache/manifest.yaml", strings.Replace(string(manifest), "id: health", "id: cache", 1))
				edit(t, root, cacheManifest, "originatingIncident: inc-1", "originatingIncident: inc-9")
			},
			want: []Diagnostic{
				{Rule: RuleOwnershipDenied, Path: ".sdo/diagnostics/detectors/health/cache/manifest.yaml", Message: "actor responder may not change health-detector-manifest files in the reflection phase", Hint: "changeable by health_judge in the initial phase or upkeep in the initial phase"},
				{Rule: RuleProtectedDetector, Path: ".sdo/diagnostics/detectors/health/cache/manifest.yaml", Message: "actor responder added protected health detector cache"},
				{Rule: RuleProvenanceRewrite, Path: cacheManifest, Message: "responder rewrote detector cache provenance"},
			},
		},
		{
			name:  "judge adds protected incident detector and refines health registration",
			actor: HealthJudge, phase: InitialPhase,
			edit: func(t *testing.T, root string) {
				write(t, root, ".sdo/diagnostics/detectors/incidents/health/manifest.yaml", incidentManifest("health", "1", "inc-2"))
				edit(t, root, healthManifest, "interval: 30s", "interval: 20s")
			},
			want: []Diagnostic{
				{Rule: RuleOwnershipDenied, Path: ".sdo/diagnostics/detectors/incidents/health/manifest.yaml", Message: "actor health_judge may not change incident-detector-manifest files in the initial phase", Hint: "changeable by human in the import phase or responder in the reflection phase or upkeep in the initial phase"},
				{Rule: RuleProtectedDetector, Path: ".sdo/diagnostics/detectors/incidents/health/manifest.yaml", Message: "actor health_judge added protected incident detector health"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := goodTree(t)
			commit(t, root, "base")
			test.edit(t, root)
			got := diagnose(t, Request{Root: root, Base: "HEAD", Actor: test.actor, Phase: test.phase, Layers: Ownership})
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("diagnostics = %v, want %v", got, test.want)
			}
		})
	}
}
