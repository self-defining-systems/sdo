package validate

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/capturefixture"
	"sdo.dev/operator/memory/layout"
	programvalidation "sdo.dev/operator/memory/validate/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestPartialPlaybookPublication(t *testing.T) {
	for _, mode := range []string{"mechanical-step", "readme-only", "broken-peer"} {
		t.Run(mode, func(t *testing.T) {
			root := goodTree(t)
			fixture := "../../../../tests/fixtures/sdo/playbook_program_app/.sdo"
			copyTree(t, filepath.Join(fixture, "diagnostics"), filepath.Join(root, ".sdo/diagnostics"))
			copyTree(t, filepath.Join(fixture, "lib"), filepath.Join(root, ".sdo/lib"))
			readme, err := os.ReadFile(filepath.Join(fixture, "playbooks/service-targetport-mismatch/README.md"))
			if err != nil {
				t.Fatal(err)
			}
			write(t, root, ".sdo/playbooks/service-targetport-mismatch/README.md", string(readme))
			write(t, root, ".sdo/playbooks/missing-configmap/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\noriginating_incident: initial\n---\nInspect the missing reference and restore the intended ConfigMap.\n")
			outcome := closureOutcome("success", &contracts.ConfirmedRootCause{Resources: []*contracts.ObjectRef{{Kind: "Service", Namespace: "demo", Name: "front"}}})
			outcome.SchemaVersion, outcome.IncidentId = 1, "partial-learning"
			outcome.ConfirmedRootCauses[0].Summary = "synthetic wrong target port"
			for _, action := range outcome.RepairActions {
				action.Target = "Service demo/front"
				action.Summary = "restore target port"
				action.Details = "synthetic repair receipt"
			}
			raw, err := protojson.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			write(t, root, layout.OutcomePath(outcome.IncidentId), string(raw)+"\n")
			commit(t, root, "accepted outcome\n\nSDO-Incident: partial-learning\nSDO-Phase: outcome")
			base := gitIn(t, root, "rev-parse", "HEAD")
			ids := []string{"partial-orchestration"}
			write(t, root, ".sdo/playbooks/partial-orchestration/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: partial-orchestration\noriginating_incident: partial-learning\n---\nAfter the mechanical Service port repair, inspect the admission backend's endpoints and policy intent. Restore its backend when available; choose a scoped bypass only when that backend is gone and availability requires it. Recheck application health.\n")
			if mode != "readme-only" {
				for _, part := range []string{"playbooks", "lib", "corpus"} {
					copyTree(t, filepath.Join(fixture, part), filepath.Join(root, ".sdo", part))
				}
				// Only the bound Service is repaired. An unrelated ConfigMap difference
				// remains in both open and fixed; it must not invalidate this step.
				dir := filepath.Join(root, ".sdo/corpus/targetport-mismatch")
				for _, phase := range []string{"open", "fixed"} {
					data, err := os.ReadFile(filepath.Join(dir, phase+".json"))
					if err != nil {
						t.Fatal(err)
					}
					var snapshot sdktest.Snapshot
					if err := json.Unmarshal(data, &snapshot); err != nil {
						t.Fatal(err)
					}
					// Recapture through the production boundary rather than hand-writing captures.
					snapshot.ConfigMapList = append(snapshot.ConfigMapList, corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "peer-judgement", Namespace: "demo"}, Data: map[string]string{"mode": "still-faulted"}})
					capturefixture.WriteBoundary(t, filepath.Join(dir, phase+".json"), snapshot, "demo")
				}
				// The fixture's guidance-only playbook predates front matter.
				write(t, root, ".sdo/playbooks/missing-configmap/README.md", "---\nschema_version: 1\nowner: responder\nfault_class: missing-configmap\noriginating_incident: partial-learning\n---\nInspect the missing reference and restore the intended ConfigMap.\n")
				ids = append(ids, "service-targetport-mismatch", "missing-configmap")
				if mode == "broken-peer" {
					// A partial playbook grants no exemption to another malformed program.
					write(t, root, ".sdo/playbooks/partial-orchestration/mitigate.go", "package broken\n")
				}
			}
			paths, err := ChangedPaths(context.Background(), root, base)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NormalizeAuthoredPlaybooks(root, paths); err != nil {
				t.Fatal(err)
			}
			decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: ids, SdkGaps: []*contracts.SdkGap{{FaultClass: "admission", Primitive: "backend decision", Reason: "requires live policy judgement", ScratchScript: "inspect backend availability"}}}
			var notes []sdktest.MitigationReplayResult
			ds, err := Check(context.Background(), Request{MitigationReplayNotes: &notes, Root: root, Base: base, Actor: Responder, Phase: ReflectionPhase, Decision: decision, Publication: &PublicationContext{BaseCommit: base, IncidentID: outcome.IncidentId}, OperatorRoot: filepath.Join("..", "..")}, CompileCandidate)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "broken-peer" {
				if len(ds) != 1 || ds[0].Rule != "program.invalid" || !strings.Contains(ds[0].Message, "no diagnose.go") {
					t.Fatalf("broken peer program not rejected by program policy: %v", ds)
				}
				return
			}
			if len(ds) != 0 {
				t.Fatalf("partial memory rejected: %+v", ds)
			}
			if mode == "mechanical-step" && len(notes) != 1 {
				t.Fatalf("partial program never replayed: %v", notes)
			}
			specs, err := programvalidation.InspectMemory(filepath.Join(root, ".sdo"), "fixture-diagnostics")
			if err != nil {
				t.Fatal(err)
			}
			for _, spec := range specs {
				if spec.ID == "partial-orchestration" {
					t.Fatal("README-only playbook entered executable registry")
				}
			}
		})
	}
}

func TestPartialReflectionResultRoundTrips(t *testing.T) {
	result := &contracts.ReflectionResult{Summary: "saved mechanical step", LearningDecision: "updated", ProposedArtifactIds: []string{"partial-orchestration"}, SdkGaps: []*contracts.SdkGap{{FaultClass: "admission", Primitive: "backend decision", Reason: "requires judgement", ScratchScript: "inspect backend"}}}
	raw, err := protojson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadReflectionResult(context.Background(), "", path)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(result, loaded) {
		t.Fatalf("result lost fields: %v", loaded)
	}
	decision, err := LoadCheckedReflectionDecision(context.Background(), "", path)
	if err != nil {
		t.Fatal(err)
	}
	if ds := ReflectionDecisionContractDiagnostics(decision); len(ds) != 0 {
		t.Fatal(ds)
	}
	if decision.GetLearningDecision() != "updated" || len(decision.GetPlaybookIds()) != 1 || len(decision.GetSdkGaps()) != 1 {
		t.Fatalf("projection lost partial learning: %v", decision)
	}
}
