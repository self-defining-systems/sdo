package validate

import (
	"context"
	"go/format"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// Real authoring -> shared check -> SDK evaluation on a changed Pod. Generic
// health grace is deliberately irrelevant to this spec-based incident detector.
func TestLateInitCauseDetectorPassesReflectionCheckWithReplayOff(t *testing.T) {
	root := goodTree(t)
	gitIn(t, root, "init")
	gitIn(t, root, "config", "user.email", "synthetic@example.invalid")
	gitIn(t, root, "config", "user.name", "Synthetic")
	outcome := repairCoverageOutcome()
	outcome.IncidentId = "inc-1"
	outcome.RepairActions[0].Resources = []*contracts.ObjectRef{{ApiVersion: "v1", Kind: "Pod", Namespace: "demo", Name: "toy"}}
	outcome.ConfirmedRootCauses = []*contracts.ConfirmedRootCause{{Summary: "unexpected init container", Resources: outcome.RepairActions[0].Resources}}
	for _, r := range outcome.RepairActions {
		r.Target = "Pod demo/toy"
		r.Summary = "saved init repair"
		r.Details = "synthetic accepted repair"
	}
	raw, err := protojson.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	write(t, root, layout.OutcomePath("inc-1"), string(raw))
	commit(t, root, "accepted outcome\n\nSDO-Incident: inc-1\nSDO-Phase: outcome")
	base := gitIn(t, root, "rev-parse", "HEAD")
	dir := ".sdo/diagnostics/detectors/incidents/cache/"
	source, err := os.ReadFile(filepath.Join(root, dir, "detector.go"))
	if err != nil {
		t.Fatal(err)
	}
	source = []byte(strings.Replace(string(source), "func (Detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) { return nil, nil }", `func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
 var findings []sdk.Finding
 for _, pod := range snapshot.Pods() {
  for _, init := range pod.Spec.InitContainers {
   if init.Name != "synthetic-dependency-wait" { continue }
   ref := sdk.ObjectRefFrom("Pod", "v1", &pod)
   findings = append(findings, sdk.Finding{RuleID:"unexpected-init", Status:sdk.FindingActive, Severity:sdk.FindingSeverity("warn"), Summary:"unexpected dependency wait", Evidence:"synthetic init container is present", PrimaryResource:ref, Playbooks:[]string{"base"}, Fingerprint:pod.Namespace+"/"+pod.Name+"/init"})
  }
 }
 return findings, nil
}`, 1))
	formatted, err := format.Source(source)
	if err != nil {
		t.Fatal(err)
	}
	write(t, root, dir+"detector.go", string(formatted))
	tests, err := format.Source([]byte(`package detector
import (
 "context"
 "testing"
 corev1 "k8s.io/api/core/v1"
 metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
 "sdo.dev/operator/sdk/sdktest"
)
func TestChangedPod(t *testing.T) {
 for _, name := range []string{"toy", "renamed"} {
  for _, present := range []bool{false, true} {
   pod := corev1.Pod{ObjectMeta:metav1.ObjectMeta{Name:name, Namespace:"demo"}}
   if present { pod.Spec.InitContainers=[]corev1.Container{{Name:"synthetic-dependency-wait"}} }
   got, err := New().Detect(context.Background(), sdktest.Snapshot{NamespaceName:"demo", PodList:[]corev1.Pod{pod}})
   want := 0; if present { want=1 }
   if err != nil || len(got)!=want { t.Fatalf("present=%v: %v %v", present, got, err) }
   if present && got[0].PrimaryResource.Name!=name { t.Fatal("lost resource binding") }
  }
 }
}
`))
	if err != nil {
		t.Fatal(err)
	}
	write(t, root, dir+"detector_test.go", string(tests))
	ds, err := Check(context.Background(), Request{Root: root, Base: base, Actor: Responder, Phase: ReflectionPhase, Replay: "off", Decision: &contracts.ReflectionDecision{LearningDecision: "updated"}, Publication: &PublicationContext{BaseCommit: base, IncidentID: "inc-1"}, OperatorRoot: filepath.Join("..", "..")}, CompileCandidate)
	if err != nil || len(ds) != 0 {
		t.Fatalf("incident detector rejected: %v %v", ds, err)
	}
}
