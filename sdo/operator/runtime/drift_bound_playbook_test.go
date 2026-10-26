package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/driftbound"
	"sdo.dev/operator/sdk/sdktest"
)

func TestHermeticSourceAbsentDriftSelectsLearnedPlaybook(t *testing.T) {
	ctx := context.Background()
	source, commit := lifecycleTestSource{}, "accepted"
	ref := sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "toy-binding"}
	object := unstructured.Unstructured{Object: map[string]any{"apiVersion": ref.APIVersion, "kind": ref.Kind, "metadata": map[string]any{"name": ref.Name}, "roleRef": map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "toy-role"}}}
	open := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1, Objects: []unstructured.Unstructured{object}}
	fixed := sdktest.Snapshot{NamespaceName: "app", ObjectSnapshotVersion: 1}
	detector := driftbound.Detector{}
	var recordings []sdktest.PrimitiveCall
	for _, phase := range []string{"open", "fixed"} {
		snapshot := open
		if phase == "fixed" {
			snapshot = fixed
		}
		view := NewCycleReadView(LiveReadView{DetectionContext: snapshot, Source: source, SourceCommit: commit})
		findings, err := sdk.DetectObserved(ctx, detector, view)
		if err != nil {
			t.Fatal(err)
		}
		if (phase == "open") != (len(findings) == 1) {
			t.Fatalf("%s findings: %+v", phase, findings)
		}
		again, err := sdk.DetectObserved(ctx, detector, view)
		if err != nil || !reflect.DeepEqual(findings, again) {
			t.Fatalf("same snapshot changed findings: %+v %+v %v", findings, again, err)
		}
		for _, call := range view.RecordedReads() {
			call.Phase = phase
			recordings = append(recordings, call)
		}
	}
	p := driftbound.Program{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	live := NewCycleReadView(LiveReadView{DetectionContext: open, Source: source, SourceCommit: commit})
	findings, err := sdk.DetectObserved(ctx, detector, live)
	if err != nil {
		t.Fatal(err)
	}
	request := IncidentRequest{IncidentID: "binding-origin", Namespace: "app", SourceCommit: commit, Findings: findings}
	provider := &changingProvider{snapshot: live}
	loop := NewResponderLoop(request, registry, provider, []sdk.Detector{detector}, PlanExecutor{}, 5)
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView { return program.ReadFrom(s) }
	runs := loop.InitialDiagnose(ctx)
	coverage := buildPlaybookCoverage(findings, runs, live, nil)
	if len(coverage.Matches) != 1 || len(coverage.UnexplainedFindingFingerprints) != 0 || coverage.Matches[0].MitigationSelection == "" || !coverage.Matches[0].CanMitigate {
		t.Fatalf("drift did not select executable playbook: runs=%+v coverage=%v", runs, coverage)
	}
	plan, err := p.Mitigate(ctx, program.ReadFrom(live), program.Bindings{Objects: program.ObjectBindings{"binding": ref}})
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}

	// Author the same compiled fixture through the complete memory compile/replay gate.
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	write := func(path string, data []byte) {
		t.Helper()
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	read := func(path string) []byte {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	write(".sdo/diagnostics/go.mod", read("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod"))
	write(".sdo/playbooks/source-absent-binding/diagnose.go", read("../sdk/program/testdata/driftbound/program.go"))
	write(".sdo/playbooks/source-absent-binding/mitigate.go", []byte("package driftbound\n"))
	write(".sdo/playbooks/source-absent-binding/README.md", []byte("---\nschema_version: 1\nowner: responder\nfault_class: source-absent-binding\noriginating_incident: binding-origin\n---\n\nRemove unexpected authority only after accepted source confirms absence.\n"))
	write(".sdo/playbooks/README.md", []byte("# Playbooks\n\n- [base](base/README.md)\n- [source-absent-binding](source-absent-binding/README.md)\n"))
	spec := detector.Spec()
	manifest := layout.DetectorManifest{APIVersion: layout.APIVersion, Kind: layout.DetectorManifestKind, SDKVersion: layout.SDKVersion, ID: spec.ID, Constructor: "New", Class: "incident", Owner: "responder", Watches: []layout.Watch{{APIVersion: ref.APIVersion, Kind: ref.Kind}}, Interval: "1s", Persistence: layout.Persistence{Firing: 1, Clearing: 1}, Batching: layout.Batching{Severity: "warn", Debounce: "100ms"}, PossiblePlaybooks: spec.Playbooks, OriginatingIncident: "binding-origin", OriginatingCommit: "fixture"}
	if err := layout.WriteDetector(root, layout.Detector{Class: layout.ClassIncident, Manifest: manifest, Files: []layout.File{{Name: "detector.go", Content: read("../sdk/program/testdata/driftbound/detector.go")}}}); err != nil {
		t.Fatal(err)
	}
	openRaw, _ := json.Marshal(open)
	fixedRaw, _ := json.Marshal(fixed)
	labels := map[string]any{"root_cause_program_ids": []string{p.Spec().ID}, "bindings": program.Bindings{Objects: program.ObjectBindings{"binding": ref}}, "provenance": sdktest.CorpusProvenance{IncidentID: "binding-origin", OpenSHA256: fmt.Sprintf("%x", sha256.Sum256(openRaw)), FixedSHA256: fmt.Sprintf("%x", sha256.Sum256(fixedRaw))}}
	for name, value := range map[string]any{"open.json": open, "fixed.json": fixed, "healthy.json": fixed, "recordings.json": recordings, "labels.json": labels} {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		write(".sdo/corpus/binding-origin/"+name, raw)
	}
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot})
	if err != nil || len(diagnostics) != 0 {
		t.Fatalf("compiled detector/program replay rejected: %v %+v", err, diagnostics)
	}
}
