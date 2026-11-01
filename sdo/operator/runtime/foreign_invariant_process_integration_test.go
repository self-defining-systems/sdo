//go:build integration

package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/kubernetes/fake"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

// Execute the actual scripted Go artifact, not a second implementation of its
// predicate. Only primitive reads are recorded at the process boundary.
type compiledForeignInvariant struct{ binary string }

func (compiledForeignInvariant) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "networkpolicy-service-isolation", Params: []program.ParamSpec{{Name: "policy", Kind: "NetworkPolicy", Required: true}}}
}

func (p compiledForeignInvariant) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	snapshot, err := sdktest.CaptureSnapshot(view, view.Namespace())
	if err != nil {
		return program.Verdict{}, err
	}
	recording := &sdktest.RecordingReadView{ReadView: view}
	if _, err := recording.DiffAgainstSource(bindings["policy"]); err != nil {
		return program.Verdict{}, err
	}
	for _, service := range view.Services() {
		if _, err := recording.DiffAgainstSource(sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}); err != nil {
			return program.Verdict{}, err
		}
	}
	data, err := json.Marshal(struct {
		Snapshot sdktest.Snapshot
		Calls    []sdktest.PrimitiveCall
		Bindings program.Bindings
	}{snapshot, recording.Calls, bindings})
	if err != nil {
		return program.Verdict{}, err
	}
	cmd := exec.CommandContext(ctx, p.binary)
	cmd.Stdin = bytes.NewReader(data)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return program.Verdict{}, fmt.Errorf("scripted invariant: %w: %s", err, output)
	}
	var verdict program.Verdict
	if err := json.Unmarshal(output, &verdict); err != nil {
		return program.Verdict{}, err
	}
	return verdict, nil
}

func compileForeignInvariant(t *testing.T) compiledForeignInvariant {
	t.Helper()
	root := t.TempDir()
	sdkRoot, err := filepath.Abs("../sdk")
	if err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module foreign-invariant\n\ngo 1.26.0\n\nrequire sdo.dev/operator/sdk v0.0.0\nreplace sdo.dev/operator/sdk => %s\n", sdkRoot)
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "candidate"), 0700); err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../../../benchmarks/sregym/fastloop/scripted_networkpolicy_program/diagnose.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "candidate", "diagnose.go"), source, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte(foreignInvariantRunner), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "diagnose")
	cmd := exec.Command("go", "build", "-p", "8", "-mod=mod", "-o", binary, ".")
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compile scripted learned invariant: %v: %s", err, output)
	}
	return compiledForeignInvariant{binary: binary}
}

func compileReflectedForeignInvariant(t *testing.T, captureRoot, incidentID string) compiledForeignInvariant {
	t.Helper()
	capture, err := memory.IncidentCorpusDir(captureRoot, incidentID)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir, err := filepath.Abs("../../../benchmarks/sregym/fastloop/scripted_networkpolicy_program")
	if err != nil {
		t.Fatal(err)
	}
	app := t.TempDir()
	copyFastloopTree(t, "../../../tests/fixtures/sdo/playbook_program_app/.sdo", filepath.Join(app, ".sdo"))
	target := filepath.Join(app, ".sdo/playbooks/networkpolicy-service-isolation")
	copyFastloopTree(t, sourceDir, target)
	writeCapturedProgramCorpus(t, app, capture, incidentID, "networkpolicy-service-isolation", "policy", "NetworkPolicy", "app", "first-deny", "healthy-peer", "detect")
	workspace, err := builder.BuildWorkspace(context.Background(), builder.Config{ApplicationRoot: app, DiagnosticsRoot: filepath.Join(app, ".sdo", "diagnostics"), OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	if _, err := workspace.ValidateAndBuild(context.Background()); err != nil {
		t.Fatalf("validate captured policy invariant and rename replay: %v", err)
	}
	runner := strings.Replace(foreignInvariantRunner, `candidate "foreign-invariant/candidate"`, `generated "`+workspace.ModulePath+`/generated"`, 1)
	runner = strings.Replace(runner, `verdict,err:=(candidate.Program{}).Diagnose(context.Background(),view,input.Bindings)`, `registry,err:=generated.Programs(); if err!=nil {panic(err)}
    candidate,ok:=registry.Diagnoser("networkpolicy-service-isolation"); if !ok {panic("missing learned program")}
    verdict,err:=candidate.Diagnose(context.Background(),view,input.Bindings)`, 1)
	dir := filepath.Join(workspace.Directory, "cmd", "foreign-invariant")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(runner), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(workspace.Directory, "foreign-invariant")
	cmd := exec.Command("go", "build", "-p", "8", "-o", binary, "./cmd/foreign-invariant")
	cmd.Dir = workspace.Directory
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build accepted invariant runner: %v: %s", err, output)
	}
	return compiledForeignInvariant{binary: binary}
}

func TestHermeticForeignNetworkPolicyInvariantRepeat(t *testing.T) {
	captureRoot, incidentID, source, commit := runForeignPolicyIncident(t)
	learned := compileReflectedForeignInvariant(t, captureRoot, incidentID)
	registry, err := program.NewRegistry([]program.Diagnoser{learned}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The first verified incident supplies the diagnosis-only artifact. A distinct
	// policy name and opposite direction exercise reuse rather than literal replay.
	policy := foreignPolicy("second-deny")
	policy.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeEgress}
	snapshot := policyWorkloadSnapshot(*policy)
	start := time.Unix(100, 0).UTC()
	cache, err := NewKubernetesCache(KubernetesCacheConfig{Namespace: "app", Client: fake.NewSimpleClientset(), TrackDrift: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	watch := sdk.WatchKind{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app"}
	cache.eventHandler(watch).OnAdd(policy, false)
	config := testControllerConfig()
	config.Namespace, config.SourceCommit, config.FiringThreshold, config.ClearThreshold = "app", commit, 1, 1
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	provider := &changingProvider{snapshot: snapshot}
	controller, err := NewController(config, nil, provider, dispatcher, start)
	if err != nil {
		t.Fatal(err)
	}
	sourceAvailable := true
	controller.ConfigureDrift(DriftControllerConfig{ProgramPersistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Events: cache, Programs: registry, View: func(snapshot sdk.DetectionContext) program.ReadView {
		if !sourceAvailable {
			return LiveReadView{Carrier: inputs.New(snapshot)}
		}
		return LiveReadView{Carrier: inputs.New(snapshot), Source: source, SourceCommit: commit}
	}})
	if err := controller.StepEvents(context.Background(), start.Add(time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if !controller.IncidentOpen() {
		t.Fatalf("compiled program did not open repeat incident: %+v", exportState(t, controller))
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	if len(request.Findings) != 1 || request.Findings[0].DetectorID != "drift.networkpolicy-service-isolation" || request.Findings[0].Severity != sdk.SeverityLatent {
		t.Fatalf("repeat did not use invariant: %+v", request)
	}
	awaitDispatchCompletionQueued(t, controller)
	snapshot.NetworkPolicyList = nil
	provider.snapshot = snapshot
	cache.eventHandler(watch).OnDelete(policy)
	sourceAvailable = false
	if err := controller.StepEvents(context.Background(), start.Add(2*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if _, ok := controller.PendingIncidentClosure(); ok {
		t.Fatal("an Unknown diagnosis cleared the learned finding")
	}
	sourceAvailable = true
	cache.eventHandler(watch).OnDelete(policy)
	if err := controller.StepEvents(context.Background(), start.Add(3*time.Second), cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
	if _, ok := controller.PendingIncidentClosure(); !ok {
		t.Fatal("deleting the policy left the learned invariant finding active")
	}

}

func TestForeignNetworkPolicyInvariantNearMisses(t *testing.T) {
	t.Parallel()
	source, commit := foreignSource(t)
	learned := compileForeignInvariant(t)
	for _, tc := range []struct {
		name     string
		policies []networkingv1.NetworkPolicy
		want     program.Result
	}{
		{"deny ingress", []networkingv1.NetworkPolicy{*foreignPolicy("deny")}, program.Match},
		{"no policy", nil, program.NoMatch},
		{"unrelated selector", func() []networkingv1.NetworkPolicy {
			p := foreignPolicy("deny")
			p.Spec.PodSelector.MatchLabels = map[string]string{"app": "elsewhere"}
			return []networkingv1.NetworkPolicy{*p}
		}(), program.NoMatch},
		{"additive allow", func() []networkingv1.NetworkPolicy {
			allow := foreignPolicy("allow")
			allow.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{}}
			return []networkingv1.NetworkPolicy{*foreignPolicy("deny"), *allow}
		}(), program.NoMatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := LiveReadView{Carrier: inputs.New(policyWorkloadSnapshot(tc.policies...)), Source: source, SourceCommit: commit}
			verdict, err := learned.Diagnose(context.Background(), view, program.Bindings{"policy": {APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "deny"}})
			if err != nil || verdict.Result != tc.want {
				t.Fatalf("verdict=%+v error=%v", verdict, err)
			}
		})
	}
}
