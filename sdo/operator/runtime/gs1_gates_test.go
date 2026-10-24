package runtime

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// GL1b used a benchmark application name different from the deployed identity.
// No administrator app-name mapping or content attestation is needed to repair it.
func TestGS1DeployedIdentityAndNamespaceScope(t *testing.T) {
	for _, namespace := range []string{"gallery-live", "sdo-control", "another-app"} {
		t.Run(namespace, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: namespace, Name: "deployed-config"}
			before := planObject(ref, nil)
			before.SetUID("config-uid")
			before.SetResourceVersion("1")
			before.Object["data"] = map[string]any{"mode": "broken"}
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			incident := IncidentRequest{IncidentID: "gl1b", Application: "sregym-gallery", Namespace: "gallery-live", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
			plan := program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "data[mode]", Value: testPlanValue("working")}}}
			result, err := executor.Apply(context.Background(), incident, plan, false)
			if namespace != incident.Namespace {
				if err == nil || len(client.patches) != 0 {
					t.Fatalf("outside scope was mutated: %+v %v", result, err)
				}
				return
			}
			if err != nil || !result.VerificationPassed {
				t.Fatalf("in-scope deployed identity refused: %+v %v", result, err)
			}
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
			if !planValuesEqual(before.Object, client.objects[ref].Object) {
				t.Fatal("pre-image not restored")
			}
		})
	}
}

func TestGS1BestMatchDeterministicWithUncoveredFindings(t *testing.T) {
	findings := []sdk.Finding{{Fingerprint: "one"}, {Fingerprint: "two"}, {Fingerprint: "unexplained", DetectorID: "drift.unexplained"}}
	run := func(id, fp, name string) ProgramRun {
		return ProgramRun{ProgramID: id, ProgramRevision: "new-revision", FindingFingerprint: fp, Bindings: program.Bindings{Objects: program.ObjectBindings{"object": {Kind: "Service", Namespace: "app", Name: name}}}, Verdict: program.Verdict{Result: program.Match}}
	}
	runs := []ProgramRun{run("z", "one", "a"), run("b", "one", "b"), run("a", "two", "z"), run("a", "one", "z"), run("b", "two", "b"), run("a", "one", "a"), run("a", "two", "a")}
	trust := map[string]ProgramTrust{}
	for _, id := range []string{"a", "b", "z"} {
		trust[id] = ProgramTrust{Trusted: true, VerifiedSuccesses: 1, LastChange: "old-revision"}
	}
	for offset := 0; offset < len(runs); offset++ {
		rotated := append(append([]ProgramRun(nil), runs[offset:]...), runs[:offset]...)
		selected, _, reason := selectTrustedFastPathRuns(findings, rotated, trust, 1, sdktest.Snapshot{})
		if reason != "" || len(selected) != 1 || selected[0].ProgramID != "a" || selected[0].Bindings.Objects["object"].Name != "a" {
			t.Fatalf("order %d: selected=%+v reason=%s", offset, selected, reason)
		}
	}
}

func TestGS1TrustSurvivesProgramRevision(t *testing.T) {
	first := verifiedTrustOutcome(t, "targetport", "revision-a")
	next := &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "targetport", ProgramRevision: "revision-b"}}}
	state := settledTrustStats([]*contracts.OutcomeRecord{first, next}, 1)["targetport"]
	if !state.Trusted || state.VerifiedSuccesses != 1 || state.LastChange != "revision-b" {
		t.Fatalf("revision reset trust: %+v", state)
	}
}

func TestGS1LifecycleKindsAndAcceptedSourceDelete(t *testing.T) {
	for _, create := range []bool{false, true} {
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "accepted-workload"}
		if create {
			ref = sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "accepted-policy"}
		}
		object := planObject(ref, map[string]any{})
		object.SetUID("existing-uid")
		object.SetResourceVersion("1")
		client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
		var action program.Action = program.DeleteObject{Ref: ref, Reason: "replace a source-present failing workload"}
		if create {
			delete(client.objects, ref)
			action = program.CreateFromSource{Ref: ref, Reason: "restore a missing accepted policy"}
		}
		source := lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}
		executor := PlanExecutor{Client: client, Source: source, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
		incident := IncidentRequest{Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions"}
		result, err := executor.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{action}}, false)
		if err != nil || !result.Applied || !result.VerificationPassed {
			t.Fatalf("create=%t: %+v %v", create, result, err)
		}
		if create {
			if err := executor.Rollback(context.Background(), result); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := executor.Rollback(context.Background(), result); err == nil || client.creates != 0 {
				t.Fatal("delete was not disclosed as irreversible")
			}
		}
	}
}

func TestGS1TrustDefaultAndRealFailure(t *testing.T) {
	success := verifiedTrustOutcome(t, "targetport", "revision-a")
	settledTrustStats([]*contracts.OutcomeRecord{success})
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{success})["targetport"]; !state.Trusted || state.VerifiedSuccesses != 1 {
		t.Fatalf("default threshold: %+v", state)
	}
	failure := &contracts.OutcomeRecord{ProgramFailureSignals: []string{"fast_path_failure:targetport@revision-a"}}
	if state := ProgramTrustStats([]*contracts.OutcomeRecord{success, failure})["targetport"]; state.Trusted || state.VerifiedSuccesses != 0 || state.Failures != 1 {
		t.Fatalf("real failure retained trust: %+v", state)
	}
}
