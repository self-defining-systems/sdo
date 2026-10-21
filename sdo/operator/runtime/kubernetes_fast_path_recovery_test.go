package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestFailedFastPathMarkerRestoresAttemptAndTrustFailures(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	loop := recoveryLoop(t, client)
	result, err := loop.Executor.Apply(context.Background(), loop.Request, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8080}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	loop.fastPathPrograms = []string{"targetport"}
	loop.batches = 1
	loop.addFailureSignal("fast_path_failure:targetport@revision-a")
	loop.applications["fast-path-1"] = result
	loop.applicationsLog = []RecordedPlanApplication{{ID: "fast-path-1", ProgramIDs: []string{"targetport"}, Result: result, Error: "verification failed", RollbackError: "API unavailable", StartedAt: time.Now()}}
	snapshot, _ := json.Marshal(loop.toolState())
	application, _ := json.Marshal(result)
	images, _ := json.Marshal(result.PreImages)
	marker := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: IncidentJobName(loop.Request.IncidentID) + "-fast-path", Namespace: "operator"}, Data: map[string]string{"state": "failed", "summary": "verification failed", "tool_state": string(snapshot), "application": string(application), "preimages": string(images), "program_ids": `["targetport"]`, "trust_failure_signals": `["fast_path_failure:targetport@revision-a"]`}}
	dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(marker), Namespace: "operator"}
	restarted := recoveryLoop(t, client)
	_, _, resolved, err := dispatcher.tryFastPath(context.Background(), loop.Request, restarted)
	if err != nil || resolved {
		t.Fatalf("rejoin: %v %v", resolved, err)
	}
	if len(restarted.PlanApplications()) != 1 || len(restarted.FailureSignals()) == 0 || restarted.LoopsUsed() != 1 {
		t.Fatalf("lost failed attempt: apps=%v signals=%v budget=%d", restarted.PlanApplications(), restarted.FailureSignals(), restarted.LoopsUsed())
	}
	if len(restarted.fastPathPrograms) != 1 {
		t.Fatal("lost failed program identity")
	}
	value, _ := testField(client.objects[ref], "spec.ports.0.targetPort")
	if value != float64(9999) {
		t.Fatalf("failed rollback was not retried: %v", value)
	}
	durable, _ := dispatcher.Client.CoreV1().ConfigMaps("operator").Get(context.Background(), marker.Name, metav1.GetOptions{})
	if durable.Data["preimages"] == "" || durable.Data["trust_failure_signals"] == "" {
		t.Fatal("recovery discarded pre-images or trust evidence")
	}
}
func TestFailedFastPathMarkerRetainsConflictingRollbackForNextRestart(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: planObject(ref, map[string]any{"ports": []any{map[string]any{"targetPort": float64(9999)}}})}}
	loop := recoveryLoop(t, client)
	result, err := loop.Executor.Apply(context.Background(), loop.Request, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: "spec.ports[0].targetPort", Value: 8080}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = program.ApplySetField(client.objects[ref].Object, "spec.ports[0].targetPort", float64(7777))
	encoded, _ := json.Marshal(result)
	marker := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: IncidentJobName(loop.Request.IncidentID) + "-fast-path", Namespace: "operator"}, Data: map[string]string{"state": "failed", "summary": "rollback pending", "application": string(encoded), "preimages": "retained", "program_ids": `["targetport"]`, "trust_failure_signals": `["fast_path_failure:targetport@revision-a"]`}}
	dispatcher := KubernetesJobDispatcher{Client: fake.NewSimpleClientset(marker), Namespace: "operator"}
	for i := 0; i < 2; i++ {
		restarted := recoveryLoop(t, client)
		_, summary, _, err := dispatcher.tryFastPath(context.Background(), loop.Request, restarted)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(summary, "rollback") || len(restarted.PlanApplications()) != 1 {
			t.Fatalf("lost recovery state: %s %+v", summary, restarted.PlanApplications())
		}
		durable, _ := dispatcher.Client.CoreV1().ConfigMaps("operator").Get(context.Background(), marker.Name, metav1.GetOptions{})
		if durable.Data["application"] == "" || durable.Data["preimages"] == "" || durable.Data["trust_failure_signals"] == "" {
			t.Fatal("failed rollback journal discarded")
		}
	}
	value, _ := testField(client.objects[ref], "spec.ports.0.targetPort")
	if value != float64(7777) {
		t.Fatalf("rollback overwrote conflict: %v", value)
	}
}
