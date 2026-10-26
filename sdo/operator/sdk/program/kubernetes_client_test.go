package program_test

import (
	"context"
	"encoding/json"
	"errors"
	"sdo.dev/operator/sdk"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type clientReadFixture struct{ program.ReadView }

func (clientReadFixture) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	return json.RawMessage(`{"apiVersion":"apps/v1","kind":"DeploymentList","items":[{"metadata":{"name":"api"},"spec":{"replicas":3}}]}`), nil
}

func TestClientGoReadsUseExistingRecordingAndReplay(t *testing.T) {
	recorded := &sdktest.RecordingReadView{ReadView: clientReadFixture{}}
	client, err := program.Kubernetes(recorded)
	if err != nil {
		t.Fatal(err)
	}
	options := metav1.ListOptions{LabelSelector: "role=api", Limit: 10}
	got, err := client.AppsV1().Deployments("app").List(context.Background(), options)
	if err != nil || got == nil {
		t.Fatalf("live: %v %v", got, err)
	}
	if len(recorded.Calls) != 1 || recorded.Calls[0].Method != "KubernetesRead" {
		t.Fatalf("second recording path: %+v", recorded.Calls)
	}
	replay, err := program.Kubernetes(&sdktest.ReplayReadView{Calls: recorded.Calls})
	if err != nil {
		t.Fatal(err)
	}
	offline, err := replay.AppsV1().Deployments("app").List(context.Background(), options)
	if err != nil || offline == nil || *offline.Items[0].Spec.Replicas != *got.Items[0].Spec.Replicas {
		t.Fatalf("replay: %v %v", offline, err)
	}
	_, err = replay.AppsV1().Deployments("app").List(context.Background(), metav1.ListOptions{LabelSelector: "role=other"})
	if err == nil {
		t.Fatal("uncaptured selector fabricated evidence")
	}
}

func TestKubernetesDiscoveryReadsAndSecretBoundary(t *testing.T) {
	for _, path := range []string{"/api", "/apis", "/api/v1", "/apis/apps/v1", "/version", "/openapi/v2"} {
		if err := program.ValidateKubernetesRead(program.KubernetesRequest{Method: "GET", Path: path}); err != nil {
			t.Errorf("discovery %s: %v", path, err)
		}
	}
	for _, path := range []string{"/api/v1/secrets", "/api/v1/namespaces/app/secrets/key", "/api/v1/watch/namespaces/app/secrets", "/api/v1/namespaces/app/pods/api/exec"} {
		if err := program.ValidateKubernetesRead(program.KubernetesRequest{Method: "GET", Path: path}); err == nil {
			t.Errorf("unsafe read %s accepted", path)
		}
	}
}

type boundClientFixture struct {
	clientReadFixture
	absent bool
}

func (v boundClientFixture) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{SourceAbsent: v.absent}, nil
}
func TestKubernetesClientRejectsDisappearedSourceRole(t *testing.T) {
	bindings := program.Bindings{Objects: program.ObjectBindings{"workload": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}}}
	if _, err := program.Kubernetes(boundClientFixture{}, bindings); err != nil {
		t.Fatal(err)
	}
	_, err := program.Kubernetes(boundClientFixture{absent: true}, bindings)
	var stale *program.StaleBindingError
	if !errors.As(err, &stale) || stale.Role != "workload" {
		t.Fatalf("removed source role was not stale: %v", err)
	}
}
