package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type kubeCountingRead struct {
	program.ReadView
	calls atomic.Int32
}

func (v *kubeCountingRead) KubernetesRead(program.KubernetesRequest) (json.RawMessage, error) {
	v.calls.Add(1)
	return json.RawMessage(`{"apiVersion":"apps/v1","kind":"DeploymentList","items":[]}`), nil
}
func TestClientGoDetectorAndDiagnoseShareCycle(t *testing.T) {
	base := &kubeCountingRead{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})}
	cycle := NewCycleReadView(base)
	options := metav1.ListOptions{LabelSelector: "app=api", Limit: 100}
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			client, err := program.Kubernetes(program.ReadFrom(cycle))
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := client.AppsV1().Deployments("app").List(context.Background(), options); err != nil {
				t.Error(err)
			}
		}()
	}
	readers.Wait()
	if base.calls.Load() != 1 {
		t.Fatalf("one cycle repeated API reads %d times", base.calls.Load())
	}
	captured, err := sdktest.CaptureSnapshot(cycle, "app")
	if err != nil {
		t.Fatal(err)
	}
	offline, err := program.Kubernetes(program.ReadFrom(captured))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := offline.AppsV1().Deployments("app").List(context.Background(), options); err != nil {
		t.Fatalf("detector observation did not replay: %v", err)
	}
}

// A successful typed API call cannot hide an ignored failure from the judge.
type ignoredClientRead struct{}

func (ignoredClientRead) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "ignored-client-error"}
}
func (ignoredClientRead) Diagnose(ctx context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	client, err := program.Kubernetes(v)
	if err != nil {
		return program.Verdict{}, err
	}
	_, _ = client.CoreV1().Pods("app").Get(ctx, "missing", metav1.GetOptions{})
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestIgnoredClientGoReadFailureRemainsUnknown(t *testing.T) {
	got := program.RunDiagnose(context.Background(), ignoredClientRead{}, program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), program.Bindings{}, time.Second)
	if got.Result != program.Unknown {
		t.Fatalf("ignored client failure cleared: %+v", got)
	}
}
