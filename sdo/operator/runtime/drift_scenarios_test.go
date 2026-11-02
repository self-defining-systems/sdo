package runtime

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type scriptedDriftResponder struct {
	client   *fake.Clientset
	requests chan IncidentRequest
}

func (s scriptedDriftResponder) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	s.requests <- r
	for _, f := range r.Findings {
		service, err := s.client.CoreV1().Services("app").Get(ctx, f.PrimaryResource.Name, metav1.GetOptions{})
		if err != nil {
			return IncidentResult{}, err
		}
		service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
		if _, err := s.client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{}); err != nil {
			return IncidentResult{}, err
		}
	}
	return completedResult(r.IncidentID), nil
}

type serviceDriftView struct{ *sdktest.ReplayReadView }

func (v serviceDriftView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	for _, s := range v.Services() {
		if s.Name == ref.Name {
			return program.Diff{Changed: s.Spec.Ports[0].TargetPort.IntVal != 8080}, nil
		}
	}
	return program.Diff{}, fmt.Errorf("missing service %s", ref.Name)
}

func injectScenarioPort(t *testing.T, client *fake.Clientset, cache *KubernetesCache, name string) {
	t.Helper()
	ctx := context.Background()
	before, err := client.CoreV1().Services("app").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	broken := before.DeepCopy()
	broken.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	if _, err := client.CoreV1().Services("app").Update(ctx, broken, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cache.eventHandler(sdk.WatchKind{APIVersion: "v1", Kind: "Service", Namespace: "app"}).OnUpdate(before, broken)
}

func stepScenario(t *testing.T, c *Controller, cache *KubernetesCache, at time.Time) {
	t.Helper()
	if err := c.StepEvents(context.Background(), at, cache.TakeEvents()); err != nil {
		t.Fatal(err)
	}
}

func acknowledgeScenario(t *testing.T, c *Controller) {
	t.Helper()
	ctx := context.Background()
	effect, ok := c.PendingClosureEffect()
	if !ok {
		t.Fatal("missing closure")
	}
	if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.closureResults)
	c.processBrokerCompletions()
	ack, ok := c.PendingClosureAcknowledgmentEffect()
	if !ok {
		t.Fatal("missing acknowledgment")
	}
	if err := c.ExecuteClosureAcknowledgmentEffect(ctx, ack); err != nil {
		t.Fatal(err)
	}
	awaitBrokerResult(t, c.acknowledgmentResults)
	c.processBrokerCompletions()
}

func dispatchScenario(t *testing.T, c *Controller) {
	t.Helper()
	if effect, ok := c.PendingWorkspaceEffect(); ok {
		if err := c.ExecuteWorkspaceEffect(context.Background(), effect); err != nil {
			t.Fatal(err)
		}
		awaitBrokerResult(t, c.workspaceResults)
		c.processBrokerCompletions()
	}
	executePendingEffect(t, c)
}

