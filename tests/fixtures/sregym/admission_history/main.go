// Synthetic controller history produced by the real scheduler and state exporter.
package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtime "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type detector struct {
	id       string
	interval time.Duration
}

func (d detector) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: d.id, Interval: d.interval} }
func (d detector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}

type provider struct{}

func (provider) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return sdktest.Snapshot{NamespaceName: "synthetic"}, nil
}

type dispatcher struct{}

func (dispatcher) Dispatch(context.Context, runtime.IncidentRequest) (runtime.IncidentResult, error) {
	panic("clear detectors cannot dispatch")
}
func main() {
	if len(os.Args) > 1 {
		changes()
		return
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c, err := runtime.NewController(runtime.ControllerConfig{Application: "synthetic", Namespace: "synthetic", ResponseTimeout: time.Minute, FiringThreshold: 1, ClearThreshold: 1}, []sdk.Detector{detector{"health", time.Second}, detector{"event-policy", time.Hour}}, provider{}, dispatcher{}, start)
	if err != nil {
		panic(err)
	}
	for _, now := range []time.Time{start, start.Add(10 * time.Second)} {
		if err = c.Step(context.Background(), now, nil); err != nil {
			panic(err)
		}
	}
	state, err := c.ExportState()
	if err != nil {
		panic(err)
	}
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(state)
	if err != nil {
		panic(err)
	}
	if _, err = os.Stdout.Write(raw); err != nil {
		panic(err)
	}
}

func changes() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ns := "synthetic"
	one := int32(1)
	client := fake.NewSimpleClientset(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: ns}}, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "config", Namespace: ns}, Data: map[string]string{"key": "synthetic"}}, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: ns}, Spec: appsv1.DeploymentSpec{Replicas: &one}})
	tracker := runtime.NewStateTracker(runtime.StateTrackerConfig{Client: client, Namespace: ns})
	if err := tracker.Start(ctx); err != nil {
		panic(err)
	}
	defer tracker.Stop()
	now := time.Now()
	tracker.Observe(now, true)
	if err := client.CoreV1().Services(ns).Delete(ctx, "api", metav1.DeleteOptions{}); err != nil {
		panic(err)
	}
	if err := client.CoreV1().ConfigMaps(ns).Delete(ctx, "config", metav1.DeleteOptions{}); err != nil {
		panic(err)
	}
	deployment, err := client.AppsV1().Deployments(ns).Get(ctx, "api", metav1.GetOptions{})
	if err != nil {
		panic(err)
	}
	two := int32(2)
	deployment.Spec.Replicas = &two
	if _, err = client.AppsV1().Deployments(ns).Update(ctx, deployment, metav1.UpdateOptions{}); err != nil {
		panic(err)
	}
	for {
		diff := tracker.Changes(now.Add(time.Second))
		if diff != nil && len(diff.Changes) == 3 {
			rows := []map[string]string{}
			for _, row := range diff.Changes {
				rows = append(rows, map[string]string{"kind": row.Kind, "name": row.Name, "change": row.Change})
			}
			if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"changes": rows}); err != nil {
				panic(err)
			}
			return
		}
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
