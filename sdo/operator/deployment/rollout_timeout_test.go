package deployment

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const rollingDeployment = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"web","namespace":"demo","uid":"u","generation":1,
"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"replicas":1,"selector":{"matchLabels":{"app":"web"}},
"template":{"metadata":{"annotations":{"sdo.dev/source-tree":"t"}},"spec":{"initContainers":[{"name":"git","image":"x/git@sha256:` + digestA + `"}],
"containers":[{"name":"web","image":"x/web@sha256:` + digestA + `"}]}}},"status":{"observedGeneration":1,"updatedReplicas":1,"readyReplicas":0}}`

const digestA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func rolloutBackend(pods string, limit time.Duration) CommandBackend {
	return CommandBackend{
		ObserveLimit: limit,
		Run: func(_ context.Context, _ string, args []string, _ []byte) ([]byte, error) {
			if len(args) > 1 && args[1] == "pods" {
				return []byte(pods), nil
			}
			return []byte(rollingDeployment), nil
		},
		Wait: func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() },
	}
}

func TestObserveTimeoutForSlowInitContainerIsTypedAndNotAPlanError(t *testing.T) {
	pods := `{"items":[{"metadata":{"name":"web-1"},"status":{"phase":"Pending","initContainerStatuses":[{"name":"git","ready":false,"state":{"running":{}}}],"containerStatuses":[{"name":"web","ready":false,"state":{"waiting":{"reason":"PodInitializing"}}}]}}]}`
	_, err := rolloutBackend(pods, 20*time.Millisecond).Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("want RolloutTimeoutError, got %v", err)
	}
	if !timeout.InfrastructureTimeout() {
		t.Fatal("a still-starting rollout is a time limit, not a plan rejection")
	}
	for _, want := range []string{"still rolling out", "phase=Pending", "init git running", "THE PLAN WAS FINE"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("feedback lacks %q: %v", want, err)
		}
	}
}

func TestObserveTimeoutForFailingPodsIsNotBlamedOnTime(t *testing.T) {
	pods := `{"items":[{"metadata":{"name":"web-1"},"status":{"phase":"Pending","containerStatuses":[{"name":"web","ready":false,"state":{"waiting":{"reason":"ImagePullBackOff"}}}]}}]}`
	_, err := rolloutBackend(pods, 20*time.Millisecond).Observe(context.Background(), "Deployment/demo/web")
	var timeout *RolloutTimeoutError
	if !errors.As(err, &timeout) || timeout.InfrastructureTimeout() {
		t.Fatalf("failing pods must not be classed as a time limit: %v", err)
	}
	if !strings.Contains(err.Error(), "ImagePullBackOff") || strings.Contains(err.Error(), "THE PLAN WAS FINE") {
		t.Fatalf("unexpected feedback: %v", err)
	}
}
