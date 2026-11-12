package core

import (
	"context"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/sdk"
)

// Every process that evaluates generic-health detectors stamps the operator's self-heal hold on its snapshots, so a
// deployment verification (--evaluate-once) judges a held condition exactly as the controller does.
func TestSnapshotProviderStampsTheConfiguredSelfHealHold(t *testing.T) {
	provider := KubernetesSnapshotProvider{Namespace: "app", Client: fake.NewSimpleClientset(), SelfHealHold: time.Minute}
	view, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := sdk.SelfHealHold(view); got != time.Minute {
		t.Fatalf("hold = %s, want 1m0s", got)
	}
	unset := KubernetesSnapshotProvider{Namespace: "app", Client: fake.NewSimpleClientset()}
	view, err = unset.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := sdk.SelfHealHold(view); got != sdk.DefaultSelfHealHold {
		t.Fatalf("an unset hold must fall back to the SDK default, got %s", got)
	}
}
