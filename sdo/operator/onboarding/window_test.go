package onboarding

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestOnboardingWindowAndRejectionAreImmutableUntilSupportedReset(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	if err := MarkWindow(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	if err := Reject(ctx, client, "control", "interrupted sampling"); err != nil {
		t.Fatal(err)
	}
	if err := Reject(ctx, client, "control", "later healthy period"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{WindowConfigMapName, RejectedConfigMapName} {
		if marked, err := Marked(ctx, client, "control", name); err != nil || !marked {
			t.Fatalf("missing durable marker: %v %v", marked, err)
		}
		cm, err := client.CoreV1().ConfigMaps("control").Get(ctx, name, metav1.GetOptions{})
		if err != nil || cm.Immutable == nil || !*cm.Immutable {
			t.Fatalf("mutable marker: %v %v", cm, err)
		}
		if name == RejectedConfigMapName && cm.Data["reason"] != "interrupted sampling" {
			t.Fatal("later observation overwrote rejection")
		}
	}
	if _, err := ResetControlState(ctx, client, "control"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{WindowConfigMapName, RejectedConfigMapName} {
		if marked, err := Marked(ctx, client, "control", name); err != nil || marked {
			t.Fatalf("supported reset retained marker: %v %v", marked, err)
		}
	}
}
