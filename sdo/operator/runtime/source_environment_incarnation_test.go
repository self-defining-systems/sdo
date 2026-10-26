package runtime

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
)

// SDO AUD-3: an install observation cannot exempt a replacement or a changed
// configuration, even when the one healthy-onboarding attempt failed.
func TestInstallEnvironmentIncarnationAfterFailedHealth(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, mutation := range []string{"replacement-and-rules", "replacement-only", "rules-only", "owner"} {
			t.Run(mutation+map[bool]string{false: "", true: "/restart"}[restart], func(t *testing.T) {
				health := inventoryHealth().(*sequenceDetector)
				health.samples = [][]sdk.Finding{{stateFinding("unhealthy")}}
				c, client := stagedInstallerController(t, health)
				ctx := context.Background()
				if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
					t.Fatal(err)
				}
				if c.namespaceOnboarding != nil {
					t.Fatal("failed health granted onboarding authority")
				}
				if restart {
					state := exportState(t, c)
					next, err := NewController(c.config, []sdk.Detector{health}, c.provider, c.dispatcher, time.Unix(102, 0))
					if err != nil {
						t.Fatal(err)
					}
					next.ConfigureDrift(*c.drift)
					if err := next.RestoreState(state); err != nil {
						t.Fatal(err)
					}
					if err := next.AttachPendingOnboarding(ctx, client, "control"); err != nil {
						t.Fatal(err)
					}
					c = next
				}
				if err := c.prepareSourceInventory(ctx); err != nil {
					t.Fatal(err)
				}
				c.observeSourceInventory(time.Unix(103, 0))
				if len(c.sourceInventoryFindings) != 0 {
					t.Fatalf("unchanged environment became drift: %+v", c.sourceInventoryFindings)
				}
				store := c.drift.Events.(*inventoryStore)
				for i, o := range store.objects {
					if o.GetKind() != "ClusterRole" || o.GetName() != "configmap-cleanup-controller" {
						continue
					}
					changed := o.DeepCopy()
					if mutation == "replacement-and-rules" || mutation == "replacement-only" {
						changed.SetUID(types.UID("new-incarnation"))
					}
					if mutation == "replacement-and-rules" || mutation == "rules-only" {
						changed.Object["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"delete"}}}
					}
					if mutation == "owner" {
						changed.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "ServiceAccount", Name: "new-owner", UID: types.UID("owner")}})
					}
					store.objects[i] = changed
				}
				c.observeSourceInventory(time.Unix(104, 0))
				clear, findings := c.verifySourceInventoryLocked(nil)
				if clear || len(findings) != 1 || findings[0].PrimaryResource.Kind != "ClusterRole" {
					t.Fatalf("changed environment incarnation inherited install exemption: clear=%v findings=%+v", clear, findings)
				}
				if len(c.sourceInventoryFindings) != 1 {
					t.Fatalf("status/verification disagree: %+v", c.sourceInventoryFindings)
				}
			})
		}
	}
}
