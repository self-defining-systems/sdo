//go:build integration

package runtime

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/onboarding"
)

func TestSourceInventoryPendingOnboardingUsesFirstClusterEvaluation(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("install prematurely granted authority: %v %v", record, err)
	}
	// Cluster detectors use the existing controller snapshot/prober, with no
	// host resolver or additional health-probe process involved.
	started := time.Now()
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record == nil {
		t.Fatalf("first in-cluster clear did not establish onboarding: %v %v", record, err)
	}
	t.Logf("hermetic first evaluation to persisted record: %s", time.Since(started))
	if !record.EstablishedAt.AsTime().Equal(time.Unix(100, 0)) {
		t.Fatal("lost install boundary")
	}
	if len(c.sourceInventoryFindings) != 0 {
		t.Fatalf("clean fixture produced findings: %+v", c.sourceInventoryFindings)
	}
	additions := latentInventoryObjects()
	c.drift.Events.(*inventoryStore).objects = append(c.drift.Events.(*inventoryStore).objects, additions[0], additions[3], additions[4])
	c.observeSourceInventory(time.Unix(102, 0))
	if ok, remaining := c.verifySourceInventoryLocked(nil); ok || len(remaining) != 3 {
		t.Fatalf("later controller and RBAC must block: %v %+v", ok, remaining)
	}
	if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
}
