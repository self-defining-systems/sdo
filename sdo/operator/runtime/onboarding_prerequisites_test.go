package runtime

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/onboarding"
	"testing"
	"time"
)

func TestSourceInventoryOnboardingWaitsForInventoryPrerequisite(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	source := c.drift.Source.(*inventorySource)
	objects := source.objects
	source.err = errors.New("rendered lock not published yet")
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("unavailable inventory consumed attempt: %v", err)
	}
	source.err = nil
	source.objects = objects
	c.sourceInventoryRetryAt = time.Time{}
	if err := c.EvaluateAll(ctx, time.Unix(102, 0)); err != nil {
		t.Fatal(err)
	}
	record, err := onboarding.Load(ctx, client, "control", "app")
	if err != nil || record == nil {
		t.Fatalf("ready inventory did not finalize: %v %v", record, err)
	}
}
