package runtime

import (
	"context"
	"errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/onboarding"
	"testing"
	"time"
)

type onboardingReadyInventory struct {
	*inventoryStore
	ready bool
}

func (s *onboardingReadyInventory) SourceDriftReady() bool { return s.ready }

func TestSourceInventoryOnboardingUnmetPrerequisitesNeverClaim(t *testing.T) {
	for _, mode := range []string{"cache", "stale-inventory", "health-authority", "no-health", "claim-transport"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			c, client := stagedInstallerController(t, inventoryHealth())
			recoverPrerequisite := func() {}
			switch mode {
			case "stale-inventory":
				c.sourceInventory = NewSourceInventory("app", "stale", c.drift.Source.(*inventorySource).objects)
				c.sourceInventoryRetryAt = time.Now().Add(time.Hour)
				recoverPrerequisite = func() { c.sourceInventoryRetryAt = time.Time{} }
			case "cache":
				events := &onboardingReadyInventory{inventoryStore: c.drift.Events.(*inventoryStore)}
				c.drift.Events = events
				recoverPrerequisite = func() { events.ready = true }
			case "health-authority":
				c.CheckHealth = func(context.Context) error { return errors.New("judge refresh incomplete") }
				recoverPrerequisite = func() { c.CheckHealth = nil }
			case "no-health":
				ids := c.healthDetectorIDs
				c.healthDetectorIDs = nil
				recoverPrerequisite = func() { c.healthDetectorIDs = ids }
			case "claim-transport":
				failed := false
				client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
					if action.(ktesting.CreateAction).GetObject().(metav1.Object).GetName() == onboarding.AttemptConfigMapName && !failed {
						failed = true
						return true, nil, errors.New("claim connection failed")
					}
					return false, nil, nil
				})
			}
			err := c.EvaluateAll(ctx, time.Unix(101, 0))
			if mode == "health-authority" {
				if err == nil {
					t.Fatal("missing health authority allowed evaluation")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().ConfigMaps("control").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("unmet %s consumed attempt: %v", mode, err)
			}
			if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
				t.Fatalf("unmet %s granted authority: %v %v", mode, record, err)
			}
			recoverPrerequisite()
			if err := c.EvaluateAll(ctx, time.Unix(102, 0)); err != nil {
				t.Fatal(err)
			}
			if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record == nil {
				t.Fatalf("ready %s could not finalize: %v %v", mode, record, err)
			}
		})
	}
}

func TestSourceInventoryOnboardingLostClaimReplyNeverRetries(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	creates := 0
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		obj := action.(ktesting.CreateAction).GetObject()
		if obj.(metav1.Object).GetName() != onboarding.AttemptConfigMapName {
			return false, nil, nil
		}
		creates++
		if creates == 1 {
			if err := client.Tracker().Create(action.GetResource(), obj, "control"); err != nil {
				t.Fatal(err)
			}
			return true, nil, errors.New("lost durable claim reply")
		}
		return false, nil, nil
	})
	for _, at := range []int64{101, 102, 103} {
		if err := c.EvaluateAll(ctx, time.Unix(at, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if creates != 2 {
		t.Fatalf("expected lost reply followed by create-once resolution: %d", creates)
	}
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("lost claim reply retried evaluation authority: %v %v", record, err)
	}
}
