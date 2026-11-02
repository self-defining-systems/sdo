package runtime

import (
	"context"
	"errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/sdk"
	"testing"
	"time"
)

type onboardingReadyInventory struct {
	*inventoryStore
	ready bool
}

func (s *onboardingReadyInventory) SourceDriftReady() bool { return s.ready }

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

func TestSourceInventoryOnboardingUnreadableContinuityCannotGrantAuthority(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	client.PrependReactor("get", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if action.(ktesting.GetAction).GetName() == onboarding.WindowConfigMapName {
			return true, nil, errors.New("continuity read unavailable")
		}
		return false, nil, nil
	})
	if err := c.AttachPendingOnboarding(ctx, client, "control"); err == nil {
		t.Fatal("missing continuity was ignored")
	}
	// Even a caller mistakenly continuing after attachment failure cannot claim.
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err != nil {
		t.Fatal(err)
	}
	if marked, err := onboarding.Marked(ctx, client, "control", onboarding.AttemptConfigMapName); err != nil || marked {
		t.Fatalf("unreadable continuity consumed attempt: %v %v", marked, err)
	}
	if record, err := onboarding.Load(ctx, client, "control", "app"); err != nil || record != nil {
		t.Fatalf("unreadable continuity granted authority: %v %v", record, err)
	}
}

type onboardingUnavailableSnapshot struct{}

func (onboardingUnavailableSnapshot) Snapshot(context.Context) (sdk.DetectionContext, error) {
	return nil, errors.New("snapshot unavailable")
}

func TestSourceInventoryOnboardingContradictionSurvivesSnapshotFailure(t *testing.T) {
	ctx := context.Background()
	c, client := stagedInstallerController(t, inventoryHealth())
	events := c.drift.Events.(*inventoryStore)
	initial := events.objects
	events.objects = append(events.objects, latentInventoryObjects()[0])
	c.provider = onboardingUnavailableSnapshot{}
	if err := c.EvaluateAll(ctx, time.Unix(101, 0)); err == nil {
		t.Fatal("snapshot failure was ignored")
	}
	if marked, err := onboarding.Marked(ctx, client, "control", onboarding.RejectedConfigMapName); err != nil || !marked {
		t.Fatalf("observed contradiction lost on snapshot error: %v %v", marked, err)
	}
	events.objects = initial
	if marked, err := onboarding.Marked(ctx, client, "control", onboarding.AttemptConfigMapName); err != nil || marked {
		t.Fatalf("contradiction consumed attempt: %v %v", marked, err)
	}
}
