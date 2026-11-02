package mongoauth

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The historical gap must stay honest: an observed safe mounted reference
// and a real open catalog are required, even when the expected fixed state exists.
func TestUnavailableCredentialOrCatalogRemainsUnknown(t *testing.T) {
	for _, missing := range []string{"credential reference", "open catalog"} {
		t.Run(missing, func(t *testing.T) {
			c, err := sdktest.LoadCorpusCase("../../corpus/mongo-auth")
			if err != nil {
				t.Fatal(err)
			}
			calls := c.PhaseRecordings("open")
			if missing == "credential reference" {
				c.Open.PodList[0].Spec.Containers[0].VolumeMounts = nil
			} else {
				calls = nil
			}
			view := &sdktest.ReplayReadView{Snapshot: c.Open, Calls: calls}
			verdict, err := (Program{}).Diagnose(context.Background(), view, c.Bindings)
			if err != nil || verdict.Result != program.Unknown {
				t.Fatalf("unavailable %s became executable evidence: %v %v", missing, verdict, err)
			}
			if missing == "credential reference" {
				if _, err := (Program{}).Mitigate(context.Background(), view, c.Bindings); err == nil {
					t.Fatal("mitigation invented an unobserved credential reference")
				}
			}
		})
	}
}

func TestHotelRepairUsesMountedReferencesAndRestartsApplication(t *testing.T) {
	c, err := sdktest.LoadCorpusCase("../../corpus/mongo-auth")
	if err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}
	for _, pod := range c.Open.Pods() {
		for _, container := range pod.Spec.Containers {
			if len(container.Env) > 0 {
				t.Fatal("hotel Mongo credentials must not use env")
			}
		}
	}
	plan, err := (Program{}).Mitigate(context.Background(), view, c.Bindings)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 2 {
		t.Fatalf("expected catalog repair and dependent restart, got %d", len(plan.Actions))
	}
	repair, ok := plan.Actions[0].(program.EnsureDatabaseUser)
	if !ok || repair.Password.ConfigMap == nil || repair.Target.Auth.Username.ConfigMap == nil || repair.Target.Auth.Password.ConfigMap == nil {
		t.Fatal("repair did not retain mounted credential references")
	}
	restart, ok := plan.Actions[1].(program.RestartRollout)
	if !ok || restart.Ref.Name != "geo" || restart.Ref.Kind != "Deployment" {
		t.Fatal("dependent application restart missing")
	}
}
