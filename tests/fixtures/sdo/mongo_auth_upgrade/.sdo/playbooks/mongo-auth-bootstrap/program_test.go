package mongoauth

import (
	"context"
	"testing"

	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The historical gap must stay honest: an observed safe environment reference
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
				env := c.Open.PodList[0].Spec.Containers[0].Env
				for i := range env {
					if env[i].Name == "APP_PASSWORD" {
						c.Open.PodList[0].Spec.Containers[0].Env = append(env[:i:i], env[i+1:]...)
						break
					}
				}
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
