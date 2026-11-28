//go:build integration

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
)

// interruptedReadiness is a deployer whose apply of the one phase fails.
type interruptedReadiness struct {
	lifecycle.SourceReadinessVerifier
}

func (interruptedReadiness) ApplyAndVerify(ctx context.Context, checkout string, _ *contracts.DeploymentPlan) (lifecycle.DeploymentReceipt, error) {
	tree, err := broker.ProposalTreeDigest(ctx, checkout)
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	effective, err := rendered.Effective(ctx, rendered.ObjectStore{Dir: checkout}, rendered.BaseTree(tree))
	if err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	write := rendered.TargetWrite{Phase: "default", Pins: rendered.ReceiptPins{EffectiveTree: effective.ID, BaseTree: tree}}
	if err := lifecycle.RecordTarget(ctx, write); err != nil {
		return lifecycle.DeploymentReceipt{}, err
	}
	return lifecycle.DeploymentReceipt{}, errors.New("apply phase \"default\": refused")
}

// From a held head deployment to what a started controller's source reads
// return: the record the head deployer makes reaches the plan source.
func TestAStartedControllerReadsTheHeldHeadsTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	root := t.TempDir()
	accepted := filepath.Join(root, "accepted")
	copyGoodMemory(t, accepted)
	writePlanFile(t, accepted, `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}}}`+"\n")
	if err := os.MkdirAll(filepath.Join(accepted, "k8s"), 0o755); err != nil {
		t.Fatal(err)
	}
	commit := func(release string, args ...string) string {
		t.Helper()
		manifest := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings, namespace: shop}\ndata: {release: '" + release + "'}\n"
		if err := os.WriteFile(filepath.Join(accepted, "k8s", "settings.yaml"), []byte(manifest), 0o644); err != nil {
			t.Fatal(err)
		}
		repairGit(t, accepted, "add", "-A")
		repairGit(t, accepted, append([]string{"commit", "-q", "-m", "release " + release}, args...)...)
		return repairGit(t, accepted, "rev-parse", "HEAD")
	}
	repairGit(t, accepted, "init", "-q")
	repairGit(t, accepted, "config", "user.name", "Synthetic Test")
	repairGit(t, accepted, "config", "user.email", "test@example.invalid")
	first := commit("1", "-m", "SDO-Actor: health_judge")
	repo, err := broker.New(accepted, filepath.Join(root, "proposals"), func(context.Context, string, []string) error { return nil }, broker.ObservationPolicy{SettingsDigest: "settings-digest", Telemetry: broker.Telemetry{Registry: telemetry.NewRegistry()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, needed, err := repo.HeadToDeploy(ctx); err != nil || needed {
		t.Fatalf("adopted head: %v %v", needed, err)
	}
	second := commit("2")
	result, err := lifecycle.HeadDeployer{Broker: repo, Deployer: lifecycle.RepairDeployer{Readiness: interruptedReadiness{}, Verifier: unusedVerifier{}}}.Deploy(ctx,
		lifecycle.RepairConfig{Namespace: "shop", HealthObjective: "health", InfrastructureTries: 1})
	if err != nil || result.Outcome != lifecycle.HeadHeld {
		t.Fatalf("head deployment = %+v, %v", result, err)
	}

	renderers := render.NewCatalog()
	if err := raw.Register(renderers); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "shop", Name: "settings"}
	wired := false
	var out syncBuffer
	runErr := RunWithOptions(ctx, nil, RuntimeOptions{
		Args: []string{"--namespace", "shop", "--control-namespace", "sdo", "--app-root", accepted, "--application", "shop",
			"--source-commit", first, "--repository-mount-path", t.TempDir()},
		Stdout: &out, Stderr: &out, Client: fake.NewSimpleClientset(), Dispatcher: noDispatch{}, Renderers: renderers,
		AcceptedScope: harnessScopeForTest, InformerSync: fastSyncPolicy(), InformerResync: 10 * time.Minute,
		NativeBroker: &NativeIncidentBroker{Repository: repo, LedgerRoot: filepath.Join(root, "ledger"), SessionRoot: filepath.Join(root, "sessions")},
		Wired: func(c *Controller) {
			defer cancel()
			wired = true
			if got := c.CurrentRenderedRevision(); got != second {
				t.Errorf("rendered revision = %s, want the held head %s", got, second)
			}
			source, ok := c.drift.Source.(GitPlanSource)
			if !ok {
				t.Errorf("drift source is %T", c.drift.Source)
				return
			}
			if _, err := source.AcceptedManifest(ctx, c.CurrentRenderedRevision(), ref); !errors.Is(err, rendered.ErrTargetInterrupted) {
				t.Errorf("a restore from the interrupted phase = %v, want a refusal", err)
			}
			if lines := c.heldTargetLines(); len(lines) != 2 {
				t.Errorf("page = %q", lines)
			}
		},
	})
	if !wired {
		t.Fatalf("the controller was never wired: %v\n%s", runErr, out.String())
	}
}
