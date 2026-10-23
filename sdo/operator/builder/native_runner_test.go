package builder

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"
	"time"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestControllerLaunchPreservesTypedConfigAndInjectsBuildPaths(t *testing.T) {
	original := &contractsv1alpha1.ControllerConfig{Namespace: "app", SourceCommit: "old-source", DeployedCommit: "deployed", ResponderAgentId: "custom-worker", DispatcherMode: "job", RepairPolicy: "commit", BrokerMode: "native", WorkerMountMode: "proposal-only", ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model"}
	data, err := controllerConfigJSON(RunConfig{Build: Config{ApplicationRoot: "/accepted/repo"}, Controller: original}, "/shared/.sdo-prober/digest/sdo-prober", provenance{Fingerprint: "tree", ArtifactDigest: "sha256:artifact", Revision: "head"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded contractsv1alpha1.ControllerConfig
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AppRoot != "/accepted/repo" || decoded.ProberBinary != "/shared/.sdo-prober/digest/sdo-prober" || decoded.ResponderAgentId != "custom-worker" || decoded.DiagnosticsFingerprint != "tree" || decoded.ControllerArtifactDigest != "sha256:artifact" || decoded.ControllerRevision != "head" || decoded.SourceCommit != "head" || decoded.DeployedCommit != "deployed" || decoded.BrokerMode != "native" || decoded.WorkerMountMode != "proposal-only" || decoded.ValidatorImage != "validator:test" || decoded.ResponderModel != "model" {
		t.Fatalf("launch contract lost fields: %s", decoded.String())
	}
	if original.AppRoot != "" || original.ProberBinary != "" {
		t.Fatal("launch mutated caller's config")
	}
}

func TestSuperviseRejectsBoundedControllerModes(t *testing.T) {
	for _, config := range []*contractsv1alpha1.ControllerConfig{{ExitAfterClosure: true}, {Duration: durationpb.New(1)}} {
		if err := Supervise(context.Background(), RunConfig{Controller: config}); err == nil {
			t.Fatal("bounded controller mode was accepted for supervision")
		}
	}
}

func TestSuperviseAnnouncesEachRelaunch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runs := 0
	run := func(context.Context, RunConfig) error {
		runs++
		if runs == 3 {
			cancel()
		}
		return nil
	}
	var out bytes.Buffer
	err := supervise(ctx, RunConfig{Controller: &contractsv1alpha1.ControllerConfig{}}, run, &out)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("supervise error = %v, want context cancellation", err)
	}
	want := "{\"controller_supervisor\":\"relaunch\",\"launches\":1}\n{\"controller_supervisor\":\"relaunch\",\"launches\":2}\n"
	if out.String() != want {
		t.Fatalf("supervisor output = %q, want %q", out.String(), want)
	}
}

func TestHealthRefreshRelaunchesBoundedControllerAndPreservesFailures(t *testing.T) {
	restart := exec.Command("sh", "-c", "exit 76").Run()
	failure := errors.New("judge publication failed")
	runs := 0
	original := &contractsv1alpha1.ControllerConfig{Duration: durationpb.New(time.Minute), ExitAfterClosure: true}
	err := runWithHealthRefresh(context.Background(), RunConfig{Controller: original}, func(_ context.Context, cfg RunConfig) error {
		runs++
		if !cfg.Controller.ExitAfterClosure || cfg.Controller.GetDuration().AsDuration() <= 0 || cfg.Controller.GetDuration().AsDuration() > time.Minute {
			t.Fatalf("bounded launch lost contract: %v", cfg.Controller)
		}
		if runs == 1 {
			return fmt.Errorf("child requires rebuild: %w", restart)
		}
		return failure
	})
	if runs != 2 || !errors.Is(err, failure) {
		t.Fatalf("refresh relaunch: runs=%d err=%v", runs, err)
	}
	if original.GetDuration().AsDuration() != time.Minute {
		t.Fatal("mutated launch contract")
	}
	runs = 0
	ordinary := exec.Command("sh", "-c", "exit 1").Run()
	if err := runWithHealthRefresh(context.Background(), RunConfig{}, func(context.Context, RunConfig) error { runs++; return ordinary }); err == nil || runs != 1 {
		t.Fatalf("ordinary failure relaunched: runs=%d err=%v", runs, err)
	}
}
