package builder

import (
<<<<<<< HEAD
=======
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
>>>>>>> origin/vic-lsh/dev/sdo-improve
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestControllerLaunchPreservesTypedConfigAndInjectsBuildPaths(t *testing.T) {
<<<<<<< HEAD
	original := &contractsv1alpha1.ControllerConfig{Namespace: "app", ResponderAgentId: "custom-worker", DispatcherMode: "job", RepairPolicy: "commit", BrokerMode: "native", WorkerMountMode: "proposal-only", ResponderBackend: "codex", ResponderModel: "model"}
	data, err := controllerConfigJSON(RunConfig{Build: Config{ApplicationRoot: "/accepted/repo"}, Controller: original}, provenance{Fingerprint: "tree", ArtifactDigest: "sha256:artifact", Revision: "head"})
=======
	original := &contractsv1alpha1.ControllerConfig{Namespace: "app", SourceCommit: "old-source", DeployedCommit: "deployed", ResponderAgentId: "custom-worker", DispatcherMode: "job", RepairPolicy: "commit", BrokerMode: "native", WorkerMountMode: "proposal-only", ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model"}
	data, err := controllerConfigJSON(RunConfig{Build: Config{ApplicationRoot: "/accepted/repo"}, Controller: original}, "/shared/.sdo-prober/digest/sdo-prober", provenance{Fingerprint: "tree", ArtifactDigest: "sha256:artifact", Revision: "head"})
>>>>>>> origin/vic-lsh/dev/sdo-improve
	if err != nil {
		t.Fatal(err)
	}
	var decoded contractsv1alpha1.ControllerConfig
	if err := protojson.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
<<<<<<< HEAD
	if decoded.AppRoot != "/accepted/repo" || decoded.ResponderAgentId != "custom-worker" || decoded.DiagnosticsFingerprint != "tree" || decoded.ControllerArtifactDigest != "sha256:artifact" || decoded.ControllerRevision != "head" || decoded.BrokerMode != "native" || decoded.WorkerMountMode != "proposal-only" || decoded.ResponderModel != "model" {
=======
	if decoded.AppRoot != "/accepted/repo" || decoded.ProberBinary != "/shared/.sdo-prober/digest/sdo-prober" || decoded.ResponderAgentId != "custom-worker" || decoded.DiagnosticsFingerprint != "tree" || decoded.ControllerArtifactDigest != "sha256:artifact" || decoded.ControllerRevision != "head" || decoded.SourceCommit != "head" || decoded.DeployedCommit != "deployed" || decoded.BrokerMode != "native" || decoded.WorkerMountMode != "proposal-only" || decoded.ValidatorImage != "validator:test" || decoded.ResponderModel != "model" {
>>>>>>> origin/vic-lsh/dev/sdo-improve
		t.Fatalf("launch contract lost fields: %s", decoded.String())
	}
	if original.AppRoot != "" {
		t.Fatal("launch mutated caller's config")
	}
}

func TestControllerConfigHasNoMemoryExecutionSwitch(t *testing.T) {
	fields := (&contractsv1alpha1.ControllerConfig{}).ProtoReflect().Descriptor().Fields()
	for _, name := range []protoreflect.Name{"memory_host", "restart_after_closure", "prober_binary", "prober_image", "prober_url"} {
		if fields.ByName(name) != nil {
			t.Errorf("obsolete execution switch %s remains", name)
		}
	}
}
<<<<<<< HEAD
=======

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
>>>>>>> origin/vic-lsh/dev/sdo-improve
