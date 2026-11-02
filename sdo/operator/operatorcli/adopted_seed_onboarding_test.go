package operatorcli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/onboarding"
)

func TestInstallExistingAdoptedVerifiedSeedOnboarding(t *testing.T) {
	testutil.FakeGo(t)
	ctx := context.Background()
	root := t.TempDir()
	// Hand-written learned incident artifacts, plus an inherited verified health
	// detector. No live repository, cluster, model backend, or oracle is used.
	if err := os.CopyFS(root, os.DirFS("../../../tests/fixtures/sdo/playbook_program_app")); err != nil {
		t.Fatal(err)
	}
	write := func(name, data string) {
		t.Helper()
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("app.yaml", "apiVersion: v1\nkind: Service\nmetadata:\n  name: query\n")
	if err := memory.WriteGoal(root, "app", "Keep intended application healthy."); err != nil {
		t.Fatal(err)
	}
	write(".sdo/diagnostics/detectors/health/seed-health/detector.go", `package seedhealth
import("time";"sdo.dev/operator/sdk";"sdo.dev/operator/sdk/workloadhealth")
func New() sdk.Detector {return workloadhealth.NewDetector(sdk.DetectorSpec{ID:"seed-health",Class:sdk.DetectorClassHealth,Owner:sdk.DetectorOwnerHealthJudge,Watches:workloadhealth.Watches(),Interval:time.Second,Persistence:sdk.PersistencePolicy{Firing:1,Clearing:1},Batching:sdk.BatchingPolicy{Severity:sdk.SeverityCritical,Debounce:time.Millisecond},Playbooks:[]string{"seed-health"},OriginatingCommit:"fixture-judge"})}
`)
	write(".sdo/diagnostics/detectors/health/seed-health/manifest.yaml", `apiVersion: sdo.dev/v1alpha1
kind: DetectorManifest
sdkVersion: v0.1
id: seed-health
constructor: New
class: health
owner: health_judge
watches:
 - {apiVersion: v1, kind: Pod}
 - {apiVersion: apps/v1, kind: Deployment}
 - {apiVersion: apps/v1, kind: ReplicaSet}
 - {apiVersion: v1, kind: Event}
interval: 1s
persistence: {firing: 1, clearing: 1}
batching: {severity: critical, debounce: 1ms}
possiblePlaybooks: [seed-health]
originatingCommit: fixture-judge
`)
	write(".sdo/playbooks/seed-health/README.md", "---\nschema_version: 1\nowner: health_judge\nfault_class: health\noriginating_incident: initial\n---\nRestore intended workload availability.\n")
	gitIn(t, root, "init", "-q")
	gitIn(t, root, "config", "user.name", "Test")
	gitIn(t, root, "config", "user.email", "test@example.invalid")
	gitIn(t, root, "add", ".")
	gitIn(t, root, "commit", "-qm", "verified inherited judge\n\nSDO-Actor: health_judge\nSDO-Phase: initial\nSDO-Validation: passed")
	verified := strings.TrimSpace(gitIn(t, root, "rev-parse", "HEAD"))
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	launch := InstallExistingConfig{Repository: root, OperatorRoot: operator, AdoptExistingMemory: true, Install: install.Config{Namespace: "app", ControlNamespace: "control", ExtraControllerArgs: []string{"--deployed-manifest-path=app.yaml"}}}
	commit, err := prepareInstallExistingRepository(ctx, launch)
	if err != nil {
		t.Fatal(err)
	}
	if changed := gitIn(t, root, "diff", "--name-only", verified, commit, "--", ".sdo/diagnostics", ".sdo/playbooks", ".sdo/lib"); strings.TrimSpace(changed) != "" {
		t.Fatalf("adoption rewrote executable memory: %s", changed)
	}
	revision, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil || revision.NeedsRefresh || revision.JudgeCommit != verified {
		t.Fatalf("inherited provenance lost: %+v %v", revision, err)
	}
	// The production refresh entry point returns before opening credentials or
	// creating workers. A nil broker would fail immediately if refresh ran.
	if err := refreshControllerHealthOnce(ctx, &contracts.ControllerConfig{AppRoot: root}, nil); err != nil {
		t.Fatalf("verified seed incurred refresh: %v", err)
	}
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "installer-script", Namespace: "app"}}, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "query", Namespace: "app"}})
	config := launch.Install
	config.AcceptedCommit, config.Application, config.Model = commit, "app", "fixture-no-model"
	config.TimeoutSeconds = 60
	config.NativeCommand = []string{"/usr/local/bin/sdo-operator"}
	config.BuildKitImage = "moby/buildkit@sha256:" + strings.Repeat("a", 64)
	config.RegistryPushSecret = "registry-push"
	config.RegistryPushDockerConfig = []byte("{}")
	config.ControllerImage, config.ResponderImage = "controller:test", "responder:test"
	config.RepositoryPVC, config.CredentialsSecret = "accepted", "worker-credentials"
	installer := install.Installer{Client: client, BeforeControllerStart: existingNamespaceOnboardingHook(client, launch, commit)}
	if _, err := installer.InstallPrepared(ctx, config, root); err != nil {
		t.Fatal(err)
	}
	pending, err := onboarding.Pending(ctx, client, "control", "app")
	if err != nil || pending == nil {
		t.Fatalf("no pending install: %v %v", pending, err)
	}
	if pending.AcceptedCommit != commit || len(pending.Objects) != 1 || pending.Objects[0].Name != "installer-script" {
		t.Fatalf("pending onboarding does not bind the adopted commit and installed object: %+v", pending)
	}

	jobs, err := client.BatchV1().Jobs("control").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 1 || jobs.Items[0].Name != install.ControllerJobName {
		t.Fatalf("unexpected worker jobs: %v %v", jobs, err)
	}
}
