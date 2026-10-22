package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
)

func TestNativeLaunchRequiresProposalOnlyMountAndBuildsBroker(t *testing.T) {
	root := nativeRepository(t)
	config := &contracts.ControllerConfig{
		BrokerMode: "native", WorkerMountMode: "linked", AppRoot: root, Namespace: "default",
		BrokerWorktreeRoot: filepath.Join(t.TempDir(), "workspaces"), RepositoryMountPath: filepath.Dir(root), RepositoryPvc: "repository",
		ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model",
	}
	if _, _, err := nativeBrokerFromLaunch(context.Background(), config, fake.NewSimpleClientset()); err == nil {
		t.Fatal("linked worker mount accepted for native broker")
	}
	config.WorkerMountMode = "proposal-only"
	native, _, err := nativeBrokerFromLaunch(context.Background(), config, fake.NewSimpleClientset())
	if err != nil {
		t.Fatal(err)
	}
	if native.Repository == nil || native.Deriver == nil || native.ResponderBackend != "codex" {
		t.Fatalf("incomplete native broker: %+v", native)
	}
}

func TestNativeReflectionSharesIncidentSessionWithResponder(t *testing.T) {
	root := t.TempDir()
	registry, err := worker_registry.New(builtInReflectorRegistration("agent:test"))
	if err != nil {
		t.Fatal(err)
	}
	config := &contracts.ControllerConfig{AppRoot: root, RepositoryMountPath: root, RepositoryPvc: "repo", ReflectionSession: "resume"}
	native := &NativeIncidentBroker{LedgerRoot: filepath.Join(root, "ledger")}
	if err := attachNativeReflector(native, config, fake.NewSimpleClientset(), registry, time.Minute); err != nil {
		t.Fatal(err)
	}
	worker := native.Reflector.(RegisteredReflectionWorker)
	executor, err := worker.SessionExecutor("incident-1")
	if err != nil {
		t.Fatal(err)
	}
	resumed := executor.(worker_registry.KubernetesExecutor)
	bound, err := memory.PrepareIncidentSession(root, "", "incident-1")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.SessionPVCSubPath != bound || resumed.Environment["CODEX_HOME"] != "/sdo/session/codex" || resumed.Environment["CLAUDE_CONFIG_DIR"] != "/sdo/session/claude" {
		t.Fatalf("reflection did not resume incident mount: %+v", resumed)
	}
	freshExecutor, err := worker.FreshSessionExecutor("incident-1")
	if err != nil {
		t.Fatal(err)
	}
	fresh := freshExecutor.(worker_registry.KubernetesExecutor)
	if fresh.SessionPVCSubPath != bound || fresh.Environment["SDO_TURN_USAGE_LOG"] != "/sdo/session/reflection-usage.jsonl" {
		t.Fatalf("fresh reflection lost incident usage mount: %+v", fresh)
	}
}
