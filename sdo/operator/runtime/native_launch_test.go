package runtime

import (
	"context"
	"path/filepath"
	"strings"
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
		ToolchainImage: "controller:compiler", BrokerMode: "native", WorkerMountMode: "linked", AppRoot: root, Namespace: "default",
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
	if deriver, ok := native.Deriver.(ProtoOutcomeDeriver); !ok || deriver.SessionRoot != config.GetRepositoryMountPath() {
		t.Fatal("production outcome derivation must retain the protected incident API history")
	}
}

func TestNativeReflectionSharesIncidentSessionWithResponder(t *testing.T) {
	root := t.TempDir()
	registry, err := worker_registry.New(builtInReflectorRegistration("agent:test"))
	if err != nil {
		t.Fatal(err)
	}
	config := &contracts.ControllerConfig{ToolchainImage: "controller:compiler", ProberImage: "traffic-only:test", AppRoot: root, RepositoryMountPath: root, RepositoryPvc: "repo", ReflectionSession: "resume"}
	native := &NativeIncidentBroker{LedgerRoot: filepath.Join(root, "ledger")}
	if err := attachNativeReflector(native, config, fake.NewSimpleClientset(), registry, time.Minute); err != nil {
		t.Fatal(err)
	}
	worker := native.Reflector.(RegisteredReflectionWorker)
	if worker.Executor.(worker_registry.KubernetesExecutor).ToolchainImage != "controller:compiler" {
		t.Fatal("reflection compiler depends on traffic prober image")
	}

	executor, err := worker.SessionExecutor("incident-1")
	if err != nil {
		t.Fatal(err)
	}
	resumed := executor.(worker_registry.KubernetesExecutor)
	if resumed.ToolchainImage != "controller:compiler" {
		t.Fatal("resumed reflection lost compiler image")
	}
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
	if fresh.ToolchainImage != "controller:compiler" {
		t.Fatal("fresh reflection lost compiler image")
	}
	if fresh.SessionPVCSubPath != bound || fresh.Environment["SDO_TURN_USAGE_LOG"] != "/sdo/session/reflection-usage.jsonl" {
		t.Fatalf("fresh reflection lost incident usage mount: %+v", fresh)
	}
}

func TestAgentWorkerEnvironmentCarriesConfiguredProviderAndModel(t *testing.T) {
	environment, err := AgentWorkerEnvironment(&contracts.ControllerConfig{ResponderBackend: "claude", ResponderModel: "claude-haiku-5-5"})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"SDO_AGENT_PROVIDER": "claude", "SDO_RESPONDER_MODEL": "claude-haiku-5-5", "SDO_REFLECTION_MODEL": "claude-haiku-5-5",
		"SDO_LIFECYCLE_MODEL": "claude-haiku-5-5", "SDO_DEPLOYMENT_MODEL": "claude-haiku-5-5",
	} {
		if environment[name] != want {
			t.Errorf("%s = %q, want %q", name, environment[name], want)
		}
	}
}

func TestAgentWorkerEnvironmentRejectsUnknownProvider(t *testing.T) {
	if _, err := AgentWorkerEnvironment(&contracts.ControllerConfig{ResponderBackend: "gemini", ResponderModel: "m"}); err == nil {
		t.Fatal("unknown provider accepted")
	}
	if environment, err := AgentWorkerEnvironment(&contracts.ControllerConfig{}); err != nil || len(environment) != 0 {
		t.Fatalf("unconfigured provider must add nothing: %v %v", environment, err)
	}
}

func TestNativeLaunchRejectsUnknownProvider(t *testing.T) {
	root := nativeRepository(t)
	config := &contracts.ControllerConfig{
		ToolchainImage: "controller:compiler", BrokerMode: "native", WorkerMountMode: "proposal-only", AppRoot: root, Namespace: "default",
		BrokerWorktreeRoot: filepath.Join(t.TempDir(), "workspaces"), RepositoryMountPath: filepath.Dir(root), RepositoryPvc: "repository",
		ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "gemini", ResponderModel: "model",
	}
	if _, _, err := nativeBrokerFromLaunch(context.Background(), config, fake.NewSimpleClientset()); err == nil {
		t.Fatal("unknown provider accepted by native launch")
	}
}

func TestNativeReflectionJobsCarryConfiguredProviderAndModel(t *testing.T) {
	root := t.TempDir()
	registry, err := worker_registry.New(builtInReflectorRegistration("agent:test"))
	if err != nil {
		t.Fatal(err)
	}
	config := &contracts.ControllerConfig{ToolchainImage: "controller:compiler", ProberImage: "traffic-only:test", AppRoot: root, RepositoryMountPath: root, RepositoryPvc: "repo", ReflectionSession: "resume", ResponderBackend: "claude", ResponderModel: "claude-haiku-5-5"}
	native := &NativeIncidentBroker{LedgerRoot: filepath.Join(root, "ledger")}
	if err := attachNativeReflector(native, config, fake.NewSimpleClientset(), registry, time.Minute); err != nil {
		t.Fatal(err)
	}
	worker := native.Reflector.(RegisteredReflectionWorker)
	if worker.Executor.(worker_registry.KubernetesExecutor).ToolchainImage != "controller:compiler" {
		t.Fatal("reflection compiler depends on traffic prober image")
	}

	environments := []map[string]string{worker.Executor.(worker_registry.KubernetesExecutor).Environment}
	for _, factory := range []func(string) (ReflectionTaskExecutor, error){worker.SessionExecutor, worker.FreshSessionExecutor} {
		executor, err := factory("incident-1")
		if err != nil {
			t.Fatal(err)
		}
		environments = append(environments, executor.(worker_registry.KubernetesExecutor).Environment)
	}
	for i, environment := range environments {
		if environment["SDO_AGENT_PROVIDER"] != "claude" || environment["SDO_REFLECTION_MODEL"] != "claude-haiku-5-5" {
			t.Errorf("reflection executor %d lost provider/model: %v", i, environment)
		}
	}
}

// Every reflection executor (fresh Job, resumed session, fresh-in-session) must
// carry the config's storage and credential routes, not only provider and model.
func TestNativeReflectionExecutorsCarryConfiguredStorageAndCredentials(t *testing.T) {
	root := t.TempDir()
	registry, err := worker_registry.New(builtInReflectorRegistration("agent:test"))
	if err != nil {
		t.Fatal(err)
	}
	config := &contracts.ControllerConfig{
		AppRoot: root, RepositoryMountPath: root, RepositoryPvc: "repo", RepositoryPvcSubpath: "sub", ReflectionSession: "resume",
		ResponderCredentialsSecret: "creds", HealthyBaseline: true,
	}
	native := &NativeIncidentBroker{LedgerRoot: filepath.Join(root, "ledger")}
	if err := attachNativeReflector(native, config, fake.NewSimpleClientset(), registry, time.Minute); err != nil {
		t.Fatal(err)
	}
	worker := native.Reflector.(RegisteredReflectionWorker)
	executors := map[string]worker_registry.KubernetesExecutor{"fresh": worker.Executor.(worker_registry.KubernetesExecutor)}
	for name, factory := range map[string]func(string) (ReflectionTaskExecutor, error){"resume": worker.SessionExecutor, "fresh-in-session": worker.FreshSessionExecutor} {
		executor, err := factory("incident-1")
		if err != nil {
			t.Fatal(err)
		}
		executors[name] = executor.(worker_registry.KubernetesExecutor)
	}
	for name, executor := range executors {
		if executor.RepositoryPVC != "repo" || executor.RepositoryMountPath != root || executor.RepositoryPVCSubPath != "sub" ||
			executor.CredentialsSecret != "creds" || !executor.HealthyBaseline {
			t.Errorf("%s executor lost config routes: %+v", name, executor)
		}
	}
}

func TestWithAgentEnvironmentOverridesResponderEnv(t *testing.T) {
	merged, err := withAgentEnvironment(map[string]string{"SDO_AGENT_PROVIDER": "codex", "KEEP": "1"}, &contracts.ControllerConfig{ResponderBackend: "claude", ResponderModel: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if merged["SDO_AGENT_PROVIDER"] != "claude" || merged["SDO_RESPONDER_MODEL"] != "m" || merged["KEEP"] != "1" {
		t.Fatalf("launch config must be authoritative: %v", merged)
	}
	if untouched, err := withAgentEnvironment(map[string]string{"A": "b"}, nil); err != nil || untouched["A"] != "b" {
		t.Fatalf("no launch config must leave env: %v %v", untouched, err)
	}
}

func TestNativeStartupRejectsMissingToolchainBeforeKubernetes(t *testing.T) {
	config := &contracts.ControllerConfig{
		BrokerMode: "native", WorkerMountMode: "proposal-only", AppRoot: "/workspace/app", Namespace: "default",
		BrokerWorktreeRoot: "/workspace/worktrees", RepositoryMountPath: "/workspace", RepositoryPvc: "repository",
		ValidatorImage: "validator:slim", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model",
		ProberImage: "traffic-only:test",
	}
	client := fake.NewSimpleClientset()
	if _, _, err := nativeBrokerFromLaunch(context.Background(), config, client); err == nil || !strings.Contains(err.Error(), "--toolchain-image") {
		t.Fatalf("missing compiler should fail at startup: %v", err)
	}
	if len(client.Actions()) != 0 {
		t.Fatal("missing compiler reached Kubernetes")
	}
}
