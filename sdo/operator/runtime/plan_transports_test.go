package runtime

import (
	"context"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func TestTypedLaunchRetainsRemovalTransports(t *testing.T) {
	// No connection is made: native launch and transport construction are local.
	// Use a deliberately unreachable local API so accidental network reads fail.
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	kubeconfig := filepath.Join(t.TempDir(), "config")
	raw := `apiVersion: v1
kind: Config
clusters:
- name: synthetic
  cluster:
    server: http://127.0.0.1:1
contexts:
- name: synthetic
  context:
    cluster: synthetic
    user: synthetic
current-context: synthetic
users:
- name: synthetic
  user: {}
`
	if err := os.WriteFile(kubeconfig, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)
	root := nativeRepository(t)
	config := &contracts.ControllerConfig{ToolchainImage: "controller:compiler", BrokerMode: "native", WorkerMountMode: "proposal-only", AppRoot: root, Namespace: "app", ControlNamespace: "control", BrokerWorktreeRoot: filepath.Join(t.TempDir(), "workspaces"), RepositoryMountPath: filepath.Dir(root), RepositoryPvc: "repository", ValidatorImage: "validator:test", SchemaIdentity: "schema", ResponderBackend: "codex", ResponderModel: "model"}
	_, client, err := nativeBrokerFromLaunch(context.Background(), config, nil)
	if err != nil {
		t.Fatal(err)
	}
	options := RuntimeOptions{Client: client}
	exec, capture, err := initializePlanTransports(options, false, "app")
	if err != nil {
		t.Fatal(err)
	}
	if exec == nil || capture == nil {
		t.Fatal("typed production launch lost exec/capture transports; guarded removals silently unavailable")
	}
	removal, err := newUnexplainedRemovalClient(exec, capture, "control")
	if err != nil || removal == nil {
		t.Fatalf("typed production launch lost removal transport: %v", err)
	}
	if exec.Impersonate.UserName != "" {
		t.Fatal("removal identity leaked into generic transport")
	}
	exec, capture, err = initializePlanTransports(options, true, "app")
	if err != nil || exec != nil || capture != nil {
		t.Fatalf("caller-injected client attempted ambient credentials: %v", err)
	}
}
