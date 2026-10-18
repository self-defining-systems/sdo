package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
