//go:build integration

package runtime

import (
	"context"
	"k8s.io/client-go/kubernetes/fake"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/worker_registry"
	"strings"
	"testing"
	"time"
)

func TestNativeReflectionTrustedCheckRequiresImmutableInputs(t *testing.T) {
	for _, test := range []struct {
		name           string
		immutable, dev bool
	}{
		{"mutable image tags", false, false}, {"immutable images", true, false}, {"mutable development source", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("SDO_DEV_MOUNT", "0")
			if test.dev {
				t.Setenv("SDO_DEV_MOUNT", "1")
			}
			root := nativeRepository(t)
			image := "example.invalid/validator:mutable"
			compiler := "example.invalid/compiler:mutable"
			if test.immutable {
				image = "example.invalid/validator@sha256:" + strings.Repeat("a", 64)
				compiler = "example.invalid/compiler@sha256:" + strings.Repeat("b", 64)
			}
			config := &contracts.ControllerConfig{ToolchainImage: compiler, ValidatorImage: image, SchemaIdentity: "synthetic-schema", BrokerMode: "native", WorkerMountMode: "proposal-only", AppRoot: root, Namespace: "app", BrokerWorktreeRoot: t.TempDir(), RepositoryMountPath: filepath.Dir(root), RepositoryPvc: "repo", ResponderBackend: "codex", ResponderModel: "synthetic", ReflectionSession: "resume"}
			client := fake.NewSimpleClientset()
			native, _, err := nativeBrokerFromLaunch(context.Background(), config, client)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := worker_registry.New(builtInReflectorRegistration("agent:test"))
			if err != nil {
				t.Fatal(err)
			}
			if err := attachNativeReflector(native, config, client, registry, time.Minute); err != nil {
				t.Fatal(err)
			}
			executor, err := native.Reflector.(RegisteredReflectionWorker).SessionExecutor("synthetic-incident")
			if err != nil {
				t.Fatal(err)
			}
			wrapped, enabled := executor.(reflectionCheckExecutor)
			want := test.immutable && !test.dev
			if enabled != want {
				t.Fatalf("trusted endpoint=%v want=%v", enabled, want)
			}
			if enabled {
				worker := wrapped.Executor.(worker_registry.KubernetesExecutor)
				if worker.Environment["SDO_MEMORY_CHECK_DIR"] != "/sdo/session/tools" || worker.Environment["SDO_TOOLS_DIR"] != "/sdo/session/tools" {
					t.Fatal("missing protected check endpoint environment")
				}
			}
		})
	}
}

