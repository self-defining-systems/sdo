//go:build integration

package builder

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sdo.dev/operator/memoryhost/hostio"
)

func stagedMemoryHost(t *testing.T) *Workspace {
	t.Helper()
	repository := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workspace.Close() })
	main, err := os.ReadFile(filepath.Join(workspace.Directory, "cmd/controller/main.go"))
	if err != nil || bytes.Contains(main, []byte("generated")) {
		t.Fatal("host controller imports accepted memory", string(main), err)
	}
	return workspace
}

// Staging and host/controller isolation are cheap PR contracts; the real
// compiler/digest round trip remains in the nightly integration tier.
func TestMemoryHostWorkspaceStagesAcceptedMemory(t *testing.T) {
	workspace := stagedMemoryHost(t)
	if err := workspace.prepareMemoryHost(); err != nil {
		t.Fatal(err)
	}
	main, err := os.ReadFile(filepath.Join(workspace.Directory, "cmd/memory-host/main.go"))
	if err != nil || !bytes.Contains(main, []byte(workspace.ModulePath+"/generated")) {
		t.Fatalf("host does not import staged memory: %s %v", main, err)
	}
	source, err := os.ReadFile(filepath.Join(workspace.Directory, "detectors/incidents/missing-configmap/detector.go"))
	if err != nil || len(source) == 0 {
		t.Fatalf("accepted detector not staged: %v", err)
	}
}

func TestBuildMemoryHostUsesStagedMemory(t *testing.T) {
	workspace := stagedMemoryHost(t)
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "sdo-memory-host")); err != nil {
		t.Fatal("validation did not build host", err)
	}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "sdo-controller")); !os.IsNotExist(err) {
		t.Fatal("validation built a controller", err)
	}
	artifact, err := workspace.BuildMemoryHost(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hostio.BinaryDigest(artifact.Path)
	if err != nil || digest != artifact.Digest {
		t.Fatal(artifact, err)
	}
	// Bounded evaluation also launches outside a pod using kubeconfig.
	kubeconfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: http://127.0.0.1:1
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
users:
- name: fake
  user: {}
`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	var output bytes.Buffer
	child, err := hostio.Launch(hostio.LaunchConfig{Binary: artifact.Path, Digest: artifact.Digest, Commit: "accepted", Namespace: "toy", MemoryBytes: 512 << 20, Output: &output})
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ready, err := child.Ready(ctx)
	if err != nil {
		_ = child.Close()
		t.Fatalf("kubeconfig host startup: %v: %s", err, output.String())
	}
	if ready.MemoryCommit != "accepted" || ready.BinaryDigest != artifact.Digest {
		t.Fatal("host identity differs from accepted artifact", ready)
	}

}
