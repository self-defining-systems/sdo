package builder

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/internal/testutil"
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

// The generated registration imports each accepted detector package; the
// compiler then proves the import resolves (nightly real-compile set).
func TestGeneratedDetectorsImportAcceptedDetectors(t *testing.T) {
	workspace := stagedMemoryHost(t)
	generated, err := os.ReadFile(filepath.Join(workspace.Directory, "generated", "detectors.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(generated, []byte(`d0 "fixture-diagnostics/detectors/incidents/missing-configmap"`)) {
		t.Fatalf("generated detector import missing: %s", generated)
	}
}

// The accepted host is linked only after the staged memory's own tests pass,
// and an adopted (unvalidated) build never runs them.
func TestBuildMemoryHostStagePlan(t *testing.T) {
	for _, test := range []struct {
		name        string
		compileOnly bool
	}{{"validated", false}, {"adopted compile-only", true}} {
		t.Run(test.name, func(t *testing.T) {
			log := testutil.FakeGo(t)
			workspace := stagedMemoryHost(t)
			workspace.compileOnly = test.compileOnly
			artifact, err := workspace.BuildMemoryHost(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if digest, err := hostio.BinaryDigest(artifact.Path); err != nil || digest != artifact.Digest {
				t.Fatalf("artifact digest %s does not describe %s: %v", artifact.Digest, artifact.Path, err)
			}
			tested, linked := -1, -1
			for i, stage := range testutil.GoStages(t, log) {
				switch {
				case strings.HasPrefix(stage, "test ") && strings.Contains(stage, "./..."):
					tested = i
				case strings.HasPrefix(stage, "build ") && strings.Contains(stage, "./cmd/memory-host"):
					linked = i
				}
			}
			if linked < 0 || (!test.compileOnly && tested < 0) || (!test.compileOnly && tested > linked) || (test.compileOnly && tested >= 0) {
				t.Fatalf("test stage %d, link stage %d (compile-only=%v)", tested, linked, test.compileOnly)
			}
		})
	}
}

func TestBuildMemoryHostUsesStagedMemory(t *testing.T) {
	workspace := stagedMemoryHost(t)
	if err := workspace.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Check links no artifact; BuildMemoryHost produces the accepted host.
	for _, name := range []string{"sdo-memory-host", "sdo-controller"} {
		if _, err := os.Stat(filepath.Join(workspace.Directory, name)); !os.IsNotExist(err) {
			t.Fatalf("validation linked %s: %v", name, err)
		}
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
