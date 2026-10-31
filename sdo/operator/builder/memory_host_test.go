//go:build integration

package builder

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memoryhost/hostio"
)

func TestBuildMemoryHostUsesStagedMemory(t *testing.T) {
	repository := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "missing_configmap_app")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := BuildWorkspace(context.Background(), Config{ApplicationRoot: repository, DiagnosticsRoot: filepath.Join(repository, ".sdo", "diagnostics"), MemoryHost: true, OperatorRoot: operatorRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	main, err := os.ReadFile(filepath.Join(workspace.Directory, "cmd/controller/main.go"))
	if err != nil || bytes.Contains(main, []byte("generated")) {
		t.Fatal("host controller imports accepted memory", string(main), err)
	}
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
}
