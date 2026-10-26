package builder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"testing"
)

func TestShellBuilderLaunchesTypedControllerWithoutGoProjection(t *testing.T) {
	directory := t.TempDir()
	capture := filepath.Join(directory, "captured.json")
	launcher := filepath.Join(directory, "image-controller")
	if err := os.WriteFile(launcher, []byte("#!/bin/sh\ntest \"$1\" = --config || exit 2\ncp \"$2\" \"$SDO_CONFIG_CAPTURE\"\n"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDO_CONFIG_CAPTURE", capture)
	prepared := false
	cfg := RunConfig{Controller: &contracts.ControllerConfig{AppRoot: directory, CodePath: &contracts.CodePathConfig{Name: "shell"}}, ShellControllerBinary: launcher,
		PrepareShell: func(context.Context) error { prepared = true; return nil },
		ResolveDiagnostics: func(context.Context, string) (DiagnosticsProjection, error) {
			t.Fatal("shell launcher compiled Go memory")
			return DiagnosticsProjection{}, nil
		}}
	if err := RunOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if !prepared {
		t.Fatal("shell health preparation skipped")
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	got := &contracts.ControllerConfig{}
	if err := wire.Unmarshal(data, got); err != nil {
		t.Fatal(err)
	}
	if got.GetCodePath().GetName() != "shell" || got.GetAppRoot() != directory {
		t.Fatal(got)
	}
	failure := errors.New("health preparation failed")
	cfg.PrepareShell = func(context.Context) error { return failure }
	if err := RunOnce(context.Background(), cfg); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
