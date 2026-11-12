package operatorcli

import (
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/contracts/settings"
)

// testModelConfig is a --config file that sets only the required model, and the package's mounted configuration is
// the defaults with that model: the operate and controller parsers read it as the installed Jobs read sdo-config.
var testModelConfig string

func init() {
	directory, err := os.MkdirTemp("", "sdo-operatorcli-settings-")
	if err != nil {
		panic(err)
	}
	testModelConfig = filepath.Join(directory, "model.yaml")
	if err := os.WriteFile(testModelConfig, []byte("agent:\n  model: m\n"), 0o600); err != nil {
		panic(err)
	}
	mountedSettings = writeTestSettings(directory, nil)
}

func writeTestSettings(directory string, mutate func(*settings.Config)) string {
	cfg := settings.Default()
	cfg.Agent.Model = "m"
	if mutate != nil {
		mutate(&cfg)
	}
	document, err := cfg.YAML()
	if err != nil {
		panic(err)
	}
	path := filepath.Join(directory, "config.yaml")
	if err := os.WriteFile(path, document, 0o600); err != nil {
		panic(err)
	}
	return path
}

// withMountedSettings makes the operate and controller parsers read a configuration for one test.
func withMountedSettings(t *testing.T, mutate func(*settings.Config)) {
	t.Helper()
	prior := mountedSettings
	mountedSettings = writeTestSettings(t.TempDir(), mutate)
	t.Cleanup(func() { mountedSettings = prior })
}
