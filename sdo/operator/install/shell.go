package install

import (
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/shellpath"
)

// ShellRunnerConfig is shared by lifecycle verification and installation so the
// read-only runner can verify initial health before the controller is started.
func ShellRunnerConfig(c Config) (shellpath.RunnerConfig, bool) {
	source := c.CodePath.GetOverrides().GetDetectorSource()
	if source == "" {
		source = c.CodePath.GetName()
	}
	if source != "shell" {
		return shellpath.RunnerConfig{}, false
	}
	image := c.ScriptImage
	if image == "" {
		image = c.ControllerImage
	}
	return shellpath.RunnerConfig{Namespace: c.Namespace, ControlNamespace: c.controlNamespace(), Image: image, RepositoryPVC: c.RepositoryPVC, RepositoryMountPath: "/workspace", AcceptedRoot: "/workspace/application"}, true
}

func codePathArgs(c Config) []string {
	if c.CodePath == nil {
		return nil
	}
	args := []string{}
	if c.CodePath.GetName() != "" {
		args = append(args, "--code-path", c.CodePath.GetName())
	}
	if c.CodePath.GetOverrides() != nil {
		data, err := wire.Marshal(c.CodePath.GetOverrides())
		if err != nil {
			panic(err)
		}
		args = append(args, "--code-path-overrides", string(data))
	}
	return args
}
