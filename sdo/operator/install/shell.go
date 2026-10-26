package install

import "sdo.dev/operator/shellpath"

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
