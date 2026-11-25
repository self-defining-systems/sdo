// Package podtext makes the text SDO puts into a Pod reach its process byte
// for byte. The kubelet expands `$(NAME)` and reduces `$$` to `$` in container
// env values, commands, args and exec probes; nothing else in a Pod spec SDO
// builds is interpreted on the way.
package podtext

import (
	"fmt"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
)

// MaxBytes is below the 128 KiB Linux allows one exec argument or environment
// entry, leaving room for the name.
const MaxBytes = 96 << 10

// Escape returns the Pod spec text the kubelet expands back to exactly text.
// Every `$` is doubled, so no reference is left to resolve, defined or not.
func Escape(text string) string { return strings.ReplaceAll(text, "$", "$$") }

// EscapeAll escapes a command or argument list.
func EscapeAll(args []string) []string {
	if args == nil {
		return nil
	}
	out := make([]string, len(args))
	for i, arg := range args {
		out[i] = Escape(arg)
	}
	return out
}

// Check reports text no Pod spec can carry: the API encodes strings as UTF-8
// and a process argument ends at NUL.
func Check(text string) error {
	switch {
	case !utf8.ValidString(text):
		return fmt.Errorf("text is not valid UTF-8")
	case strings.ContainsRune(text, 0):
		return fmt.Errorf("text contains NUL")
	case len(text) > MaxBytes:
		return fmt.Errorf("text is %d bytes, more than %d", len(text), MaxBytes)
	}
	return nil
}

// Literal escapes every env value, command, argument and exec probe of a Pod
// spec SDO authored. Call it once, on the finished spec, right before it is
// created; a spec copied from the application keeps Kubernetes' meaning and is
// never passed here.
func Literal(spec *corev1.PodSpec) {
	for _, containers := range [][]corev1.Container{spec.InitContainers, spec.Containers} {
		for i := range containers {
			container := &containers[i]
			container.Command, container.Args = EscapeAll(container.Command), EscapeAll(container.Args)
			env := append([]corev1.EnvVar(nil), container.Env...)
			for j := range env {
				env[j].Value = Escape(env[j].Value)
			}
			container.Env = env
			for _, probe := range []**corev1.Probe{&container.StartupProbe, &container.ReadinessProbe, &container.LivenessProbe} {
				if *probe != nil && (*probe).Exec != nil {
					*probe = (*probe).DeepCopy()
					(*probe).Exec.Command = EscapeAll((*probe).Exec.Command)
				}
			}
		}
	}
}
