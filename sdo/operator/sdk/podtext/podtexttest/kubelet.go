// Package podtexttest models how the kubelet expands Pod spec text
// (kubernetes third_party/forked/golang/expansion), for round-trip tests.
package podtexttest

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Expand replaces $(NAME) with the named value, leaves a reference to an
// undefined name as written, and reduces $$ to $.
func Expand(input string, env map[string]string) string {
	var out strings.Builder
	for i := 0; i < len(input); i++ {
		if input[i] != '$' || i+1 == len(input) {
			out.WriteByte(input[i])
			continue
		}
		i++
		switch input[i] {
		case '$':
			out.WriteByte('$')
		case '(':
			end := strings.IndexByte(input[i+1:], ')')
			if end < 0 {
				out.WriteString("$(")
				continue
			}
			name := input[i+1 : i+1+end]
			if value, ok := env[name]; ok {
				out.WriteString(value)
			} else {
				out.WriteString("$(" + name + ")")
			}
			i += 1 + end
		default:
			out.WriteByte('$')
			out.WriteByte(input[i])
		}
	}
	return out.String()
}

// Process returns the environment and argument vector the container's process gets.
func Process(container corev1.Container) (env map[string]string, argv []string) {
	env = map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = Expand(variable.Value, env)
	}
	for _, arg := range append(append([]string(nil), container.Command...), container.Args...) {
		argv = append(argv, Expand(arg, env))
	}
	return env, argv
}
