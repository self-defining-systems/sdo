package program

import (
	"fmt"
	"strings"
	"time"
)

const MaxScriptBytes = 64 << 10
const MaxScriptOutputBytes = 64 << 10
const DefaultScriptTimeout = time.Minute
const MaxScriptTimeout = 5 * time.Minute

// RunScript is a first-class learnable mitigation. Application facts are
// relation-derived Values injected as environment, never interpolated into code.
// The executor runs bash in an app-local Job and records the observed transition.
// Typed actions remain optional upgrades; opaque effects are never simulated.
type RunScript struct {
	Script  string
	Env     map[string]Value
	Timeout time.Duration
	Reason  string
}

func (RunScript) action() {}

func (a RunScript) Validate() error {
	if strings.TrimSpace(a.Script) == "" || len(a.Script) > MaxScriptBytes || strings.ContainsRune(a.Script, 0) {
		return fmt.Errorf("RunScript requires nonempty script of at most 64 KiB without NUL")
	}
	if a.Timeout < 0 || a.Timeout > MaxScriptTimeout {
		return fmt.Errorf("RunScript timeout must be between zero and five minutes")
	}
	for name, value := range a.Env {
		if !credentialEnv.MatchString(name) {
			return fmt.Errorf("RunScript invalid environment name %q", name)
		}
		if name == "KUBECONFIG" || strings.HasPrefix(name, "SDO_SCRIPT_") {
			return fmt.Errorf("RunScript environment %q is executor-owned", name)
		}
		if err := value.Validate(); err != nil {
			return fmt.Errorf("RunScript environment %s: %w", name, err)
		}
	}
	return nil
}
func validateScriptAction(action Action) error {
	switch a := action.(type) {
	case RunScript:
		return a.Validate()
	case *RunScript:
		if a == nil {
			return fmt.Errorf("nil RunScript")
		}
		return a.Validate()
	}
	return nil
}
