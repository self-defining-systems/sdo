package program

import (
	"fmt"
	"slices"
	"strings"
)

// Values of Verdict.Reach.
const (
	ReachInScope    = "in-scope"
	ReachOutOfReach = "out-of-reach"
)

// causeClasses is the closed mechanism vocabulary of a confirmed root cause,
// shared with the responder's ConfirmedRootCause.cause_class.
var causeClasses = []string{
	"network-delay", "network-loss", "network-blocked", "dependency-unavailable",
	"config-value-wrong", "object-missing", "admission-mutated", "admission-rejected",
	"permission-denied", "credential-invalid", "capacity-exhausted", "process-runtime",
	"state-lost", "data-corrupt", "data-missing", "read-only", "code-defect", "other",
}

// CauseClasses lists the values Verdict.CauseClass may take besides "".
func CauseClasses() []string { return slices.Clone(causeClasses) }

func validateCause(class, reach string) error {
	if class != "" && !slices.Contains(causeClasses, class) {
		return fmt.Errorf("cause class %q is not one of %s", class, strings.Join(causeClasses, ", "))
	}
	switch reach {
	case "", ReachInScope, ReachOutOfReach:
	default:
		return fmt.Errorf("reach %q is not %s or %s", reach, ReachInScope, ReachOutOfReach)
	}
	if reach == ReachOutOfReach && class == "" {
		return fmt.Errorf("an out-of-reach verdict names its cause class")
	}
	return nil
}

// EscalationOf returns the Escalate action of a plan, given by value or
// pointer, or false when the plan has none.
func EscalationOf(p Plan) (Escalate, bool) {
	for _, action := range p.Actions {
		switch a := action.(type) {
		case Escalate:
			return a, true
		case *Escalate:
			if a != nil {
				return *a, true
			}
		}
	}
	return Escalate{}, false
}

func validateEscalation(p Plan) error {
	escalation, ok := EscalationOf(p)
	if !ok {
		return nil
	}
	if len(p.Actions) != 1 {
		return fmt.Errorf("Escalate must be the only action of its plan")
	}
	if strings.TrimSpace(escalation.Reason) == "" {
		return fmt.Errorf("Escalate requires a reason")
	}
	for _, ref := range escalation.Resources {
		if ref.Kind == "" || ref.Name == "" {
			return fmt.Errorf("Escalate resources need a kind and name")
		}
	}
	return nil
}
