package program

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"sdo.dev/operator/sdk"
)

// ProposeSourceChange proposes a durable change to accepted source.
// The field at Path of the source-owned object Ref should become Value (an
// observation, such as the replicas a ScaleWorkload just set, or a declared
// parameter). It writes nothing to the cluster or the repository. The
// controller records the resolved value; only after the incident's recovery is
// independently verified does Go capture it as a responder-owned patch under
// .sdo/deployment/patches/, through the same patch validation as any responder
// patch, so the change survives the next deploy. Path uses SetField's dotted
// vocabulary. Reason (40 to 2000 characters) says what fails without the
// change; it becomes the patch's reason.
type ProposeSourceChange struct {
	Ref    sdk.ObjectRef
	Path   string
	Value  Value
	Reason string
}

func (ProposeSourceChange) action() {}

// SourceChangeProposal is the controller's record of one ProposeSourceChange
// with its value resolved to JSON.
type SourceChangeProposal struct {
	Ref    sdk.ObjectRef `json:"ref"`
	Path   string        `json:"path"`
	Value  any           `json:"value"`
	Reason string        `json:"reason"`
}

func (a ProposeSourceChange) Validate() error {
	if a.Ref.APIVersion == "" || a.Ref.Kind == "" || a.Ref.Namespace == "" || a.Ref.Name == "" {
		return fmt.Errorf("ProposeSourceChange requires a namespaced object with API version, kind and name")
	}
	if strings.EqualFold(a.Ref.Kind, "Secret") {
		return fmt.Errorf("ProposeSourceChange cannot change a Secret")
	}
	if strings.HasPrefix(a.Path, "/") {
		return fmt.Errorf("ProposeSourceChange takes a dotted SetField path, not a JSON pointer")
	}
	if err := ValidateSetField(SetField{Ref: a.Ref, Path: a.Path, Value: a.Value}); err != nil {
		return fmt.Errorf("ProposeSourceChange: %w", err)
	}
	if a.Value.Origin() == SourceValue {
		return fmt.Errorf("ProposeSourceChange of the accepted source value changes nothing")
	}
	if n := utf8.RuneCountInString(strings.TrimSpace(a.Reason)); n < 40 || n > 2000 {
		return fmt.Errorf("ProposeSourceChange reason must be 40 to 2000 characters saying what fails without the change, got %d", n)
	}
	return nil
}

// SourceChangeAction returns a ProposeSourceChange given by value or pointer.
func SourceChangeAction(action Action) (ProposeSourceChange, bool) {
	switch a := action.(type) {
	case ProposeSourceChange:
		return a, true
	case *ProposeSourceChange:
		if a != nil {
			return *a, true
		}
	}
	return ProposeSourceChange{}, false
}

// validateSourceChanges keeps proposals after every action that writes, so a
// proposal records the change the plan's writes made.
func validateSourceChanges(p Plan) error {
	proposing := false
	for _, action := range p.Actions {
		_, proposal := SourceChangeAction(action)
		if proposing && !proposal {
			return fmt.Errorf("ProposeSourceChange actions follow every other action of their plan")
		}
		proposing = proposing || proposal
	}
	return nil
}
