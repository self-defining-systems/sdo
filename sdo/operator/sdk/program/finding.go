package program

import (
	"fmt"

	"sdo.dev/operator/sdk"
)

// BindFinding binds one finding without enumerating objects or assignments.
// Explicit ParameterBindings take precedence; otherwise only a unique resource
// of the declared kind in the finding can bind a role. Additional objects are
// read by Diagnose through ReadView, by name or relation.
func BindFinding(spec ProgramSpec, finding sdk.Finding) (Bindings, error) {
	b := Bindings{Objects: ObjectBindings{}}
	used := map[objectIdentity]bool{}
	for _, p := range spec.Params {
		if p.ValueKind != "" {
			continue
		}
		if ref, ok := finding.ParameterBindings[p.Name]; ok {
			if ref.Kind != string(p.Kind) || ref.Name == "" {
				return b, fmt.Errorf("role %q has invalid finding binding; require a named %s", p.Name, p.Kind)
			}
			key := identity(ref)
			if used[key] {
				return b, fmt.Errorf("finding roles share object %s/%s", ref.Kind, ref.Name)
			}
			b.Objects[p.Name] = ref
			used[key] = true
		}
	}
	// Resolve required roles before optional peers of the same kind. Within
	// either group, two unbound roles of one kind require explicit named bindings.
	refs := append([]sdk.ObjectRef{finding.PrimaryResource}, finding.RelatedResources...)
	for _, required := range []bool{true, false} {
		unbound := map[Kind]int{}
		for _, p := range spec.Params {
			if p.Kind != "" && p.Required == required {
				if _, ok := b.Objects[p.Name]; !ok {
					unbound[p.Kind]++
				}
			}
		}
		for _, p := range spec.Params {
			if p.Kind == "" || p.Required != required {
				continue
			}
			if _, ok := b.Objects[p.Name]; ok {
				continue
			}
			choices := map[objectIdentity]sdk.ObjectRef{}
			if unbound[p.Kind] == 1 {
				for _, ref := range refs {
					if ref.Kind == string(p.Kind) && ref.Name != "" && !used[identity(ref)] {
						choices[identity(ref)] = ref
					}
				}
			}
			if len(choices) == 1 {
				for key, ref := range choices {
					b.Objects[p.Name] = ref
					used[key] = true
				}
			}
		}
	}
	if err := ValidateBindings(spec, b); err != nil {
		return b, fmt.Errorf("program.finding-bindings: program %q: %w; bind required roles in the triggering detector's ParameterBindings, or read additional objects through ReadView", spec.ID, err)
	}
	return b, nil
}

type objectIdentity struct{ kind, namespace, name string }

func identity(ref sdk.ObjectRef) objectIdentity {
	return objectIdentity{ref.Kind, ref.Namespace, ref.Name}
}
