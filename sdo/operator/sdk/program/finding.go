package program

import (
	"fmt"
	"slices"

	"sdo.dev/operator/sdk"
)

// BindFinding binds one finding without enumerating objects or assignments.
// Explicit ParameterBindings take precedence; otherwise only a unique resource
// of the declared kind in the finding can bind a role. Additional objects are
// read by Diagnose through ReadView, by name or relation.
//
// A finding that lists playbooks binds only those programs, by name or by
// resource; every other program gets an error.
//
// Named bindings are the finding detector's claim about what its own playbooks
// repair, so they bind only programs the finding lists by logical ID (the
// Finding.Playbooks contract; the controller rejects any other reference). A
// finding that lists no playbook (drift and source-inventory findings) offers
// them to every program; the memory check refuses an incident detector that
// sets named bindings without declaring its playbooks.
func BindFinding(spec ProgramSpec, finding sdk.Finding) (Bindings, error) {
	b := Bindings{Objects: ObjectBindings{}}
	used := map[objectIdentity]bool{}
	named := len(finding.Playbooks) == 0 || slices.Contains(finding.Playbooks, spec.ID)
	for _, p := range spec.Params {
		if p.ValueKind != "" || !named {
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
	if len(finding.Playbooks) > 0 && !named {
		// A finding that names playbooks routes only those; it never offers its
		// resources to other programs by role kind.
		return b, fmt.Errorf("program.finding-bindings: program %q: finding lists playbooks %v, not this one", spec.ID, finding.Playbooks)
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

// CheckFindingRoles rejects object roles that no unnamed finding can bind.
// Health and generated detectors never name roles: BindFinding fills a role
// from such a finding only when it is the one unbound role of its kind. So for
// each declared kind, a finding on one object of that kind (with one object of
// every other declared kind as related resources) must bind a role of that
// kind. Kinds where BindFinding refuses the probe need named ParameterBindings
// from an incident detector, as for a required pair of one kind; those stay
// allowed. Two optional roles of one kind (aliases) never bind and are refused.
func CheckFindingRoles(spec ProgramSpec) error {
	var kinds []Kind
	roles := map[Kind][]string{}
	for _, p := range spec.Params {
		if p.Kind == "" {
			continue
		}
		if roles[p.Kind] == nil {
			kinds = append(kinds, p.Kind)
		}
		roles[p.Kind] = append(roles[p.Kind], p.Name)
	}
	for _, kind := range kinds {
		finding := sdk.Finding{PrimaryResource: sdk.ObjectRef{Kind: string(kind), Name: "finding-object"}}
		for _, other := range kinds {
			if other != kind {
				finding.RelatedResources = append(finding.RelatedResources, sdk.ObjectRef{Kind: string(other), Name: "related-object"})
			}
		}
		bound, err := BindFinding(spec, finding)
		if err != nil {
			continue
		}
		filled := false
		for _, name := range roles[kind] {
			_, ok := bound.Objects[name]
			filled = filled || ok
		}
		if !filled {
			return fmt.Errorf("program.finding-bindings: program %q: roles %v are all %s, so a finding on one %s binds none of them; declare one role per kind and read other objects through ReadView", spec.ID, roles[kind], kind, kind)
		}
	}
	return nil
}

type objectIdentity struct{ kind, namespace, name string }

func identity(ref sdk.ObjectRef) objectIdentity {
	return objectIdentity{ref.Kind, ref.Namespace, ref.Name}
}
