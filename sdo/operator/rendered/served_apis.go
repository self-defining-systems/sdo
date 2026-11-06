package rendered

import (
	"fmt"
	"slices"
	"strings"
)

// UnservedAPIError is a desired set with objects whose apiVersion/kind the
// recorded cluster discovery does not serve (for example a PodDisruptionBudget
// at policy/v1beta1 on a cluster past 1.25). Nothing was applied.
type UnservedAPIError struct {
	// Objects are "apiVersion Kind namespace/name" of each offender, sorted.
	Objects []string
}

func (e *UnservedAPIError) Error() string {
	shown := e.Objects
	if len(shown) > 5 {
		shown = append(slices.Clone(shown[:5]), fmt.Sprintf("and %d more", len(e.Objects)-5))
	}
	return "rendered objects use API versions the cluster does not serve: " + strings.Join(shown, "; ")
}

// CheckServedAPIs checks that every object of the set uses a group/version/kind
// present in the recorded discovery (renderenv.Discover entries: "group/version"
// and "group/version/Kind"). Kinds a CRD of the same set defines are exempt (they
// are served once the CRD is applied). An empty discovery is unknown and passes.
func CheckServedAPIs(set DesiredSet, served []string) error {
	if len(served) == 0 {
		return nil
	}
	known := make(map[string]bool, len(served))
	for _, entry := range served {
		known[entry] = true
	}
	defined := map[string]bool{}
	for _, phase := range set.Phases {
		for _, object := range phase.Objects {
			if object.Ref.Kind != "CustomResourceDefinition" {
				continue
			}
			kind := nestedString(object.Object.Object, "spec", "names", "kind")
			if g := nestedString(object.Object.Object, "spec", "group"); g != "" && kind != "" {
				defined[g+"/"+kind] = true
			}
		}
	}
	var offenders []string
	for _, phase := range set.Phases {
		for _, object := range phase.Objects {
			ref := object.Ref
			if known[ref.APIVersion+"/"+ref.Kind] {
				continue
			}
			group, _, hasGroup := strings.Cut(ref.APIVersion, "/")
			if !hasGroup {
				group = ""
			}
			if defined[group+"/"+ref.Kind] {
				continue
			}
			offenders = append(offenders, fmt.Sprintf("%s %s %s/%s", ref.APIVersion, ref.Kind, ref.Namespace, ref.Name))
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	slices.Sort(offenders)
	return &UnservedAPIError{Objects: offenders}
}

func nestedString(object map[string]any, fields ...string) string {
	var current any = object
	for _, field := range fields {
		next, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = next[field]
	}
	value, _ := current.(string)
	return value
}
