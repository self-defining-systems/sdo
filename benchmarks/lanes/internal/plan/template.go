package plan

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/miniyaml"
)

var varRE = regexp.MustCompile(`\$\{([A-Za-z0-9_.-]+)\}`)

func decodeTemplate(b []byte) (map[string]any, error) {
	var tree map[string]any
	if err := miniyaml.Decode(b, &tree); err != nil {
		return nil, err
	}
	return tree, nil
}

// vars are the template variables of one node.
func (e *Engine) vars(n Node, name string, artifacts map[string]string) map[string]string {
	v := map[string]string{"plan": e.Plan.Name, "arm": n.Arm, "item": n.Item, "rep": fmt.Sprint(n.Rep), "name": name}
	for _, it := range e.items {
		if it.ID == n.Item {
			for k, x := range e.Plan.Groups[it.Group].Vars {
				v["item."+k] = x
			}
			for k, x := range it.Vars {
				v["item."+k] = x
			}
		}
	}
	for _, a := range e.Plan.Arms {
		if a.Name == n.Arm {
			for k, x := range a.Vars {
				v["arm."+k] = x
			}
		}
	}
	for k, x := range artifacts {
		v["artifact."+k] = x
	}
	return v
}

func substitute(tree any, vars map[string]string) (any, error) {
	switch t := tree.(type) {
	case string:
		var missing []string
		out := varRE.ReplaceAllStringFunc(t, func(m string) string {
			k := varRE.FindStringSubmatch(m)[1]
			v, ok := vars[k]
			if !ok {
				missing = append(missing, k)
			}
			return v
		})
		if len(missing) > 0 {
			return nil, fmt.Errorf("unknown variable ${%s} in %q", strings.Join(missing, "}, ${"), t)
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, v := range t {
			s, err := substitute(v, vars)
			if err != nil {
				return nil, err
			}
			out[k] = s
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, v := range t {
			s, err := substitute(v, vars)
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
		return out, nil
	}
	return tree, nil
}

// Render is the spec of attempt of node n, as JSON. The engine owns these fields whatever the template says: name,
// the pinned workload revision (fuzzops), the pinned subject revision (operator.revision), retries 0 (only the runner
// retries) and the plan's isolation; relative input paths are made absolute against the plan file's directory.
func (e *Engine) Render(n Node, attempt int, artifacts map[string]string) ([]byte, string, error) {
	name := ExpName(e.Plan.Name, n.ID, attempt)
	raw, err := readFileTree(templateFile(e.Dir, n.Arm))
	if err != nil {
		return nil, "", err
	}
	out, err := substitute(raw, e.vars(n, name, artifacts))
	if err != nil {
		return nil, "", fmt.Errorf("arm %s: %w", n.Arm, err)
	}
	spec := out.(map[string]any)
	if ov, err := e.override(n, e.vars(n, name, artifacts)); err != nil {
		return nil, "", fmt.Errorf("item %s: %w", n.Item, err)
	} else if ov != nil {
		mergeSpec(spec, ov)
	}
	spec["name"] = name
	pins := e.pinsOf(n.Pins)
	spec["fuzzops"] = pins.Workload.Rev
	op, _ := spec["operator"].(map[string]any)
	if op == nil {
		op = map[string]any{}
		spec["operator"] = op
	}
	if pins.Subject.Rev != "" {
		op["revision"] = pins.Subject.Rev
	} else {
		delete(op, "revision")
	}
	spec["retries"] = 0
	if e.Plan.Isolation != "" {
		spec["isolation"] = e.Plan.Isolation
	}
	abs := func(m map[string]any, key string) {
		if s, ok := m[key].(string); ok && s != "" && !filepath.IsAbs(s) {
			m[key] = filepath.Join(e.Lock.Base, s)
		}
	}
	if f, ok := spec["faults"].(map[string]any); ok {
		abs(f, "instances")
	}
	abs(op, "baseline")
	abs(op, "seed")
	b, err := json.MarshalIndent(spec, "", " ")
	return b, name, err
}

// engineOwned are the spec fields the engine sets itself; an override may not name them.
var engineOwned = []string{"name", "fuzzops", "retries", "isolation"}

// checkOverride refuses an override that sets a field the engine owns.
func checkOverride(ov map[string]any) error {
	for _, k := range engineOwned {
		if _, ok := ov[k]; ok {
			return fmt.Errorf("spec override sets %q, which the plan owns", k)
		}
	}
	if op, ok := ov["operator"].(map[string]any); ok {
		if _, ok := op["revision"]; ok {
			return fmt.Errorf("spec override sets operator.revision, which the plan owns (pins.subject)")
		}
	}
	return nil
}

// override is the item's spec override: its group's, merged under its own, with the variables substituted.
func (e *Engine) override(n Node, vars map[string]string) (map[string]any, error) {
	for _, it := range e.items {
		if it.ID != n.Item {
			continue
		}
		merged := map[string]any{}
		mergeSpec(merged, e.Plan.Groups[it.Group].Spec)
		mergeSpec(merged, it.Spec)
		if len(merged) == 0 {
			return nil, nil
		}
		out, err := substitute(merged, vars)
		if err != nil {
			return nil, err
		}
		return out.(map[string]any), nil
	}
	return nil, nil
}

// mergeSpec merges src into dst: maps merge key by key, every other value (numbers, strings, lists) replaces, and a
// null removes the key. src is not modified.
func mergeSpec(dst, src map[string]any) {
	for k, v := range src {
		switch x := v.(type) {
		case nil:
			delete(dst, k)
		case map[string]any:
			d, ok := dst[k].(map[string]any)
			if !ok {
				d = map[string]any{}
				dst[k] = d
			}
			mergeSpec(d, x)
		default:
			dst[k] = v
		}
	}
}

func readFileTree(path string) (map[string]any, error) {
	var tree map[string]any
	if err := readJSONFile(path, &tree); err != nil {
		return nil, err
	}
	return tree, nil
}

// dryRender renders every node with placeholder artifacts and hands the result to validate (one per distinct content):
// a typo in a template fails `plan add`, not the first release.
func (e *Engine) dryRender(validate func([]byte) error) error {
	seen := map[string]bool{}
	nodes, err := e.Plan.Expand(e.items)
	if err != nil {
		return err
	}
	for _, n := range nodes { // every node of every arm: a rep or an item can differ in what its template renders
		b, name, err := e.Render(n, 1, e.placeholderArtifacts(n.Arm))
		if err != nil {
			return err
		}
		// Every item is validated (an override can break one item's spec), but one spec per distinct content.
		key := strings.ReplaceAll(string(b), name, "")
		if seen[key] {
			continue
		}
		seen[key] = true
		if validate != nil {
			if err := validate(b); err != nil {
				return fmt.Errorf("arm %s, item %s: spec: %w", n.Arm, n.Item, err)
			}
		}
	}
	return nil
}

// PlaceholderPrefix starts the stand-in path an artifact edge's field gets at add time: a path of the right kind (an
// absolute one) whose file does not exist yet, so a rule on field combinations still fires and a rule about the
// file's content cannot.
const PlaceholderPrefix = "/plan-artifact/"

func (e *Engine) placeholderArtifacts(arm string) map[string]string {
	m := map[string]string{}
	for _, ed := range e.Plan.Edges {
		if ed.To == arm && ed.Artifact != "" {
			m[ed.Artifact] = PlaceholderPrefix + ed.Artifact
		}
	}
	return m
}
