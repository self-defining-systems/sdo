package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Lock is <plan dir>/lock.json, written once by `plan add`: the exact pins and inputs the plan runs with. The plan file
// and the templates are copied into the plan directory and hashed here; a copy that no longer matches refuses to run.
type Lock struct {
	Version   int               `json:"version"`
	Plan      string            `json:"plan"`
	Created   time.Time         `json:"created"`
	Source    string            `json:"source"` // the plan file it was added from
	Base      string            `json:"base"`   // relative paths in templates are relative to this directory
	PlanSHA   string            `json:"plan_sha256"`
	Pins      Pins              `json:"pins"`
	Templates map[string]string `json:"templates"` // arm -> sha256 of the template copy
	Items     []string          `json:"items"`
	Nodes     int               `json:"nodes"`
}

const lockVersion = 1

func sha(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// planDirs: files of a plan directory.
func planFile(dir string) string          { return filepath.Join(dir, "plan.json") }
func lockFile(dir string) string          { return filepath.Join(dir, "lock.json") }
func stateFile(dir string) string         { return filepath.Join(dir, "state.json") }
func eventsFile(dir string) string        { return filepath.Join(dir, "events.jsonl") }
func templateFile(dir, arm string) string { return filepath.Join(dir, "templates", arm+".json") }

// Dir is the directory of plan name under the plans root.
func Dir(plansRoot, name string) string { return filepath.Join(plansRoot, name) }

// Names lists the plans under the plans root.
func Names(plansRoot string) []string {
	ents, _ := os.ReadDir(plansRoot)
	var out []string
	for _, e := range ents {
		if _, err := os.Stat(lockFile(filepath.Join(plansRoot, e.Name()))); err == nil {
			out = append(out, e.Name())
		}
	}
	return out
}

// Add validates the plan file, copies it and its templates under plansRoot/<name> and writes the lock and an empty
// state. validate, when non-nil, checks a rendered spec (the platform's own spec validation) for one node per arm.
// A plan name is never reused.
func Add(plansRoot, file string, clock Clock, validate func(specJSON []byte) error) (*Lock, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	p, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	abs, err := filepath.Abs(file)
	if err != nil {
		return nil, err
	}
	base := filepath.Dir(abs)
	items, err := p.LoadItems(base)
	if err != nil {
		return nil, err
	}
	nodes, err := p.Expand(items)
	if err != nil {
		return nil, err
	}
	dir := Dir(plansRoot, p.Name)
	if _, err := os.Stat(dir); err == nil {
		return nil, fmt.Errorf("plan %s exists (%s): plan names are never reused", p.Name, dir)
	}
	lock := Lock{Version: lockVersion, Plan: p.Name, Created: clock.Now().UTC(), Source: abs, Base: base, Pins: p.Pins,
		Templates: map[string]string{}, Nodes: len(nodes)}
	for _, it := range items {
		lock.Items = append(lock.Items, it.ID)
	}
	if err := os.MkdirAll(filepath.Join(dir, "templates"), 0o755); err != nil {
		return nil, err
	}
	fail := func(err error) (*Lock, error) {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	for _, a := range p.Arms {
		b, err := os.ReadFile(resolve(base, a.Spec))
		if err != nil {
			return fail(fmt.Errorf("arm %s: %w", a.Name, err))
		}
		tree, err := decodeTemplate(b)
		if err != nil {
			return fail(fmt.Errorf("arm %s: spec %s: %w", a.Name, a.Spec, err))
		}
		out, _ := json.MarshalIndent(tree, "", " ")
		if err := os.WriteFile(templateFile(dir, a.Name), out, 0o644); err != nil {
			return fail(err)
		}
		lock.Templates[a.Name] = sha(out)
	}
	stored := *p
	stored.Items = Items{List: items}
	pb, _ := json.MarshalIndent(&stored, "", " ")
	if err := os.WriteFile(planFile(dir), pb, 0o644); err != nil {
		return fail(err)
	}
	lock.PlanSHA = sha(pb)
	e := &Engine{Dir: dir, Plan: &stored, Lock: lock, Clock: clock, items: items}
	if err := e.dryRender(validate); err != nil {
		return fail(err)
	}
	if err := writeJSONFile(lockFile(dir), lock); err != nil {
		return fail(err)
	}
	st := e.newState(nodes)
	if err := writeJSONFile(stateFile(dir), st); err != nil {
		return fail(err)
	}
	return &lock, nil
}

func writeJSONFile(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readJSONFile(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func readJSONFileBytes(b []byte, v any) error { return json.Unmarshal(b, v) }

// AllExperiments lists every experiment name the plan in dir has released or may have released: each node's attempts,
// including the next one (a submit whose state write was lost).
func AllExperiments(dir string) []string {
	st, err := LoadState(dir)
	var lock Lock
	if err != nil || readJSONFile(lockFile(dir), &lock) != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, n := range st.Nodes {
		for a := 1; a <= n.Attempt+1; a++ {
			name := ExpName(lock.Plan, n.ID, a)
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}
