// Package plan is the lanes plan engine: a declarative plan.yaml (arms x items x reps, edges that carry a verified
// artifact, lane-free steps, pins) that the runner executes and recovers from state on disk. It sits above the
// experiments the manager runs: it releases one experiment per node, in plan order, once the node's inputs are
// verified, and observes the result through a Backend. It knows no benchmark and no operator.
//
// Semantics are those of benchmarks/sregym/eval/plan.py (a recorded order, a dependency graph, a merge step that
// pass 2 waits for), generalised: arms and edges are data.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/internal/miniyaml"
)

// Orders of the nodes. rep-major runs rep 1 of every item before rep 2; item-major runs every rep of an item first.
const (
	RepMajor  = "rep-major"
	ItemMajor = "item-major"
)

// Verify modes of an edge: the artifact exists and is non-empty; the subject's verify-artifact verb answers; a
// command (`cmd:<shell>`) exits 0. Every mode runs before a dependent starts, never after.
const (
	VerifyExists  = "exists"
	VerifySubject = "subject"
	VerifyCmd     = "cmd:"
)

// Pin is a checkout identity, name@revision. The revision is recorded in the lock and in every row; the engine
// writes it into the spec of every run.
type Pin struct {
	Name, Rev string
}

func (p Pin) String() string { return p.Name + "@" + p.Rev }

// UnmarshalJSON reads "name@rev".
func (p *Pin) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("pin: want a string name@revision: %w", err)
	}
	name, rev, ok := strings.Cut(s, "@")
	if !ok || name == "" || !revRE.MatchString(rev) {
		return fmt.Errorf("pin %q: want name@revision (revision: 7-40 hex digits)", s)
	}
	p.Name, p.Rev = name, rev
	return nil
}

// MarshalJSON writes "name@rev".
func (p Pin) MarshalJSON() ([]byte, error) { return json.Marshal(p.String()) }

// Pins are the checkouts per role: the workload adapter and the subject adapter.
type Pins struct {
	Workload Pin `json:"workload"`
	Subject  Pin `json:"subject"`
}

// Arm is one kind of run of an item. Its spec template is a spec file (JSON or the YAML subset) in which `${...}`
// variables are replaced per node: ${plan} ${arm} ${item} ${item.<var>} ${rep} ${name} and ${artifact.<kind>} for the
// artifacts of the arm's incoming edges. Relative paths in the template are relative to the plan file.
type Arm struct {
	Name string            `json:"name"`
	Spec string            `json:"spec"`
	Vars map[string]string `json:"vars,omitempty"`
}

// Item is one thing the arms run: a problem, a fault, an instance. Vars are available to templates as ${item.<var>}.
//
// Group names an entry of Plan.Groups whose vars and spec overrides the item inherits. Spec is a typed override merged
// over the arm's rendered template (see mergeSpec): any spec field, numbers, lists and nested maps included.
type Item struct {
	ID    string            `json:"id"`
	Vars  map[string]string `json:"vars,omitempty"`
	Group string            `json:"group,omitempty"`
	Spec  map[string]any    `json:"spec,omitempty"`
}

// Group is what several items share: vars (as ${item.<var>}, item vars win) and a spec override (item spec wins).
type Group struct {
	Vars map[string]string `json:"vars,omitempty"`
	Spec map[string]any    `json:"spec,omitempty"`
}

// Items is the item list: written out, or read from a JSON file (`from`, and `select`: the key of the array in a
// top-level object; elements are ids or objects with an id).
type Items struct {
	List   []Item
	From   string
	Select string
}

// UnmarshalJSON accepts a list of ids/objects or {from, select}.
func (it *Items) UnmarshalJSON(b []byte) error {
	var raw any
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	switch v := raw.(type) {
	case []any:
		list, err := itemsOf(v)
		it.List = list
		return err
	case map[string]any:
		for k, x := range v {
			s, ok := x.(string)
			if !ok || (k != "from" && k != "select") {
				return fmt.Errorf("items: unknown or non-string field %q (want from, select)", k)
			}
			if k == "from" {
				it.From = s
			} else {
				it.Select = s
			}
		}
		if it.From == "" {
			return fmt.Errorf("items: an object needs from")
		}
		return nil
	}
	return fmt.Errorf("items: want a list or {from, select}")
}

func itemsOf(list []any) ([]Item, error) {
	var out []Item
	for i, e := range list {
		switch v := e.(type) {
		case string:
			out = append(out, Item{ID: v})
		case map[string]any:
			id, _ := v["id"].(string)
			if id == "" {
				return nil, fmt.Errorf("items[%d]: no id", i)
			}
			it := Item{ID: id}
			vars, _ := v["vars"].(map[string]any)
			for k := range v {
				if k != "id" && k != "vars" && k != "group" && k != "spec" {
					return nil, fmt.Errorf("items[%d] (%s): unknown field %q (want id, vars, group, spec)", i, id, k)
				}
			}
			if g, ok := v["group"]; ok {
				if it.Group, ok = g.(string); !ok || it.Group == "" {
					return nil, fmt.Errorf("items[%d] (%s): group is not a name", i, id)
				}
			}
			if sp, ok := v["spec"]; ok {
				if it.Spec, ok = sp.(map[string]any); !ok {
					return nil, fmt.Errorf("items[%d] (%s): spec is not a map", i, id)
				}
			}
			for k, x := range vars {
				s, ok := x.(string)
				if !ok {
					return nil, fmt.Errorf("items[%d] (%s): var %q is not a string", i, id, k)
				}
				if it.Vars == nil {
					it.Vars = map[string]string{}
				}
				it.Vars[k] = s
			}
			out = append(out, it)
		default:
			return nil, fmt.Errorf("items[%d]: want an id or an object", i)
		}
	}
	return out, nil
}

// MarshalJSON writes the expanded list (the stored copy of a plan is self-contained).
func (it Items) MarshalJSON() ([]byte, error) {
	if it.From != "" {
		m := map[string]string{"from": it.From}
		if it.Select != "" {
			m["select"] = it.Select
		}
		return json.Marshal(m)
	}
	return json.Marshal(it.List)
}

// Edge: the node of arm To for an (item, rep) starts only after From (an arm: the same item and rep; a step) is done
// and, when the edge names an artifact, the artifact has been exported and verified.
type Edge struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Artifact string `json:"artifact,omitempty"`
	// Verify overrides the artifact's default mode (exists, subject, cmd:<shell>).
	Verify string `json:"verify,omitempty"`
}

// ArtifactDef says where an artifact kind lives. Path is a template resolved by the backend (${run} the newest done
// run directory of the source, ${exp_dir} its experiment directory; for a step ${step_dir}).
type ArtifactDef struct {
	Path   string `json:"path"`
	Verify string `json:"verify,omitempty"`
}

// Step is a lane-free step: a command that runs off-lane once its inputs are done (the merge of the pass-1 memories,
// say). Per "rep" it runs once per rep, after that rep's nodes of the After arms; per "plan" (default) once, after
// all of them. A step's exit 0 is its success; its output directory is its artifact.
type Step struct {
	Name    string   `json:"name"`
	After   []string `json:"after"`
	Run     string   `json:"run"`
	Per     string   `json:"per,omitempty"`
	Lane    string   `json:"lane,omitempty"`
	Timeout string   `json:"timeout,omitempty"`
	// Out is the artifact path template when an edge takes an artifact from the step.
	Out string `json:"out,omitempty"`
}

// Plan is plan.yaml.
type Plan struct {
	Name string `json:"plan"`
	Pins Pins   `json:"pins"`
	Arms []Arm  `json:"arms"`
	// Groups are shared vars and spec overrides items name with `group` (one plan, several apps).
	Groups map[string]Group `json:"groups,omitempty"`
	Items  Items            `json:"items"`
	Reps   int              `json:"reps,omitempty"`
	Order  string           `json:"order,omitempty"`
	// Parallel bounds the experiments released and not yet ended (default 4): the manager's lanes decide how many run.
	Parallel  int                    `json:"parallel,omitempty"`
	Edges     []Edge                 `json:"edges,omitempty"`
	Steps     []Step                 `json:"steps,omitempty"`
	Artifacts map[string]ArtifactDef `json:"artifacts,omitempty"`
	// Isolation is written into every spec: "two-way" requires reserved hosts, "one-way-ok" accepts the rest.
	Isolation string `json:"isolation,omitempty"`
	// Stages is the minimum time of each stage ("pre-inject: 12m"): the runner alarms when a stage takes more than
	// Alarms.Factor times its minimum. Alarms configures the other runner alarms.
	Stages map[string]string `json:"stages,omitempty"`
	Alarms *AlarmConfig      `json:"alarms,omitempty"`
}

var (
	nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,23}$`)
	armRE  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{0,15}$`)
	idRE   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	kindRE = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,23}$`)
	revRE  = regexp.MustCompile(`^[0-9a-f]{7,40}$`)
)

// Parse reads plan text (JSON or the YAML subset) and validates it.
func Parse(b []byte) (*Plan, error) {
	var p Plan
	if err := miniyaml.Decode(b, &p); err != nil {
		return nil, err
	}
	p.defaults()
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *Plan) defaults() {
	if p.Reps == 0 {
		p.Reps = 1
	}
	if p.Order == "" {
		p.Order = RepMajor
	}
	if p.Parallel == 0 {
		p.Parallel = 4
	}
	for i := range p.Steps {
		if p.Steps[i].Per == "" {
			p.Steps[i].Per = "plan"
		}
		if p.Steps[i].Lane == "" {
			p.Steps[i].Lane = "none"
		}
	}
}

// Validate checks the plan on its own: no files are read except by LoadItems.
func (p *Plan) Validate() error {
	var errs []string
	bad := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if !nameRE.MatchString(p.Name) {
		bad("plan %q: lowercase letters, digits and '-', at most 24", p.Name)
	}
	if p.Pins.Workload.Name == "" || p.Pins.Subject.Name == "" {
		bad("pins: workload and subject are both required (name@revision)")
	}
	if p.Reps < 1 {
		bad("reps must be at least 1")
	}
	if p.Order != RepMajor && p.Order != ItemMajor {
		bad("order %q: want %s or %s", p.Order, RepMajor, ItemMajor)
	}
	if p.Parallel < 1 {
		bad("parallel must be at least 1")
	}
	if p.Isolation != "" && p.Isolation != "two-way" && p.Isolation != "one-way-ok" {
		bad("isolation %q: want two-way or one-way-ok", p.Isolation)
	}
	arms := map[string]bool{}
	if len(p.Arms) == 0 {
		bad("no arms")
	}
	for _, a := range p.Arms {
		switch {
		case !armRE.MatchString(a.Name):
			bad("arm %q: lowercase letters, digits, '.' and '-', at most 16", a.Name)
		case arms[a.Name]:
			bad("arm %q repeated", a.Name)
		case a.Spec == "":
			bad("arm %q: no spec template", a.Name)
		}
		arms[a.Name] = true
	}
	for name, g := range p.Groups {
		if !idRE.MatchString(name) {
			bad("group %q: letters, digits, '.', '_' and '-'", name)
		}
		if err := checkOverride(g.Spec); err != nil {
			bad("group %q: %v", name, err)
		}
	}
	steps := map[string]bool{}
	for _, s := range p.Steps {
		switch {
		case !armRE.MatchString(s.Name):
			bad("step %q: lowercase letters, digits, '.' and '-'", s.Name)
		case arms[s.Name] || steps[s.Name]:
			bad("step %q: name already used", s.Name)
		case s.Run == "":
			bad("step %q: no run command", s.Name)
		case len(s.After) == 0:
			bad("step %q: after names no arm", s.Name)
		case s.Per != "plan" && s.Per != "rep":
			bad("step %q: per %q (want plan or rep)", s.Name, s.Per)
		case s.Lane != "none":
			bad("step %q: lane %q (only lane-free steps exist: lane: none)", s.Name, s.Lane)
		}
		for _, a := range s.After {
			if !arms[a] {
				bad("step %q: after %q is not an arm", s.Name, a)
			}
		}
		steps[s.Name] = true
	}
	for kind, d := range p.Artifacts {
		if !kindRE.MatchString(kind) || d.Path == "" {
			bad("artifact %q: a kind name and a path", kind)
		}
		if err := checkVerify(d.Verify); err != nil {
			bad("artifact %q: %v", kind, err)
		}
	}
	for i, e := range p.Edges {
		if !arms[e.From] && !steps[e.From] {
			bad("edges[%d]: from %q is neither an arm nor a step", i, e.From)
		}
		if !arms[e.To] {
			bad("edges[%d]: to %q is not an arm", i, e.To)
		}
		if e.Artifact != "" {
			if _, ok := p.Artifacts[e.Artifact]; !ok && !steps[e.From] {
				bad("edges[%d]: artifact %q is not declared in artifacts", i, e.Artifact)
			}
			if steps[e.From] && p.stepOf(e.From).Out == "" {
				bad("edges[%d]: step %q carries an artifact but has no out", i, e.From)
			}
		}
		if err := checkVerify(e.Verify); err != nil {
			bad("edges[%d]: %v", i, err)
		}
	}
	if len(errs) == 0 {
		if _, err := p.armOrder(); err != nil {
			bad("%v", err)
		}
	}
	errs = append(errs, p.validateAlarms()...)
	if len(errs) > 0 {
		return fmt.Errorf("plan: %s", strings.Join(errs, "; "))
	}
	return nil
}

func checkVerify(v string) error {
	switch {
	case v == "", v == VerifyExists, v == VerifySubject, strings.HasPrefix(v, VerifyCmd) && len(v) > len(VerifyCmd):
		return nil
	}
	return fmt.Errorf("verify %q: want exists, subject or cmd:<shell>", v)
}

func (p *Plan) stepOf(name string) Step {
	for _, s := range p.Steps {
		if s.Name == name {
			return s
		}
	}
	return Step{}
}

func (p *Plan) isStep(name string) bool {
	for _, s := range p.Steps {
		if s.Name == name {
			return true
		}
	}
	return false
}

// armOrder is the arms in declaration order, moved so every arm follows the arms its edges take from. A cycle is an
// error. Declaration order breaks every other tie: the order inside an item is the author's, never the alphabet's.
func (p *Plan) armOrder() ([]string, error) {
	need := map[string][]string{}
	for _, e := range p.Edges {
		if !p.isStep(e.From) {
			need[e.To] = append(need[e.To], e.From)
		}
	}
	var order []string
	done, onPath := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(a string) error {
		if done[a] {
			return nil
		}
		if onPath[a] {
			return fmt.Errorf("edges form a cycle through arm %q", a)
		}
		onPath[a] = true
		for _, d := range need[a] {
			if err := visit(d); err != nil {
				return err
			}
		}
		onPath[a], done[a] = false, true
		order = append(order, a)
		return nil
	}
	for _, a := range p.Arms {
		if err := visit(a.Name); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// LoadItems resolves `items.from` against dir and returns the final list. A plan with a written-out list returns it.
func (p *Plan) LoadItems(dir string) ([]Item, error) {
	list := p.Items.List
	if p.Items.From != "" {
		b, err := os.ReadFile(resolve(dir, p.Items.From))
		if err != nil {
			return nil, fmt.Errorf("items.from: %w", err)
		}
		var raw any
		if err := json.Unmarshal(b, &raw); err != nil {
			return nil, fmt.Errorf("items.from %s: %w", p.Items.From, err)
		}
		if p.Items.Select != "" {
			m, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("items.from %s: select %q needs a JSON object", p.Items.From, p.Items.Select)
			}
			raw = m[p.Items.Select]
		}
		arr, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("items.from %s: no array of items (select %q)", p.Items.From, p.Items.Select)
		}
		if list, err = itemsOf(arr); err != nil {
			return nil, err
		}
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("plan has no items")
	}
	seen := map[string]bool{}
	for _, it := range list {
		if it.Group != "" {
			if _, ok := p.Groups[it.Group]; !ok {
				return nil, fmt.Errorf("item %q: group %q is not declared in groups", it.ID, it.Group)
			}
		}
		if err := checkOverride(it.Spec); err != nil {
			return nil, fmt.Errorf("item %q: %v", it.ID, err)
		}
		if !idRE.MatchString(it.ID) {
			return nil, fmt.Errorf("item %q: letters, digits, '.', '_' and '-'", it.ID)
		}
		if seen[it.ID] {
			return nil, fmt.Errorf("item %q repeated", it.ID)
		}
		seen[it.ID] = true
	}
	return list, nil
}

func resolve(dir, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(dir, p)
}

// Node is one run to do: an arm on an item in a rep.
type Node struct {
	ID    string // <arm>/<item>/r<rep>
	Arm   string
	Item  string
	Rep   int
	Index int // position in the plan's order
}

// NodeID is the id of the node of arm on item in rep.
func NodeID(arm, item string, rep int) string { return fmt.Sprintf("%s/%s/r%d", arm, item, rep) }

// StepID is the id of a step instance (rep 0: a per-plan step).
func StepID(name string, rep int) string {
	if rep == 0 {
		return "step:" + name
	}
	return fmt.Sprintf("step:%s/r%d", name, rep)
}

// Expand lists every node in the plan's order. rep-major: rep 1 of every item before rep 2. Within a rep the items
// run in list order and all arms of an item come together (arms interleaved per item, in dependency-then-declaration
// order), so a comparison's partner is released right after its pair and never behind the whole arm.
func (p *Plan) Expand(items []Item) ([]Node, error) {
	arms, err := p.armOrder()
	if err != nil {
		return nil, err
	}
	var nodes []Node
	add := func(arm string, it Item, rep int) {
		nodes = append(nodes, Node{ID: NodeID(arm, it.ID, rep), Arm: arm, Item: it.ID, Rep: rep})
	}
	if p.Order == RepMajor {
		for rep := 1; rep <= p.Reps; rep++ {
			for _, it := range items {
				for _, a := range arms {
					add(a, it, rep)
				}
			}
		}
	} else {
		for _, it := range items {
			for rep := 1; rep <= p.Reps; rep++ {
				for _, a := range arms {
					add(a, it, rep)
				}
			}
		}
	}
	for i := range nodes {
		nodes[i].Index = i
	}
	return nodes, nil
}

// StepInstance is a step for a rep (0: per plan).
type StepInstance struct {
	ID   string
	Name string
	Rep  int
}

// StepInstances lists the steps to run, in declaration order.
func (p *Plan) StepInstances() []StepInstance {
	var out []StepInstance
	for _, s := range p.Steps {
		if s.Per == "rep" {
			for rep := 1; rep <= p.Reps; rep++ {
				out = append(out, StepInstance{ID: StepID(s.Name, rep), Name: s.Name, Rep: rep})
			}
		} else {
			out = append(out, StepInstance{ID: StepID(s.Name, 0), Name: s.Name})
		}
	}
	return out
}

// ExpName is the experiment name of attempt (1-based) of a node: <plan>-<arm>-<item>-r<rep>[-x<attempt-1>], lowercase,
// at most 40 characters (the manager's limit); a longer name keeps its head and ends in a hash of the full id.
func ExpName(plan, nodeID string, attempt int) string {
	suffix := ""
	if attempt > 1 {
		suffix = fmt.Sprintf("-x%d", attempt-1)
	}
	parts := strings.Split(nodeID, "/")
	base := plan + "-" + strings.Join(parts, "-")
	base = strings.ToLower(base)
	var b strings.Builder
	for _, r := range base {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	name := b.String()
	const limit = 40
	sum := sha256.Sum256([]byte(plan + "/" + nodeID))
	tail := hex.EncodeToString(sum[:])[:6]
	if len(name)+len(suffix) > limit || name != strings.ToLower(plan+"-"+strings.Join(parts, "-")) {
		room := limit - len(suffix) - len(tail) - 1
		name = strings.TrimRight(name[:min(len(name), room)], "-") + "-" + tail
	}
	return name + suffix
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
