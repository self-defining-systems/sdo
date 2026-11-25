// Package staleness is a PROTOTYPE of three deterministic rules that decide
// whether a learned playbook may run automatically on the running release.
//
//	R1 a playbook that matches a healthy app is a hint, never automatic
//	R2 a repair restores what is healthy now: accepted source only while it is
//	   the running release, a literal only when the healthy app of the current
//	   release holds that literal
//	R3 a plan writes no more objects, and no other components, than the plan
//	   that was verified to recover users
//
// Nothing here calls an LLM or refuses an agent; a refusal is one line.
package staleness

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storeops"
)

// Cell is one literal write and the read that says whether the healthy app
// already holds the literal.
type Cell struct {
	// Keys identify the write, narrowest read first. A fact under any key answers it.
	Keys []string
	// Text is the write in one line.
	Text string
	// Check is the read of the same index as Keys: does the live app hold the literal?
	Check []func(program.ReadView) (bool, error)
}

// Facts are what one healthy reading of a release says, by cell key.
type Facts map[string]bool

// deref returns the value form of an action.
func deref(action program.Action) any {
	v := reflect.ValueOf(action)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

// Literals splits a plan's writes: cells whose healthy value can be read, and
// the writes whose value cannot be derived (a script, a SQL expression).
func Literals(plan program.Plan) (cells []Cell, opaque []string) {
	for _, action := range plan.Actions {
		switch a := deref(action).(type) {
		case program.RepairData:
			c, o := repairCells(a)
			cells, opaque = append(cells, c...), append(opaque, o...)
		case program.EnsureExec:
			cells = append(cells, observeCell(program.ExternalOperation{Exec: &a.Observe}, a.Expect))
		case program.EnsureHTTP:
			cells = append(cells, observeCell(program.ExternalOperation{HTTP: &a.Observe}, a.Expect))
		case program.SetField:
			if a.Value.Origin() == program.ParameterValue {
				opaque = append(opaque, "SetField "+a.Path+" from a parameter")
			}
		case program.PatchObject:
			if c, ok := objectCell(a.Ref, a.Patch); ok {
				cells = append(cells, c)
			} else {
				opaque = append(opaque, "PatchObject "+a.Ref.Kind+"/"+a.Ref.Name)
			}
		case program.ApplyObject:
			opaque = append(opaque, "ApplyObject")
		case program.RunScript, program.RunJob, program.Exec, program.KubernetesRequest, program.RestoreBackup, program.ProposeSourceChange:
			opaque = append(opaque, strings.TrimPrefix(fmt.Sprintf("%T", a), "program."))
		}
	}
	return cells, opaque
}

// FromSource names the plan's restores from accepted source.
func FromSource(plan program.Plan) []string {
	var out []string
	add := func(kind string, ref sdk.ObjectRef) { out = append(out, kind+" "+ref.Kind+"/"+ref.Name) }
	for _, action := range plan.Actions {
		switch a := deref(action).(type) {
		case program.ReconcileFromSource:
			add("ReconcileFromSource", a.Ref)
		case program.CreateFromSource:
			add("CreateFromSource", a.Ref)
		case program.SetField:
			if a.Value.Origin() == program.SourceValue {
				add("SetField", a.Ref)
			}
		case program.ScaleWorkload:
			if a.Replicas.Origin() == program.SourceValue {
				add("ScaleWorkload", a.Ref)
			}
		}
	}
	return out
}

func observeCell(o program.ExternalOperation, want int32) Cell {
	raw := ""
	if o.Exec != nil {
		// The Pod is a late binding; what is run in which container is the cell.
		raw = fmt.Sprintf("exec %s %q %q = %d", o.Exec.Container, o.Exec.Args, o.Exec.Stdin, want)
	} else {
		b, _ := json.Marshal(o.HTTP)
		raw = fmt.Sprintf("http %s = %d", b, want)
	}
	return Cell{Keys: []string{raw}, Text: "the plan's own Observe", Check: []func(program.ReadView) (bool, error){func(v program.ReadView) (bool, error) {
		p := o
		if o.Exec != nil {
			c := *o.Exec
			c.Pod = currentPod(v, c.Pod)
			p.Exec = &c
		}
		code, err := v.Observe(p)
		return code == want, err
	}}}
}

// currentPod is ref, or the running Pod of the same workload when ref is gone.
func currentPod(v program.ReadView, ref sdk.ObjectRef) sdk.ObjectRef {
	var found sdk.ObjectRef
	for _, pod := range v.Pods() {
		if pod.Name == ref.Name {
			return ref
		}
		if found.Name == "" && Component("Pod", pod.Name) == Component("Pod", ref.Name) {
			found = sdk.ObjectRefFrom("Pod", "v1", &pod)
		}
	}
	if found.Name != "" {
		return found
	}
	return ref
}

var (
	literal  = regexp.MustCompile(`^(?i:-?\d+(\.\d+)?|true|false|null|'(?:[^']|'')*')$`)
	clock    = regexp.MustCompile(`^(?i:now\(\)|current_timestamp(\(\))?)$`)
	column   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	conjunct = regexp.MustCompile(`(?i)\s+AND\s+`)
)

// assignments splits a SQL assignment list at its top-level commas.
func assignments(statement string) (out [][2]string, ok bool) {
	s := strings.TrimSpace(statement)
	if len(s) > 4 && strings.EqualFold(s[:4], "SET ") {
		s = s[4:]
	}
	depth, quoted, start := 0, false, 0
	var parts []string
	for i, r := range s {
		switch {
		case r == '\'':
			quoted = !quoted
		case quoted:
		case r == '(':
			depth++
		case r == ')':
			depth--
		case r == ',' && depth == 0:
			parts, start = append(parts, s[start:i]), i+1
		}
	}
	for _, p := range append(parts, s[start:]) {
		name, value, found := strings.Cut(p, "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if !found || !column.MatchString(name) {
			return nil, false
		}
		out = append(out, [2]string{name, value})
	}
	return out, len(out) > 0
}

// residual is the selector without the conditions that name a written column:
// those select the fault, the rest selects the rows. An OR, or a condition that
// cannot be split, leaves the whole table.
func residual(selector string, written []string) string {
	if regexp.MustCompile(`(?i)\bOR\b|\bBETWEEN\b|[()]`).MatchString(selector) {
		return ""
	}
	var keep []string
	for _, c := range conjunct.Split(strings.TrimSpace(selector), -1) {
		if c != "" && !slices.ContainsFunc(written, func(w string) bool { return regexp.MustCompile(`\b` + w + `\b`).MatchString(c) }) {
			keep = append(keep, c)
		}
	}
	return strings.Join(keep, " AND ")
}

func repairCells(a program.RepairData) (cells []Cell, opaque []string) {
	what := "RepairData " + a.Scope.Collection
	if a.Statement == "" || (a.Target.Engine != "postgres" && a.Target.Engine != "mysql") {
		return nil, []string{what + ": no literal"}
	}
	list, ok := assignments(a.Statement)
	if !ok {
		return nil, []string{what + ": " + a.Statement}
	}
	var written []string
	for _, p := range list {
		written = append(written, p[0])
	}
	for _, p := range list {
		col, value := p[0], p[1]
		if clock.MatchString(value) {
			continue // a bookkeeping timestamp is not a literal
		}
		if !literal.MatchString(value) {
			opaque = append(opaque, fmt.Sprintf("%s: %s = %s", what, col, value))
			continue
		}
		cell := Cell{Text: fmt.Sprintf("%s.%s = %s", a.Scope.Collection, col, value)}
		scopes := []string{""}
		if r := residual(a.Scope.Selector, written); r != "" {
			scopes = []string{r, ""}
		}
		for _, scope := range scopes {
			cell.Keys = append(cell.Keys, fmt.Sprintf("sql %s/%s.%s=%s where %s", a.Scope.Database, a.Scope.Collection, col, value, scope))
			cell.Check = append(cell.Check, func(v program.ReadView) (bool, error) { return holds(v, a, col, value, scope) })
		}
		cells = append(cells, cell)
	}
	return cells, opaque
}

// holds reports whether every row of the scope has the literal. An empty scope
// holds nothing: it proves no value.
func holds(v program.ReadView, a program.RepairData, col, value, scope string) (bool, error) {
	target := a.Target
	if t, err := program.StoreTargetFor(v, a.Target.Component); err == nil {
		target = t
	}
	count := func(selector string) (int, error) {
		o, err := v.Read(program.Query{Store: &program.StoreQuery{Target: target, Fields: []string{col},
			Scope: storeops.Scope{Database: a.Scope.Database, Collection: a.Scope.Collection, Selector: selector, Limit: 100000}}})
		return o.Count, err
	}
	same := col + " = " + value
	if strings.EqualFold(value, "null") {
		same = col + " IS NULL"
	}
	if scope != "" {
		same = scope + " AND " + same
	}
	all, err := count(scope)
	if err != nil {
		return false, err
	}
	matching, err := count(same)
	return all > 0 && all == matching, err
}

// objectCell checks that the live object already contains a merge patch.
func objectCell(ref sdk.ObjectRef, patch any) (Cell, bool) {
	raw, err := json.Marshal(patch)
	var want any
	if err != nil || json.Unmarshal(raw, &want) != nil {
		return Cell{}, false
	}
	if _, ok := want.(map[string]any); !ok {
		return Cell{}, false
	}
	return Cell{Keys: []string{"object " + ref.Kind + "/" + ref.Name + " " + string(raw)}, Text: "patch of " + ref.Kind + "/" + ref.Name,
		Check: []func(program.ReadView) (bool, error){func(v program.ReadView) (bool, error) {
			live, err := v.Object(ref)
			if err != nil || live == nil {
				return false, fmt.Errorf("object unavailable: %v", err)
			}
			var have any
			b, _ := json.Marshal(live.Object)
			_ = json.Unmarshal(b, &have)
			return contains(have, want), nil
		}}}, true
}

func contains(have, want any) bool {
	switch w := want.(type) {
	case nil:
		return have == nil
	case map[string]any:
		h, _ := have.(map[string]any)
		for k, x := range w {
			if strings.HasPrefix(k, "$") || k == "annotations" {
				continue
			}
			if !contains(h[k], x) {
				return false
			}
		}
		return true
	case []any:
		h, _ := have.([]any)
		for _, x := range w {
			if !slices.ContainsFunc(h, func(y any) bool { return contains(y, x) }) {
				return false
			}
		}
		return true
	}
	return reflect.DeepEqual(have, want)
}

// Check reads every cell of a verified plan on a healthy app.
func Check(view program.ReadView, plan program.Plan, into Facts) {
	cells, _ := Literals(plan)
	for _, cell := range cells {
		for i, key := range cell.Keys {
			if ok, err := cell.Check[i](view); err == nil {
				into[key] = ok
			}
		}
	}
}

var podSuffix = regexp.MustCompile(`(-[a-f0-9]{6,10}-[a-z0-9]{5}|-\d+)$`)

// Component is the kind and name a write belongs to: a Pod's is its workload.
func Component(kind, name string) string {
	switch kind {
	case "Pod":
		return "workload/" + podSuffix.ReplaceAllString(name, "")
	case "Deployment", "StatefulSet", "DaemonSet":
		return "workload/" + name
	case "ReplicaSet":
		return "workload/" + regexp.MustCompile(`-[a-f0-9]{6,10}$`).ReplaceAllString(name, "")
	}
	return kind + "/" + name
}

// Footprint is the sorted set of components a plan writes.
func Footprint(plan program.Plan) []string {
	set := map[string]bool{}
	add := func(ref sdk.ObjectRef) {
		if ref.Name != "" {
			set[Component(ref.Kind, ref.Name)] = true
		}
	}
	for _, action := range plan.Actions {
		switch a := deref(action).(type) {
		case program.RepairData:
			set["store/"+a.Target.Component+"/"+a.Scope.Collection] = true
		case program.InvalidateDerived:
			set["store/"+a.Target.Component] = true
		case program.RestoreStoreItem:
			set["store/"+a.Target.Component] = true
		case program.EnsureExec:
			add(a.Ensure.Pod)
		case program.DeleteObject:
			// A foreign object has a new name each time; its kind is what was proven.
			set["delete/"+a.Ref.Kind] = true
		case program.WaitFor, program.StoreBackup, program.Escalate:
		default:
			v := reflect.ValueOf(a)
			if v.Kind() == reflect.Struct {
				if f := v.FieldByName("Ref"); f.IsValid() {
					if ref, ok := f.Interface().(sdk.ObjectRef); ok {
						add(ref)
					}
				}
				if f := v.FieldByName("Pod"); f.IsValid() {
					if ref, ok := f.Interface().(sdk.ObjectRef); ok {
						add(ref)
					}
				}
			}
		}
	}
	out := []string{}
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Proof is what is known about one program revision.
type Proof struct {
	// Releases its plan (or the agent, in an incident it matched) recovered users on.
	Releases []string `json:"releases,omitempty"`
	// Footprint the verified plan wrote; nil is unknown.
	Footprint []string `json:"footprint"`
	// Hint is why the playbook is never automatic (R1a).
	Hint string `json:"hint,omitempty"`
}

// Release is what is known about the release that runs now.
type Release struct {
	Commit string `json:"commit"`
	// Unaccepted: the cluster runs a release that is not the accepted source.
	Unaccepted bool `json:"-"`
	// Checked programs were diagnosed on the healthy app of this release.
	Checked map[string]bool `json:"checked,omitempty"`
	// Demoted programs matched on the healthy app of this release.
	Demoted map[string]string `json:"demoted,omitempty"`
	Facts   Facts             `json:"facts,omitempty"`
}

// BeforePlan is R1: why a program is a hint on this release, "" when it is not.
func BeforePlan(id string, proof Proof, release Release) string {
	if proof.Hint != "" {
		return proof.Hint
	}
	if reason := release.Demoted[id]; reason != "" {
		return reason
	}
	return ""
}

// Refuse is R2 and R3: why this plan does not run automatically, "" when it does.
func Refuse(id string, plan program.Plan, proof Proof, release Release) string {
	if reason := R2a(plan, release); reason != "" {
		return reason
	}
	if reason := R2b(id, plan, proof, release); reason != "" {
		return reason
	}
	return R3(plan, proof)
}

// R2a refuses a restore from accepted source that is not the running release.
func R2a(plan program.Plan, release Release) string {
	if restores := FromSource(plan); len(restores) > 0 && release.Unaccepted {
		return "restores from accepted source, which is not the running release: " + restores[0]
	}
	return ""
}

// R2b refuses a literal the healthy app of this release does not hold, or
// whose healthy value is not known, until the program is proven on the release.
func R2b(id string, plan program.Plan, proof Proof, release Release) string {
	proven := slices.Contains(proof.Releases, release.Commit)
	if !proven && !release.Checked[id] {
		return "not yet checked on the healthy app of release " + release.Commit
	}
	cells, opaque := Literals(plan)
	for _, cell := range cells {
		known, same := false, false
		for _, key := range cell.Keys {
			fact, ok := release.Facts[key]
			known, same = known || ok, same || fact
		}
		switch {
		case known && !same:
			return "writes a value the healthy app of this release did not hold: " + cell.Text
		case !known && !proven:
			return "the healthy value on this release is not known: " + cell.Text
		}
	}
	if len(opaque) > 0 && !proven {
		return "writes a value SDO cannot read, not proven on this release: " + opaque[0]
	}
	return ""
}

// R3 refuses a plan wider than the verified one. An unknown proof refuses nothing.
func R3(plan program.Plan, proof Proof) string {
	if proof.Footprint == nil {
		return ""
	}
	wrote := Footprint(plan)
	if len(wrote) > len(proof.Footprint) {
		return fmt.Sprintf("writes %d objects, its verified plan wrote %d", len(wrote), len(proof.Footprint))
	}
	for _, c := range wrote {
		if !slices.Contains(proof.Footprint, c) {
			return "writes " + c + ", which its verified plan did not"
		}
	}
	return ""
}
