package plan

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// PreviewNode is one node of the expanded plan in run order, with the experiment name its first attempt would get.
type PreviewNode struct {
	ID   string `json:"id"`
	Arm  string `json:"arm"`
	Item string `json:"item"`
	Rep  int    `json:"rep"`
	Name string `json:"name"`
}

// Preview is what `lanes plan add --dry-run` prints: the plan as `plan add` would store it.
type Preview struct {
	Plan     string            `json:"plan"`
	Pins     Pins              `json:"pins"`
	Order    string            `json:"order"`
	Reps     int               `json:"reps"`
	Parallel int               `json:"parallel"`
	Arms     []string          `json:"arms"`
	Items    []string          `json:"items"`
	Groups   map[string]string `json:"groups,omitempty"` // item -> group, for items that name one
	Edges    []string          `json:"edges,omitempty"`
	Steps    []string          `json:"steps,omitempty"`
	Nodes    []PreviewNode     `json:"nodes"`
}

// DryRun validates a plan file exactly as Add does (parse, items, templates, every rendered spec through validate)
// and returns the expansion, without creating anything under plansRoot: the work happens in a scratch directory that
// is removed before it returns. A plan name already taken is refused, as Add refuses it.
func DryRun(plansRoot, file string, clock Clock, validate func(specJSON []byte) error, opts ...Option) (*Preview, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	p, err := Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", file, err)
	}
	if dir := Dir(plansRoot, p.Name); dirExists(dir) {
		return nil, fmt.Errorf("plan %s exists (%s): plan names are never reused", p.Name, dir)
	}
	scratch, err := os.MkdirTemp("", "lanes-plan-dry-run-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	if _, err := Add(scratch, file, clock, validate, opts...); err != nil {
		return nil, err
	}
	e, err := Open(Dir(scratch, p.Name), nil, clock)
	if err != nil {
		return nil, err
	}
	nodes, err := e.Plan.Expand(e.items)
	if err != nil {
		return nil, err
	}
	arms, _ := e.Plan.armOrder()
	pv := &Preview{Plan: e.Plan.Name, Pins: e.Plan.Pins, Order: e.Plan.Order, Reps: e.Plan.Reps, Parallel: e.Plan.Parallel, Arms: arms}
	for _, it := range e.items {
		pv.Items = append(pv.Items, it.ID)
		if it.Group != "" {
			if pv.Groups == nil {
				pv.Groups = map[string]string{}
			}
			pv.Groups[it.ID] = it.Group
		}
	}
	for _, ed := range e.Plan.Edges {
		s := ed.From + " -> " + ed.To
		if ed.Artifact != "" {
			s += " (" + ed.Artifact + ")"
		}
		pv.Edges = append(pv.Edges, s)
	}
	for _, st := range e.Plan.Steps {
		pv.Steps = append(pv.Steps, fmt.Sprintf("%s after %s (per %s)", st.Name, strings.Join(st.After, ","), st.Per))
	}
	for _, n := range nodes {
		pv.Nodes = append(pv.Nodes, PreviewNode{ID: n.ID, Arm: n.Arm, Item: n.Item, Rep: n.Rep, Name: ExpName(e.Plan.Name, n.ID, 1)})
	}
	return pv, nil
}

func dirExists(dir string) bool {
	st, err := os.Stat(filepath.Clean(dir))
	return err == nil && st.IsDir()
}

// Render prints the preview as text: the summary, then every node in run order.
func (pv *Preview) Render(w io.Writer) {
	fmt.Fprintf(w, "plan %s (dry run, nothing stored): %d nodes = %d items x arms [%s] x %d rep(s), %s, parallel %d\n", pv.Plan,
		len(pv.Nodes), len(pv.Items), strings.Join(pv.Arms, ", "), pv.Reps, pv.Order, pv.Parallel)
	fmt.Fprintf(w, "workload %s, subject %s\n", pv.Pins.Workload, pv.Pins.Subject)
	perArm := map[string]int{}
	for _, n := range pv.Nodes {
		perArm[n.Arm]++
	}
	for _, a := range pv.Arms {
		fmt.Fprintf(w, "arm %s: %d nodes\n", a, perArm[a])
	}
	perGroup := map[string]int{}
	for _, it := range pv.Items {
		perGroup[pv.Groups[it]]++
	}
	for _, g := range sortedKeys(perGroup) {
		name := g
		if name == "" {
			name = "(no group)"
		}
		fmt.Fprintf(w, "group %s: %d items\n", name, perGroup[g])
	}
	for _, e := range pv.Edges {
		fmt.Fprintf(w, "edge %s\n", e)
	}
	for _, s := range pv.Steps {
		fmt.Fprintf(w, "step %s\n", s)
	}
	fmt.Fprintln(w, "order:")
	for i, n := range pv.Nodes {
		g := ""
		if pv.Groups[n.Item] != "" {
			g = "  [" + pv.Groups[n.Item] + "]"
		}
		fmt.Fprintf(w, "%4d  %-28s %s%s\n", i+1, n.ID, n.Name, g)
	}
}
