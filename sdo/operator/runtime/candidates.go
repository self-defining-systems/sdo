package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk/program"
)

// The candidate view: the app map is the index, so every row of evidence the
// responder can see is placed once, in Go, on the component it belongs to, and
// `sdo detect` and the first message show the components that could explain
// the failing user tests (or the opening Kubernetes condition), each with its
// own evidence. It is a reading order, never a verdict: nothing here touches
// opening, recovery or operability, and no node is ever removed because a
// passing operation shares it (a passing operation shows a node is alive, not
// that its data or code for the failing operation is right).
//
// The map comes from one source: the accepted `.sdo/arch.md` at the accepted
// commit, parsed once into a mapGraph (loadCandidateGraph). The probes' traffic
// descriptors fill in a test's path only when the map names no probe for it.

// candidateRenderCap bounds the candidate block of the first message.
const candidateRenderCap = 2048

const (
	// candidateConditionHops is how far upstream of an opening condition's
	// component the candidates reach; a user-test incident reaches one hop
	// upstream of its failing paths.
	candidateConditionHops = 2
	candidateUserHops      = 1
	candidateEvidenceLines = 3
	candidateElsewhereRows = 3
	candidateElsewhereName = 3
	candidateFullNodes     = 2
)

// testState is how a user test stands in the traffic window.
type testState int

const (
	// testPassing is judged healthy with no failed sample in the window.
	testPassing testState = iota
	// testUnevaluated has too few samples to judge and no failed one.
	testUnevaluated
	// testFailing is judged failing: the group the incident opened on.
	testFailing
	// testAlsoFailing is not judged failing (healthy or unevaluated) yet has a
	// failed sample in the window: a lower-confidence group.
	testAlsoFailing
)

// candidateTest is one user test as the candidate computation sees it.
type candidateTest struct {
	ID       string
	Symptom  string
	Path     []string // component names
	Source   string   // "map" or "descriptor"
	State    testState
	Failures int
	Samples  int
	// LatestOK is true when the newest sample passed.
	LatestOK bool
}

func (t candidateTest) failed() bool { return t.State == testFailing || t.State == testAlsoFailing }

// candidateEvidence is one line of evidence on a node.
type candidateEvidence struct {
	Kind        string // kubernetes, store, app-state, state, logs, clue, playbook, note
	At          time.Time
	Text        string
	Unexplained bool
	Routine     bool
	// Command is the command that restores or expands the evidence's object.
	Command string
}

// candidateNode is one component of the view.
type candidateNode struct {
	Name       string
	Component  program.AppComponent
	Class      string // a Position* class
	DataHolder bool
	Label      string
	OnK, OfN   int
	Hop        int
	Condition  bool // the opening condition's component
	Unmapped   bool // not a component of the map
	Serves     []string
	Evidence   []candidateEvidence
	// Hidden counts evidence lines past the cap of the node's own list.
	Hidden int
}

// candidateGroup is the failing tests that share a symptom.
type candidateGroup struct {
	Symptom string
	Tests   []candidateTest
}

// candidateInput is everything the view is computed from.
type candidateInput struct {
	Graph      *mapGraph // nil when there is no map
	MapNote    string    // why there is no graph
	MapLines   []string  // `map:` notes, such as a descriptor edge the map lacks
	Conditions []string  // components of the opening Kubernetes conditions
	Tests      []candidateTest
	Onset, Now time.Time
	Rows       []IncidentChange
	// Extra is evidence the rows do not carry (log errors, clues, playbooks).
	Extra map[string][]candidateEvidence
	// Observed maps a data holder to why it has no observed state; "" means
	// its state was read. A data holder absent from it has no reader.
	Observed map[string]string
	// SourceChecked is true when the accepted-source comparison ran, so a node
	// without a departure may say "no change since accepted".
	SourceChecked bool
	// Components resolve nodes the graph lacks (descriptor-only services).
	Components program.AppMap
}

// candidateView is the computed view.
type candidateView struct {
	in        candidateInput
	Groups    []candidateGroup
	Nodes     []*candidateNode // ranked
	elsewhere []IncidentChange
	unplaced  []IncidentChange
	partial   bool
	// otherClues are the summaries of clues that resolve to no candidate.
	otherClues []string
}

// position places a component against the failing and passing paths, also for
// a component the map does not hold (a descriptor service): membership alone
// then decides the class.
func (g *mapGraph) place(component string, failing, passing map[string][]string) MapPosition {
	if g != nil {
		if _, ok := g.comps[component]; ok {
			return g.position(component, failing, passing)
		}
	}
	through := func(paths map[string][]string) []string {
		var ids []string
		for id, path := range paths {
			if slices.Contains(path, component) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return ids
	}
	p := MapPosition{Failing: through(failing), Passing: through(passing), FailingHop: -1}
	switch {
	case len(p.Failing) > 0 && len(p.Passing) > 0:
		p.Class, p.FailingHop = PositionShared, 0
	case len(p.Failing) > 0:
		p.Class, p.FailingHop = PositionFailingOnly, 0
	default:
		p.Class = PositionUnplaced
	}
	return p
}

// computeCandidates is the shared map-position computation of `sdo detect` and
// the first message.
func computeCandidates(in candidateInput) candidateView {
	v := candidateView{in: in}
	failing := map[string][]string{}
	passing := map[string][]string{}
	bySymptom := map[string]*candidateGroup{}
	var symptoms []string
	for _, test := range in.Tests {
		switch {
		case test.failed():
			failing[test.ID] = test.Path
			group := bySymptom[test.Symptom]
			if group == nil {
				group = &candidateGroup{Symptom: test.Symptom}
				bySymptom[test.Symptom] = group
				symptoms = append(symptoms, test.Symptom)
			}
			group.Tests = append(group.Tests, test)
			if test.Failures < test.Samples {
				v.partial = true
			}
		case test.State == testPassing:
			// Only a fully passing, evaluated test with no failed sample exonerates liveness.
			passing[test.ID] = test.Path
		}
	}
	for _, symptom := range symptoms {
		group := bySymptom[symptom]
		// The group that opened the incident leads, unless its tests pass again
		// at the latest sample: then it prints last.
		sort.SliceStable(group.Tests, func(i, j int) bool { return rank(group.Tests[i]) < rank(group.Tests[j]) })
		v.Groups = append(v.Groups, *group)
	}
	sort.SliceStable(v.Groups, func(i, j int) bool { return rank(v.Groups[i].Tests[0]) < rank(v.Groups[j].Tests[0]) })

	conditionPaths := map[string][]string{}
	for _, name := range in.Conditions {
		conditionPaths["condition:"+name] = []string{name}
	}
	universe := map[string]bool{}
	if in.Graph != nil {
		for name := range in.Graph.comps {
			universe[name] = true
		}
	}
	for _, paths := range []map[string][]string{failing, conditionPaths} {
		for _, path := range paths {
			for _, name := range path {
				universe[name] = true
			}
		}
	}
	for name := range universe {
		user := in.Graph.place(name, failing, passing)
		condition := in.Graph.place(name, conditionPaths, passing)
		node := &candidateNode{Name: name, Class: user.Class, Hop: user.FailingHop, OfN: len(failing), OnK: len(user.Failing), Serves: user.Passing, Condition: slices.Contains(in.Conditions, name)}
		switch {
		case user.Class == PositionFailingOnly || user.Class == PositionShared:
		case user.Class == PositionDependency && user.FailingHop <= candidateUserHops:
		case condition.Class == PositionFailingOnly || condition.Class == PositionShared:
			node.Class, node.Hop, node.Serves = condition.Class, 0, condition.Passing
		case condition.Class == PositionDependency && condition.FailingHop <= candidateConditionHops:
			node.Class, node.Hop, node.Serves = condition.Class, condition.FailingHop, condition.Passing
		default:
			continue
		}
		node.Component = program.AppComponent{Name: name}
		if in.Graph != nil {
			if c, ok := in.Graph.comps[name]; ok {
				node.Component = program.AppComponent{Name: c.Name, Workload: c.Workload, Services: c.Services, HoldsData: c.HoldsData}
				node.DataHolder = c.HoldsData || c.Data != nil
				node.Label = in.Graph.label(name)
			} else {
				node.Unmapped = true
			}
		}
		if c, ok := in.Components.Component(name); ok && node.Component.Workload == "" {
			node.Component = c
			node.DataHolder = node.DataHolder || c.HoldsData
		}
		if node.Label == "" {
			node.Label = name
		}
		v.Nodes = append(v.Nodes, node)
	}

	byName := map[string]*candidateNode{}
	for _, node := range v.Nodes {
		byName[node.Name] = node
	}
	for _, row := range in.Rows {
		if row.Noisy {
			continue
		}
		if node := byName[row.Component]; node != nil {
			if e, ok := rowEvidence(row, in.Onset); ok {
				node.Evidence = append(node.Evidence, e)
			}
			continue
		}
		if row.Component == "" || in.Graph != nil && len(in.Graph.comps) > 0 && !in.Graph.hasComponent(row.Component) {
			v.unplaced = append(v.unplaced, row)
		} else {
			v.elsewhere = append(v.elsewhere, row)
		}
	}
	for _, node := range v.Nodes {
		node.Evidence = append(node.Evidence, in.Extra[node.Name]...)
		node.sortEvidence(in.Onset)
		if len(node.Evidence) == 0 {
			if node.Component.Workload != "" && in.SourceChecked {
				node.Evidence = append(node.Evidence, candidateEvidence{Kind: "note", Text: "no change since accepted source"})
			}
			if reason, ok := in.Observed[node.Name]; ok && reason == "" && node.DataHolder {
				node.Evidence = append(node.Evidence, candidateEvidence{Kind: "note", Text: "state read ok, no difference from last healthy"})
			}
		}
	}
	sort.SliceStable(v.Nodes, func(i, j int) bool { return v.less(v.Nodes[i], v.Nodes[j]) })
	return v
}

func rank(t candidateTest) int {
	r := 0
	if t.State == testAlsoFailing {
		r += 2
	}
	if t.LatestOK {
		r++
	}
	return r
}

func (g *mapGraph) hasComponent(name string) bool {
	if g == nil {
		return false
	}
	_, ok := g.comps[name]
	return ok
}

// evidenceTier orders a node by its best evidence: unexplained first (a state
// difference against last healthy, a departure no rollout explains, new log
// errors, a clue), then routine rollout or churn writes, then none.
func (n *candidateNode) evidenceTier() int {
	tier := 2
	for _, e := range n.Evidence {
		switch {
		case e.Kind == "note" || e.Kind == "playbook":
		case e.Unexplained:
			return 0
		default:
			tier = 1
		}
	}
	return tier
}

// nearest is the distance of the node's nearest timed evidence to onset.
func (n *candidateNode) nearest(onset time.Time) time.Duration {
	best := time.Duration(1<<62 - 1)
	for _, e := range n.Evidence {
		if e.At.IsZero() || e.Kind == "note" {
			continue
		}
		d := e.At.Sub(onset)
		if d < 0 {
			d = -d
		}
		best = min(best, d)
	}
	return best
}

// less is the documented ordering, lexicographic and never a score: evidence
// tier, then the nearest timed evidence to onset, then position (on a failing
// path only, then shared, then dependency), data holders before stateless
// nodes, the more failing paths first, and the name.
func (v candidateView) less(a, b *candidateNode) bool {
	if x, y := a.evidenceTier(), b.evidenceTier(); x != y {
		return x < y
	}
	if x, y := a.nearest(v.in.Onset), b.nearest(v.in.Onset); x != y {
		return x < y
	}
	classRank := func(n *candidateNode) int {
		switch n.Class {
		case PositionFailingOnly:
			return 0
		case PositionShared:
			return 1
		}
		return 2
	}
	if x, y := classRank(a), classRank(b); x != y {
		return x < y
	}
	if a.DataHolder != b.DataHolder {
		return a.DataHolder
	}
	if a.OnK != b.OnK {
		return a.OnK > b.OnK
	}
	if a.Hop != b.Hop {
		return a.Hop < b.Hop
	}
	return a.Name < b.Name
}

// sortEvidence puts unexplained evidence first, routine churn after it; within
// a class timed evidence comes nearest onset first, untimed after.
func (n *candidateNode) sortEvidence(onset time.Time) {
	class := func(e candidateEvidence) int {
		switch {
		case e.Kind == "playbook":
			return 3
		case e.Kind == "note":
			return 2
		case e.Unexplained:
			return 0
		}
		return 1
	}
	dist := func(e candidateEvidence) time.Duration {
		if e.At.IsZero() {
			return time.Duration(1<<62 - 1)
		}
		d := e.At.Sub(onset)
		if d < 0 {
			d = -d
		}
		return d
	}
	sort.SliceStable(n.Evidence, func(i, j int) bool {
		a, b := n.Evidence[i], n.Evidence[j]
		if x, y := class(a), class(b); x != y {
			return x < y
		}
		return dist(a) < dist(b)
	})
}

// routineRolloutPaths are field paths a rollout or autoscaler rewrites as a
// matter of course.
var routineRolloutPaths = []string{"annotations", "restartedat", "replicas", "generation", "revision"}

// routineChange reports a departure that is a rollout or churn write rather
// than an unexplained difference.
func routineChange(row IncidentChange) bool {
	if row.Source != "kubernetes" || row.After == string(DepartureMissing) || row.After == string(DepartureExtra) {
		return false
	}
	if strings.Contains(strings.ToLower(row.Writer), "rollout") {
		return true
	}
	if row.Key == "" {
		return false
	}
	for _, field := range strings.Split(row.Key, ";") {
		lower := strings.ToLower(field)
		if !slices.ContainsFunc(routineRolloutPaths, func(p string) bool { return strings.Contains(lower, p) }) {
			return false
		}
	}
	return true
}

// rowEvidence renders one change row as evidence. A store row has no change
// time: it shows when it was read, never an offset from onset.
func rowEvidence(row IncidentChange, onset time.Time) (candidateEvidence, bool) {
	row = maskCredentialValues(row)
	clean := func(text string) string { return redactDetectText(text, incidentInlineValue) }
	// An object name is not a secret: the token redaction would mask a long one.
	object := plainName(row.ReaderOrObject, 70)
	var body string
	switch row.Source {
	case "kubernetes":
		switch row.After {
		case string(DepartureExtra):
			body = object + " present, absent from accepted source"
		case string(DepartureMissing):
			body = object + " missing, accepted source declares it"
		default:
			body = object + " " + clean(row.After) + " vs accepted source"
		}
		if row.Key != "" {
			body += " (" + clean(row.Key) + ")"
		}
	default:
		body = row.Source + " " + object
		if row.Key != "" {
			body += " " + clean(row.Key)
		}
		if row.Before != "" || row.After != "" {
			body += ": " + orNone(clean(row.Before)) + " → " + orNone(clean(row.After))
		}
	}
	if row.Writer != "" {
		body += " by " + clean(row.Writer)
	}
	e := candidateEvidence{Kind: row.Source, At: row.At, Unexplained: true, Command: row.Command}
	switch {
	case !row.At.IsZero():
		body = relativeOffset(row.At.Sub(onset)) + " " + body
	case !row.ReadAt.IsZero():
		body += fmt.Sprintf("  (read %s, change untimed)", hms(row.ReadAt))
	default:
		body += "  (untimed)"
	}
	if row.FirstSeen {
		e.Unexplained, e.Routine = false, true
		body += " (not in the last healthy read)"
	}
	if routineChange(row) {
		e.Unexplained, e.Routine = false, true
		body += " (rollout churn)"
	}
	e.Text = body
	return e, true
}

// plainName bounds a Kind/name without credential redaction.
func plainName(name string, limit int) string {
	name = strings.Join(strings.Fields(name), " ")
	if runes := []rune(name); len(runes) > limit {
		return string(runes[:limit-1]) + "…"
	}
	return name
}

func rows(n int) string {
	if n == 1 {
		return "1 row"
	}
	return fmt.Sprintf("%d rows", n)
}

// tags are the node's position and exoneration labels.
func (n *candidateNode) tags() string {
	var tags []string
	switch {
	case n.Condition && n.Class != PositionDependency:
		tags = append(tags, "the opening condition's component")
	case n.Class == PositionDependency:
		hops := "1 hop"
		if n.Hop > 1 {
			hops = fmt.Sprintf("%d hops", n.Hop)
		}
		tags = append(tags, hops+" upstream")
	case n.OfN > 0 && n.OnK == n.OfN:
		tags = append(tags, "on every failing path")
	case n.OfN > 0:
		tags = append(tags, fmt.Sprintf("on %d of %d failing paths", n.OnK, n.OfN))
	}
	if n.Unmapped {
		tags = append(tags, "not in the map")
	}
	if len(n.Serves) > 0 {
		tags = append(tags, "serves passing ops")
	}
	return strings.Join(tags, "; ")
}

func (n *candidateNode) title() string {
	if tags := n.tags(); tags != "" {
		return n.Label + "  " + tags
	}
	return n.Label
}

// nodeLines renders a node: its title, up to evidenceLines of evidence and a
// drill-down, or just the title and the evidence count when collapsed.
func (n *candidateNode) nodeLines(full bool, evidenceLines int) []string {
	if !full {
		line := n.title()
		if len(n.Evidence) > 0 {
			line += fmt.Sprintf("  +%d evidence", len(n.Evidence))
		}
		return []string{line + "  more: sdo changes --component " + n.Name}
	}
	lines := []string{n.title()}
	shown := 0
	for _, e := range n.Evidence {
		if e.Kind == "playbook" || shown < evidenceLines {
			lines = append(lines, "  "+e.Text)
			if e.Command != "" {
				lines = append(lines, "    more: "+e.Command)
			}
			if e.Kind != "playbook" {
				shown++
			}
		}
	}
	if hidden := len(n.Evidence) - shown; hidden > 0 && shown > 0 && n.Evidence[len(n.Evidence)-1].Kind != "playbook" {
		lines = append(lines, fmt.Sprintf("  +%d more evidence lines", hidden))
	}
	return append(lines, "    more: sdo changes --component "+n.Name+"; sdo timeline --component "+n.Name)
}

// header is the CANDIDATES line; it claims "every failing path" only when a
// user test failed.
func (v candidateView) header() string {
	if len(v.Groups) == 0 {
		if len(v.Nodes) == 0 {
			return ""
		}
		return fmt.Sprintf("CANDIDATES (no failing user test: the opening component and what it depends on, %d hops; an evidence ordering, not a narrowing)", candidateConditionHops)
	}
	return "CANDIDATES (on every failing path unless marked k of n, plus one hop upstream; none cleared by passing ops; unexplained evidence first)"
}

// groupsLine names every failing test with its group and, when there is more
// than one group, a test judged only by a failed sample, or a partial failure,
// how much of it fails.
func (v candidateView) groupsLine() string {
	also := false
	for _, g := range v.Groups {
		for _, t := range g.Tests {
			also = also || t.State == testAlsoFailing
		}
	}
	if len(v.Groups) == 0 || len(v.Groups) == 1 && !also && !v.partial {
		return ""
	}
	var parts []string
	for _, g := range v.Groups {
		for _, t := range g.Tests {
			role := "opened on"
			if t.State == testAlsoFailing {
				role = "also failing"
			}
			if t.LatestOK {
				role += ", passes again"
			}
			parts = append(parts, fmt.Sprintf("%s (%s, %s, %d/%d)", redactDetectText(t.ID, 40), role, redactDetectText(g.Symptom, 24), t.Failures, t.Samples))
		}
	}
	prefix := "failing:"
	if len(v.Groups) > 1 {
		prefix = fmt.Sprintf("%d symptom groups, possibly separate causes:", len(v.Groups))
	}
	return redactDetectText(prefix+" "+strings.Join(parts, "; "), 300)
}

func (v candidateView) notObserved() string {
	g := v.in.Graph
	if g == nil {
		return ""
	}
	var parts []string
	for _, c := range g.m.Components {
		if !c.HoldsData && c.Data == nil {
			continue
		}
		reason, known := v.in.Observed[c.Name]
		if known && reason == "" {
			continue
		}
		if !known {
			reason = "no state reader"
		}
		parts = append(parts, fmt.Sprintf("%s [data] %s", c.Name, redactDetectText(reason, 80)))
	}
	if len(parts) == 0 {
		return ""
	}
	return "not observed: " + strings.Join(parts, "; ")
}

// elsewhereLines name every off-path component with a row (up to three) and
// the newest three rows anywhere in it, so a cause on a missing edge is never
// hidden; rows on objects the map does not place are counted apart.
func (v candidateView) elsewhereLines(detail bool) []string {
	var out []string
	if len(v.elsewhere) > 0 {
		timed := slices.Clone(v.elsewhere)
		sort.SliceStable(timed, func(i, j int) bool {
			a, b := timed[i], timed[j]
			if a.At.IsZero() != b.At.IsZero() {
				return !a.At.IsZero()
			}
			return a.At.After(b.At)
		})
		var names []string
		for _, row := range timed {
			if !slices.Contains(names, row.Component) {
				names = append(names, row.Component)
			}
		}
		shown := names
		more := ""
		if len(names) > candidateElsewhereName {
			shown = names[:candidateElsewhereName]
			more = fmt.Sprintf(", +%d", len(names)-candidateElsewhereName)
		}
		out = append(out, fmt.Sprintf("elsewhere: %s on %d components (%s%s)", rows(len(v.elsewhere)), len(names), redactDetectText(strings.Join(shown, ", "), 120), more))
		if detail {
			var newest []string
			for _, row := range timed {
				if row.At.IsZero() || len(newest) == candidateElsewhereRows {
					break
				}
				newest = append(newest, fmt.Sprintf("%s %s %s", relativeOffset(row.At.Sub(v.in.Onset)), redactDetectText(row.Component, 30), redactDetectText(row.ReaderOrObject, 40)))
			}
			if len(newest) > 0 {
				out = append(out, "  newest: "+strings.Join(newest, "; "), "  more: sdo changes")
			}
		}
	}
	if len(v.unplaced) > 0 {
		out = append(out, fmt.Sprintf("unplaced: %s on objects the map does not place", rows(len(v.unplaced))))
	}
	return out
}

// render prints the candidate block within cap bytes. The cap collapses the
// lowest-ranked nodes first (to a title and an evidence count), then the
// elsewhere detail, then evidence lines, then trailing node titles; the top
// two nodes keep their evidence longest.
func (v candidateView) render(cap int) string {
	header := v.header()
	if header == "" && len(v.in.MapLines) == 0 && v.in.MapNote == "" && len(v.elsewhere)+len(v.unplaced) == 0 && v.notObserved() == "" {
		return ""
	}
	full, shown, evidenceLines, detail := len(v.Nodes), len(v.Nodes), candidateEvidenceLines, true
	serves := false
	for _, n := range v.Nodes {
		serves = serves || len(n.Serves) > 0
	}
	assemble := func() string {
		var out []string
		if line := v.groupsLine(); line != "" {
			out = append(out, line)
		}
		if v.in.Graph == nil {
			note := v.in.MapNote
			if note == "" {
				note = "no application map"
			}
			out = append(out, "map: unavailable ("+redactDetectText(note, 110)+"); paths come from the traffic descriptors, no upstream or dependency candidates")
		}
		out = append(out, v.in.MapLines...)
		if header != "" {
			out = append(out, header)
		}
		for index, n := range v.Nodes[:shown] {
			out = append(out, n.nodeLines(index < full, evidenceLines)...)
		}
		if dropped := len(v.Nodes) - shown; dropped > 0 {
			out = append(out, fmt.Sprintf("+%d more candidates: sdo detect --finding candidates", dropped))
		}
		if serves {
			out = append(out, "serves passing ops: alive for them, not shown right for the failing operation")
		}
		out = append(out, v.elsewhereLines(detail)...)
		if n := len(v.otherClues); n > 0 {
			first := ""
			if detail {
				first = fmt.Sprintf(", first %q", v.otherClues[0])
			}
			out = append(out, fmt.Sprintf("clues elsewhere: %d%s  more: sdo detect --finding background", n, first))
		}
		if line := v.notObserved(); line != "" {
			out = append(out, line)
		}
		if v.in.Graph != nil && len(v.Nodes) > 0 {
			out = append(out, "more: sdo map near "+v.Nodes[0].Name)
		}
		return strings.Join(out, "\n")
	}
	for len(assemble()) > cap {
		switch {
		case full > candidateFullNodes:
			full--
		case detail:
			detail = false
		case evidenceLines > 1:
			evidenceLines--
		case shown > full && shown > 1:
			shown--
		case full > 1:
			full--
			shown = max(shown, full)
		default:
			text := assemble()
			return text[:cap-3] + "…"
		}
	}
	return assemble()
}

// items are the drill-down texts of `sdo detect --finding NODE`: every node's
// whole evidence, uncapped.
func (v candidateView) items() map[string]string {
	items := map[string]string{}
	var all []string
	for _, n := range v.Nodes {
		lines := []string{n.title()}
		for _, e := range n.Evidence {
			lines = append(lines, "  "+e.Text)
		}
		items[n.Name] = strings.Join(lines, "\n")
		all = append(all, n.title())
	}
	items["candidates"] = strings.Join(all, "\n")
	return items
}
