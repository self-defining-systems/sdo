package operability

import (
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand"
	"reflect"
	"strings"
	"testing"
)

func subject() Subject {
	return Subject{
		AcceptedCommit: "c0", BaseTree: "t-base", EffectiveTree: "t-eff",
		PatchesDigest: "p0", RenderDigest: "r0", EnvDigest: "e0", DiagnosticsTree: "t-diag",
	}
}

func chk(id string, st Status, cause Cause) Check {
	return Check{ID: id, Phase: PhaseOffline, Status: st, Cause: cause}
}

func report(t *testing.T, stage Stage, checks ...Check) Report {
	t.Helper()
	r, err := New(stage, subject()).With(checks...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func allPass(t *testing.T) Report {
	t.Helper()
	var cs []Check
	for _, id := range AllItems() {
		cs = append(cs, chk(string(id)+".ok", StatusPass, ""))
	}
	return report(t, StageComplete, cs...)
}

func TestDecideTable(t *testing.T) {
	cases := []struct {
		name   string
		checks []Check
		req    []ItemID
		kind   DecisionKind
		owner  Owner
	}{
		{"all pass", nil, AllItems(), Accepted, ""},
		{"note and skipped never block", []Check{chk("O3.t", StatusNote, ""), chk("O3.s", StatusSkipped, "")}, AllItems(), Accepted, ""},
		{"fail routes to owner", []Check{chk("O1.render", StatusFail, CauseRender)}, AllItems(), Correct, OwnerDeployer},
		{"designer cause", []Check{chk("O3.cov", StatusFail, CauseCoverage)}, AllItems(), Correct, OwnerDesigner},
		{"pending waits", []Check{chk("O4.d", StatusPending, CauseProbeUnavailable)}, AllItems(), Wait, ""},
		{"fail beats pending", []Check{chk("O4.d", StatusPending, CauseProbeUnavailable), chk("O1.r", StatusFail, CausePatch)}, AllItems(), Correct, OwnerDeployer},
		{"infrastructure beats everything", []Check{chk("O1.r", StatusFail, CausePatch), chk("O3.c", StatusFail, CauseCoverage), chk("O2.i", StatusFail, CauseInfrastructure)}, AllItems(), Retry, OwnerRetry},
		{"internal beats deployer", []Check{chk("O1.r", StatusFail, CausePatch), chk("O5.m", StatusFail, CauseInternal)}, AllItems(), Hold, OwnerHuman},
		{"deployer beats designer", []Check{chk("O3.c", StatusFail, CauseCoverage), chk("O1.r", StatusFail, CauseRollout)}, AllItems(), Correct, OwnerDeployer},
		{"attribution is the operator", []Check{chk("O2.a", StatusFail, CauseAttribution)}, AllItems(), Hold, OwnerOperator},
		{"unrequired failing item ignored", []Check{chk("O4.d", StatusFail, CauseDetectorFault)}, StageItems(StageDeployment), Accepted, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := allPass(t)
			r, err := r.With(tc.checks...)
			if err != nil {
				t.Fatal(err)
			}
			d := r.Decide(tc.req...)
			if d.Kind != tc.kind || d.Owner != tc.owner {
				t.Fatalf("Decide = %s/%s, want %s/%s", d.Kind, d.Owner, tc.kind, tc.owner)
			}
		})
	}
}

func TestSilenceIsNotAPass(t *testing.T) {
	r := report(t, StageDeployment, chk("O1.ok", StatusPass, ""), chk("O2.ok", StatusPass, ""))
	d := r.Decide(StageItems(StageDeployment)...)
	if d.Kind != Hold || d.Owner != OwnerHuman || len(d.Checks) != 1 || d.Checks[0].ID != "O6.present" {
		t.Fatalf("empty required item must fail closed: %+v", d)
	}
	if got := r.Decide(O1, O2); got.Kind != Accepted {
		t.Fatalf("O1, O2 required only: %+v", got)
	}
	if got := r.Decide(O4); got.Kind != Hold {
		t.Fatalf("missing item accepted: %+v", got)
	}
}

func TestDecideOwnsOnlyItsChecksAndFeedback(t *testing.T) {
	r := allPass(t)
	r, _ = r.With(
		Check{ID: "O1.render", Phase: PhaseOffline, Status: StatusFail, Cause: CauseRender, Detail: "template failed", Paths: []string{"a.yaml"}},
		chk("O3.cov", StatusFail, CauseCoverage),
	)
	d := r.Decide(AllItems()...)
	if len(d.Checks) != 1 || d.Checks[0].ID != "O1.render" {
		t.Fatalf("decision checks = %+v", d.Checks)
	}
	if got := r.Feedback(OwnerDeployer); got != "O1.render [render]: template failed (a.yaml)\n" {
		t.Fatalf("deployer feedback = %q", got)
	}
	if got := r.Feedback(OwnerDesigner); !strings.HasPrefix(got, "O3.cov [coverage]") {
		t.Fatalf("designer feedback = %q", got)
	}
	if got := r.Feedback(OwnerResponder); got != "" {
		t.Fatalf("responder feedback = %q", got)
	}
}

// Adding a failing or pending check to a report never makes it more accepted.
func TestDecideMonotone(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	statuses := []Status{StatusPass, StatusFail, StatusNote, StatusPending, StatusSkipped}
	causes := Causes()
	rank := func(k DecisionKind) int {
		if k == Accepted {
			return 0
		}
		return 1
	}
	for n := 0; n < 2000; n++ {
		r := New(StageComplete, subject())
		for i := 0; i < rng.Intn(8); i++ {
			st := statuses[rng.Intn(len(statuses))]
			c := chk(string(AllItems()[rng.Intn(6)])+"."+string(rune('a'+i)), st, "")
			if st == StatusFail || st == StatusPending {
				c.Cause = causes[rng.Intn(len(causes))]
			}
			r, _ = r.With(c)
		}
		before := r.Decide(AllItems()...)
		bad := chk(string(AllItems()[rng.Intn(6)])+".zz", StatusFail, causes[rng.Intn(len(causes))])
		after, _ := r.With(bad)
		if got := after.Decide(AllItems()...); rank(got.Kind) < rank(before.Kind) {
			t.Fatalf("adding a failing check accepted: before %+v after %+v", before, got)
		}
		if got := after.Decide(AllItems()...); got.Kind == Accepted || got.Kind == Wait {
			t.Fatalf("a failing required check left %s", got.Kind)
		}
	}
}

func TestRouteTable(t *testing.T) {
	type row struct{ fresh, provided, advance Owner }
	want := map[Cause]row{
		CausePatch:                  {OwnerDeployer, OwnerHuman, OwnerDeployer},
		CauseRender:                 {OwnerDeployer, OwnerHuman, OwnerDeployer},
		CauseRenderNondeterministic: {OwnerDeployer, OwnerHuman, OwnerDeployer},
		CauseBuild:                  {OwnerDeployer, OwnerHuman, OwnerDeployer},
		CauseRollout:                {OwnerDeployer, OwnerHuman, OwnerResponder},
		CauseScope:                  {OwnerDeployer, OwnerHuman, OwnerHuman},
		CauseAllowlist:              {OwnerDeployer, OwnerHuman, OwnerHuman},
		CauseBudget:                 {OwnerDeployer, OwnerHuman, OwnerHuman},
		CauseAttribution:            {OwnerOperator, OwnerOperator, OwnerOperator},
		CauseBinding:                {OwnerDesigner, OwnerDesigner, OwnerDesigner},
		CauseCoverage:               {OwnerDesigner, OwnerDesigner, OwnerDesigner},
		CauseMemoryInvalid:          {OwnerDesigner, OwnerDesigner, OwnerDesigner},
		CauseProbeUnavailable:       {OwnerDesigner, OwnerDesigner, OwnerDesigner},
		CauseDetectorFault:          {OwnerDesigner, OwnerDesigner, OwnerDesigner},
		CauseSourceFault:            {OwnerDeployer, OwnerHuman, OwnerResponder},
		CauseInfrastructure:         {OwnerRetry, OwnerRetry, OwnerRetry},
		CauseEnvironment:            {OwnerHuman, OwnerHuman, OwnerHuman},
		CauseInternal:               {OwnerHuman, OwnerHuman, OwnerHuman},
		CauseAgentNotConverging:     {OwnerHuman, OwnerHuman, OwnerHuman},
		CauseBaseInvalid:            {OwnerOperator, OwnerOperator, OwnerOperator},
	}
	for _, c := range Causes() {
		w, ok := want[c]
		if !ok {
			t.Fatalf("cause %s has no expected row", c)
		}
		got := row{RouteIn(ModeFresh, c), RouteIn(ModeProvided, c), RouteIn(ModeAdvance, c)}
		if got != w || Route(c) != w.fresh {
			t.Errorf("%s routes %+v, want %+v", c, got, w)
		}
	}
	if len(want) != len(Causes()) {
		t.Fatalf("table has %d rows for %d causes", len(want), len(Causes()))
	}
}

func TestUnknownCauseModeFailClosed(t *testing.T) {
	if Route("made_up") != OwnerHuman || RouteIn("elsewhere", CausePatch) != OwnerHuman {
		t.Fatal("unknown cause or mode must route to the human")
	}
	r := allPass(t)
	r.Items[0].Checks = append(r.Items[0].Checks, chk("O1.x", StatusFail, "made_up"))
	if d := r.Decide(O1); d.Kind != Hold || d.Owner != OwnerHuman {
		t.Fatalf("unknown cause decision = %+v", d)
	}
	r.Items[0].Checks[len(r.Items[0].Checks)-1].Status = "weird"
	if d := r.Decide(O1); d.Kind == Accepted {
		t.Fatal("unknown status accepted")
	}
}

func TestProvidedModeNeverCorrectsTheDeployer(t *testing.T) {
	r := allPass(t)
	r, _ = r.With(chk("O1.r", StatusFail, CauseRender), chk("O3.c", StatusFail, CauseCoverage))
	if d := r.DecideIn(ModeProvided, AllItems()...); d.Kind != Hold || d.Owner != OwnerHuman {
		t.Fatalf("provided render failure = %+v", d)
	}
	r = allPass(t)
	r, _ = r.With(chk("O3.c", StatusFail, CauseCoverage))
	if d := r.DecideIn(ModeProvided, AllItems()...); d.Kind != Correct || d.Owner != OwnerDesigner {
		t.Fatalf("provided designer failure = %+v", d)
	}
}

// Causes() must list exactly the Cause constants declared in route.go.
func TestCausesListsEveryConstant(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "route.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	declared := map[string]bool{}
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}
		for _, s := range gd.Specs {
			vs := s.(*ast.ValueSpec)
			if id, ok := vs.Type.(*ast.Ident); ok && id.Name == "Cause" {
				for _, v := range vs.Values {
					declared[strings.Trim(v.(*ast.BasicLit).Value, `"`)] = true
				}
			}
		}
	}
	listed := map[string]int{}
	for _, c := range Causes() {
		listed[string(c)]++
		if !c.known() {
			t.Errorf("cause %s is not routed", c)
		}
	}
	for c := range declared {
		if listed[c] != 1 {
			t.Errorf("cause %s listed %d times", c, listed[c])
		}
	}
	if len(declared) != len(listed) {
		t.Errorf("declared %d causes, listed %d", len(declared), len(listed))
	}
}

func TestBindsTo(t *testing.T) {
	s := subject()
	if err := s.BindsTo(s); err != nil {
		t.Fatal(err)
	}
	mutators := map[string]func(*Subject){
		"accepted_commit":  func(x *Subject) { x.AcceptedCommit = "other" },
		"base_tree":        func(x *Subject) { x.BaseTree = "other" },
		"effective_tree":   func(x *Subject) { x.EffectiveTree = "other" },
		"patches_digest":   func(x *Subject) { x.PatchesDigest = "other" },
		"render_digest":    func(x *Subject) { x.RenderDigest = "other" },
		"env_digest":       func(x *Subject) { x.EnvDigest = "other" },
		"diagnostics_tree": func(x *Subject) { x.DiagnosticsTree = "other" },
	}
	for name, m := range mutators {
		want := s
		m(&want)
		if err := s.BindsTo(want); err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if err := (Subject{}).BindsTo(Subject{}); err == nil {
		t.Fatal("a zero subject must bind to nothing, even another zero subject")
	}
}

func TestMerge(t *testing.T) {
	off := report(t, StageComplete, chk("O1.render", StatusPass, ""))
	live := report(t, StageComplete, Check{ID: "O1.ready", Phase: PhaseLive, Status: StatusPass})
	m, err := Merge(off, live)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Items[0].Checks; len(got) != 2 || got[0].ID != "O1.render" || got[1].ID != "O1.ready" {
		t.Fatalf("merged = %+v", got)
	}
	if len(off.Items[0].Checks) != 1 {
		t.Fatal("merge mutated its input")
	}
	if _, err := Merge(off, off); err == nil {
		t.Fatal("duplicate check id merged")
	}
	other := live
	other.Subject.RenderDigest = "other"
	if _, err := Merge(off, other); err == nil {
		t.Fatal("different subjects merged")
	}
	dep := report(t, StageDeployment)
	if _, err := Merge(off, dep); err == nil {
		t.Fatal("different stages merged")
	}
}

func TestValidateRejects(t *testing.T) {
	bad := map[string]func(*Report){
		"schema":                       func(r *Report) { r.Schema = 2 },
		"stage":                        func(r *Report) { r.Stage = "later" },
		"missing item":                 func(r *Report) { r.Items = r.Items[:5] },
		"item order":                   func(r *Report) { r.Items[0], r.Items[1] = r.Items[1], r.Items[0] },
		"foreign prefix":               func(r *Report) { r.Items[0].Checks[0].ID = "O2.x" },
		"bare id":                      func(r *Report) { r.Items[0].Checks[0].ID = "O1." },
		"phase":                        func(r *Report) { r.Items[0].Checks[0].Phase = "later" },
		"status":                       func(r *Report) { r.Items[0].Checks[0].Status = "maybe" },
		"pass with cause":              func(r *Report) { r.Items[0].Checks[0].Cause = CauseRender },
		"fail no cause":                func(r *Report) { r.Items[0].Checks[0].Status = StatusFail },
		"fail bad cause":               func(r *Report) { r.Items[0].Checks[0].Status = StatusFail; r.Items[0].Checks[0].Cause = "nope" },
		"long detail":                  func(r *Report) { r.Items[0].Checks[0].Detail = strings.Repeat("x", MaxDetailBytes+1) },
		"multiline":                    func(r *Report) { r.Items[0].Checks[0].Detail = "a\nb" },
		"duplicate id":                 func(r *Report) { r.Items[0].Checks = append(r.Items[0].Checks, r.Items[0].Checks[0]) },
		"empty subject":                func(r *Report) { r.Subject.EffectiveTree = "" },
		"complete without diagnostics": func(r *Report) { r.Subject.DiagnosticsTree = "" },
	}
	for name, mut := range bad {
		r := allPass(t)
		mut(&r)
		if err := r.Validate(); err != nil {
			continue
		}
		t.Errorf("%s: validated", name)
	}
	dep := report(t, StageDeployment, chk("O1.a", StatusPass, ""))
	dep.Subject.DiagnosticsTree = ""
	if err := dep.Validate(); err != nil {
		t.Fatalf("deployment stage needs no diagnostics tree: %v", err)
	}
}

func TestClip(t *testing.T) {
	if got := Clip("a\n  b\tc"); got != "a b c" {
		t.Fatalf("clip = %q", got)
	}
	long := strings.Repeat("é", 400)
	got := Clip(long)
	if len(got) > MaxDetailBytes || strings.ToValidUTF8(got, "") != got {
		t.Fatalf("clip produced %d bytes, valid=%v", len(got), strings.ToValidUTF8(got, "") == got)
	}
}

func TestCodecRoundTripDigestAndStrictness(t *testing.T) {
	r := allPass(t)
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	d1, _ := r.Digest()
	d2, _ := back.Digest()
	if d1 != d2 || len(d1) != 64 {
		t.Fatalf("digests %q %q", d1, d2)
	}
	// nil and empty check lists are the same state.
	n := New(StageDeployment, subject())
	n.Items[3].Checks = nil
	e := New(StageDeployment, subject())
	dn, _ := n.Digest()
	de, _ := e.Digest()
	if dn != de {
		t.Fatal("digest depends on nil versus empty checks")
	}
	// Any content change moves the digest.
	c := allPass(t)
	c.Items[2].Checks[0].Detail = "changed"
	if dc, _ := c.Digest(); dc == d1 {
		t.Fatal("digest ignores detail")
	}
	s := string(b)
	for name, in := range map[string]string{
		"unknown field":       strings.Replace(s, `"stage"`, `"verdict":"accepted","stage"`, 1),
		"trailing":            s + "{}",
		"schema":              strings.Replace(s, `"schema":1`, `"schema":2`, 1),
		"empty":               "",
		"not json":            "accepted",
		"unknown check field": strings.Replace(s, `"checks":[{`, `"checks":[{"nope":1,`, 1),
	} {
		if _, err := Decode([]byte(in)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
	if _, err := (Report{}).Encode(); err == nil {
		t.Fatal("invalid report encoded")
	}
	if _, err := (Report{}).Digest(); err == nil {
		t.Fatal("invalid report has a digest")
	}
}

func FuzzDecode(f *testing.F) {
	ok := New(StageDeployment, subject())
	b, _ := ok.Encode()
	f.Add(b)
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		r, err := Decode(data)
		if err != nil {
			return
		}
		out, err := r.Encode()
		if err != nil {
			t.Fatalf("decoded report does not encode: %v", err)
		}
		again, err := Decode(out)
		if err != nil {
			t.Fatalf("encoded report does not decode: %v", err)
		}
		d1, _ := r.Digest()
		d2, _ := again.Digest()
		if d1 != d2 {
			t.Fatal("round trip moved the digest")
		}
	})
}

// The report carries no verdict and no benchmark vocabulary: not in its type
// shape and not in a populated encoding.
func TestNoVerdictOrBenchmarkVocabulary(t *testing.T) {
	deny := []string{"verdict", "sregym", "benchmark", "fault_injection", "grade", "secret", "timestamp", "observed_at"}
	var walk func(rt reflect.Type, seen map[reflect.Type]bool)
	walk = func(rt reflect.Type, seen map[reflect.Type]bool) {
		for rt.Kind() == reflect.Slice || rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
		if rt.Kind() == reflect.Map {
			t.Errorf("report type contains a free-form map: %s", rt)
			return
		}
		if rt.Kind() != reflect.Struct || seen[rt] {
			return
		}
		seen[rt] = true
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			name := strings.ToLower(f.Name + " " + f.Tag.Get("json"))
			for _, d := range deny {
				if strings.Contains(name, d) {
					t.Errorf("field %s.%s names %q", rt.Name(), f.Name, d)
				}
			}
			if f.Type.Kind() == reflect.Struct && f.Type.Name() == "Time" {
				t.Errorf("field %s.%s is a time", rt.Name(), f.Name)
			}
			walk(f.Type, seen)
		}
	}
	walk(reflect.TypeOf(Report{}), map[reflect.Type]bool{})

	r := allPass(t)
	r, _ = r.With(Check{ID: "O1.render", Phase: PhaseOffline, Status: StatusFail, Cause: CauseRender, Detail: Clip("template failed"), Paths: []string{".sdo/deployment/plan.yaml"}})
	b, err := r.Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range deny {
		if strings.Contains(strings.ToLower(string(b)), d) {
			t.Errorf("encoding contains %q", d)
		}
	}
}

func TestWithUnavailableFillsOnlyEmptyFieldsAndNeverBinds(t *testing.T) {
	got := Subject{BaseTree: "t"}.WithUnavailable()
	if got.BaseTree != "t" || got.EffectiveTree != Unavailable || got.DiagnosticsTree != Unavailable {
		t.Fatalf("filled = %+v", got)
	}
	r := New(StageComplete, got)
	if err := r.Validate(); err != nil {
		t.Fatalf("a filled subject must validate: %v", err)
	}
	if err := got.BindsTo(subject()); err == nil {
		t.Fatal("an unavailable subject bound to a recomputed one")
	}
}

func TestAdvanceStageRequiresDeploymentItemsAndTheDetectorVerdict(t *testing.T) {
	if got := StageItems(StageAdvance); len(got) != 4 || got[0] != O1 || got[1] != O2 || got[2] != O4 || got[3] != O6 {
		t.Fatalf("advance stage items = %v, want O1 O2 O4 O6", got)
	}
	r := report(t, StageAdvance, chk("O1.ok", StatusPass, ""), chk("O2.ok", StatusPass, ""), chk("O6.ok", StatusPass, ""))
	if err := r.Validate(); err != nil {
		t.Fatalf("an advance report must validate without O3 and O5: %v", err)
	}
	if d := r.Decide(StageItems(StageAdvance)...); d.Kind != Hold || len(d.Checks) != 1 || d.Checks[0].ID != "O4.present" {
		t.Fatalf("an advance without a detector verdict must fail closed: %+v", d)
	}
	r, err := r.With(chk("O4.detectors", StatusPass, ""))
	if err != nil {
		t.Fatal(err)
	}
	if d := r.Decide(StageItems(StageAdvance)...); d.Kind != Accepted {
		t.Fatalf("complete advance report: %+v", d)
	}
}
