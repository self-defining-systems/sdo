package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/staleness"
)

// PROTOTYPE (staleness rules). The one switch: SDO_PROTO_STALENESS_RULES=off.
const stalenessSwitch = "SDO_PROTO_STALENESS_RULES"

// stageNotAutomatic is the skip of a refused playbook: the agent takes it as a
// hint, with the reason in the attempt's error. A skip is never a playbook
// failure (ProgramTrustStats counts none). It reuses an existing contract value.
const stageNotAutomatic = "repair_policy"

// stalenessDiagnose bounds one Diagnose of the healthy pass.
const stalenessDiagnose = 5 * time.Second

// StalenessGuard holds what the rules know. Proofs are derived from outcomes
// and the corpus at each healthy pass; the release facts are controller state
// beside the store baselines (.sdo-runtime/staleness.json), never in .sdo/.
type StalenessGuard struct {
	Path     string
	Corpus   func(context.Context, string) (dir string, done func(), err error)
	Outcomes func(context.Context) ([]*contracts.OutcomeRecord, error)
	View     func(sdk.DetectionContext) program.ReadView
	Target   func() broker.Target

	mu      sync.Mutex
	running bool
	state   stalenessState
	loaded  bool
}

type stalenessState struct {
	// Pass is the (release, memory commit) the last healthy pass covered.
	Pass    string                     `json:"pass"`
	Release staleness.Release          `json:"release"`
	Proofs  map[string]staleness.Proof `json:"proofs,omitempty"`
}

// StalenessStatePath is the guard's record below the repository mount.
func StalenessStatePath(stateRoot string) string {
	return filepath.Join(stateRoot, ".sdo-runtime", "staleness.json")
}

func stalenessEnabled() bool { return lifecycleio.Getenv(stalenessSwitch) != "off" }

func (g *StalenessGuard) load() {
	if g.loaded {
		return
	}
	g.loaded = true
	if raw, err := os.ReadFile(g.Path); err == nil && json.Unmarshal(raw, &g.state) != nil {
		g.state = stalenessState{}
	}
}

// view is what is known now, for the release the incident compares with.
func (g *StalenessGuard) view(id, commit string) (staleness.Proof, staleness.Release) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.load()
	release := g.state.Release
	if release.Commit != commit {
		release = staleness.Release{Commit: commit} // facts of another release say nothing
	}
	if g.Target != nil {
		target := g.Target()
		release.Unaccepted = target.Unaccepted || target.Commit != "" && target.Commit != commit
	}
	return g.state.Proofs[id], release
}

func notAutomatic(reason string) string {
	if reason == "" {
		return ""
	}
	return "not automatic: " + reason
}

// stalenessHint is R1 and stalenessRefusal is R2 and R3. Both are "" for the built-in restore,
// which is not learned.
func (l *ResponderLoop) stalenessHint(id string) string {
	if l.Staleness == nil || id == SourceRestoreID {
		return ""
	}
	proof, release := l.Staleness.view(id, l.Request.renderedRevision())
	return notAutomatic(staleness.BeforePlan(id, proof, release))
}

func (l *ResponderLoop) stalenessRefusal(id string, plan program.Plan) string {
	if l.Staleness == nil || id == SourceRestoreID {
		return ""
	}
	proof, release := l.Staleness.view(id, l.Request.renderedRevision())
	return notAutomatic(staleness.Refuse(id, plan, proof, release))
}

// observeStaleness starts one healthy pass when the app is verified healthy on
// a (release, accepted memory) the guard has not covered. A pass is read-only.
func (c *Controller) observeStaleness(ctx context.Context, snapshot sdk.DetectionContext, healthy bool) {
	g := c.Staleness
	if g == nil || !healthy {
		return
	}
	release, commit := c.CurrentRenderedRevision(), c.CurrentAcceptedCommit()
	g.mu.Lock()
	g.load()
	skip := g.running || g.state.Pass == release+"@"+commit
	if !skip {
		g.running = true
	}
	g.mu.Unlock()
	c.mu.Lock()
	programs := c.stalenessPrograms
	c.mu.Unlock()
	if skip || programs == nil {
		if !skip {
			g.mu.Lock()
			g.running = false
			g.mu.Unlock()
		}
		return
	}
	c.goBackground(func() {
		defer func() { g.mu.Lock(); g.running = false; g.mu.Unlock() }()
		started := c.clock()
		state, err := g.pass(ctx, release, commit, programs(), g.View(snapshot))
		if err != nil {
			slog.Warn("staleness_pass", "error", err)
			return
		}
		// A symptom seen while the pass ran makes its readings no healthy ones.
		if !c.quiet() || c.CurrentRenderedRevision() != release || c.CurrentAcceptedCommit() != commit {
			return
		}
		g.mu.Lock()
		g.state = state
		raw, err := json.MarshalIndent(state, "", " ")
		if err == nil {
			err = os.WriteFile(g.Path, raw, 0o600)
		}
		g.mu.Unlock()
		slog.Info("staleness_pass", "release", release, "memory", commit, "programs", len(state.Release.Checked), "demoted", state.Release.Demoted, "facts", len(state.Release.Facts), "ms", c.clock().Sub(started).Milliseconds(), "error", err)
	})
}

// pass diagnoses every learned program on the healthy app and reads the cells
// of each verified plan.
func (g *StalenessGuard) pass(ctx context.Context, release, commit string, registry program.ProgramRegistry, view program.ReadView) (stalenessState, error) {
	state := stalenessState{Pass: release + "@" + commit, Release: staleness.Release{Commit: release, Checked: map[string]bool{}, Demoted: map[string]string{}, Facts: staleness.Facts{}}}
	outcomes, err := g.Outcomes(ctx)
	if err != nil {
		return state, err
	}
	dir, done, err := g.Corpus(ctx, commit)
	if err != nil {
		return state, err
	}
	defer done()
	proofs, plans := StalenessProofs(ctx, dir, outcomes, registry)
	state.Proofs = proofs
	for _, spec := range registry.List() {
		if spec.ID == SourceRestoreID {
			continue
		}
		d, ok := registry.Diagnoser(spec.ID)
		if !ok {
			continue
		}
		if len(requiredRoles(spec)) == 0 {
			verdict := program.RunDiagnose(ctx, d, view, program.Bindings{}, stalenessDiagnose)
			if verdict.Result == program.Match {
				state.Release.Demoted[spec.ID] = "Diagnose matches the healthy app of release " + release
			}
		}
		state.Release.Checked[spec.ID] = true
		for _, plan := range plans[spec.ID] {
			staleness.Check(view, plan, state.Release.Facts)
		}
	}
	return state, nil
}

func requiredRoles(spec program.ProgramSpec) (roles []string) {
	for _, p := range spec.Params {
		if p.Required {
			roles = append(roles, p.Name)
		}
	}
	return roles
}

// StalenessProofs derives, per program, the releases it recovered users on, the
// components its verified plan wrote, and whether it matches its own recovered
// capture; with the verified plans whose cells a healthy pass reads.
func StalenessProofs(ctx context.Context, corpus string, outcomes []*contracts.OutcomeRecord, registry program.ProgramRegistry) (map[string]staleness.Proof, map[string][]program.Plan) {
	proofs, plans := map[string]staleness.Proof{}, map[string][]program.Plan{}
	byIncident := map[string]*contracts.OutcomeRecord{}
	prove := func(id, release string) {
		p := proofs[id]
		if release != "" && !slices.Contains(p.Releases, release) {
			p.Releases = append(p.Releases, release)
		}
		proofs[id] = p
	}
	widen := func(id string, wrote []string) {
		p := proofs[id]
		if p.Footprint == nil {
			p.Footprint = []string{}
		}
		for _, c := range wrote {
			if !slices.Contains(p.Footprint, c) {
				p.Footprint = append(p.Footprint, c)
			}
		}
		proofs[id] = p
	}
	for _, o := range outcomes {
		byIncident[o.GetIncidentId()] = o
		if o.GetClassification() != "success" {
			continue
		}
		release := outcomeRelease(o)
		// Users recovered after the program's plan: the last write, or any in
		// an incident that a success closed without an agent write.
		if app := finalApplication(o); app != nil && len(app.GetProgramIds()) == 1 {
			id := app.GetProgramIds()[0]
			plan := planApplicationState(app).Result.Plan
			prove(id, release)
			widen(id, staleness.Footprint(plan))
			plans[id] = append(plans[id], plan)
			continue
		}
		// Or after the agent's, in an incident the program matched.
		for _, run := range o.GetProgramRuns() {
			if run.GetVerdict().GetResult() == contracts.Verdict_RESULT_MATCH && len(o.GetAgentChanges())+len(o.GetRepairActions()) > 0 {
				prove(run.GetProgramId(), release)
			}
		}
	}
	entries, _ := os.ReadDir(corpus)
	for _, entry := range entries {
		c, err := sdktest.LoadCorpusCase(filepath.Join(corpus, entry.Name()))
		if err != nil || c.Provenance.IncidentID == "" {
			continue
		}
		for _, id := range c.RootCauseProgramIDs {
			d, ok := registry.Diagnoser(id)
			if !ok {
				continue
			}
			fixed := &sdktest.ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}
			if v := program.RunDiagnose(ctx, d, fixed, c.Bindings, stalenessDiagnose); v.Result == program.Match {
				p := proofs[id]
				p.Hint = "Diagnose matches the recovered state of its own incident " + c.Provenance.IncidentID
				proofs[id] = p
			}
			origin := byIncident[c.Provenance.IncidentID]
			if origin.GetClassification() == "success" {
				prove(id, outcomeRelease(origin))
			}
			m, ok := registry.Mitigator(id)
			if !ok {
				continue
			}
			open := &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}
			if program.RunDiagnose(ctx, d, open, c.Bindings, stalenessDiagnose).Result != program.Match {
				// The capture lacks a read: elicit the plan's literals with made-up
				// answers. They only name which cells to read; never a proof.
				plans[id] = append(plans[id], elicit(ctx, m, open, c.Bindings)...)
				continue
			}
			plan, err := BuildMitigationPlan(ctx, m, open, c.Bindings, stalenessDiagnose)
			if err != nil {
				continue
			}
			plans[id] = append(plans[id], plan)
			widen(id, agentWrote(origin, staleness.Footprint(plan)))
		}
	}
	return proofs, plans
}

func outcomeRelease(o *contracts.OutcomeRecord) string {
	if c := o.GetDeployedCommit(); c != "" {
		return c
	}
	return o.GetSourceCommit()
}

// agentWrote keeps the workloads of a learned plan that the origin incident's
// responder wrote too. A responder whose writes are not known keeps them all.
func agentWrote(origin *contracts.OutcomeRecord, planned []string) []string {
	wrote, known := map[string]bool{}, false
	add := func(ref *contracts.ObjectRef) {
		if ref.GetName() != "" {
			wrote[staleness.Component(ref.GetKind(), ref.GetName())] = true
		}
	}
	for _, a := range origin.GetRepairActions() {
		for _, ref := range a.GetResources() {
			add(ref)
		}
		for _, owner := range a.GetOwners() {
			add(owner.GetResource())
			for _, ref := range owner.GetOwnerChain() {
				add(ref)
			}
		}
	}
	text := ""
	for _, change := range origin.GetAgentChanges() {
		text += " " + change.GetTarget() + " " + change.GetSummary()
	}
	for _, c := range planned {
		name := strings.TrimPrefix(c, "workload/")
		if name != c && strings.Contains(text, name) {
			wrote[c] = true
		}
	}
	for c := range wrote {
		known = known || strings.HasPrefix(c, "workload/")
	}
	if !known {
		return planned
	}
	kept := []string{}
	for _, c := range planned {
		if !strings.HasPrefix(c, "workload/") || wrote[c] {
			kept = append(kept, c)
		}
	}
	return kept
}

// stalenessCorpus extracts the accepted corpus at commit into a temporary directory.
func stalenessCorpus(read gitRead, repository string) func(context.Context, string) (string, func(), error) {
	return func(ctx context.Context, commit string) (string, func(), error) {
		dir, err := os.MkdirTemp("", "sdo-staleness-")
		if err != nil {
			return "", nil, err
		}
		done := func() { _ = os.RemoveAll(dir) }
		raw, err := read(ctx, repository, "archive", "--format=tar", commit, "--", layout.CorpusDir)
		if err != nil {
			return dir, done, nil // no corpus yet
		}
		reader := tar.NewReader(bytes.NewReader(raw))
		for {
			header, err := reader.Next()
			if err == io.EOF {
				return filepath.Join(dir, layout.CorpusDir), done, nil
			}
			if err != nil {
				done()
				return "", nil, fmt.Errorf("read accepted corpus: %w", err)
			}
			path := filepath.Join(dir, filepath.Clean("/"+header.Name))
			if header.Typeflag != tar.TypeReg {
				continue
			}
			body, err := io.ReadAll(reader)
			if err == nil {
				err = os.MkdirAll(filepath.Dir(path), 0o700)
			}
			if err == nil {
				err = os.WriteFile(path, body, 0o600)
			}
			if err != nil {
				done()
				return "", nil, err
			}
		}
	}
}

// deploymentTargetNow is the revision the cluster was last moved to (#1965).
func (c *Controller) deploymentTargetNow() broker.Target {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.head.target
}

func stalenessOutcomes(root string) func(context.Context) ([]*contracts.OutcomeRecord, error) {
	reader := memory.OpenReader(root)
	return func(ctx context.Context) ([]*contracts.OutcomeRecord, error) {
		stored, _, err := memory.ListOutcomes(ctx, reader, "staleness proofs")
		var records []*contracts.OutcomeRecord
		for _, o := range stored {
			if o.Record != nil {
				records = append(records, o.Record)
			}
		}
		return records, err
	}
}

// elicitView answers the reads a capture lacks with made-up values.
type elicitView struct {
	*sdktest.ReplayReadView
	code int32
	row  any
}

func (v elicitView) Observe(o program.ExternalOperation) (int32, error) {
	if code, err := v.ReplayReadView.Observe(o); err == nil {
		return code, nil
	}
	return v.code, nil
}

func (v elicitView) Read(q program.Query) (program.Observation, error) {
	if o, err := v.ReplayReadView.Read(q); err == nil || q.Store == nil {
		return o, err
	}
	row := program.Row{}
	for _, field := range q.Store.Fields {
		row[field] = v.row
	}
	return program.Observation{Count: 1, Rows: []program.Row{row}}, nil
}

// elicit returns the plans a mitigation makes under the made-up answers.
func elicit(ctx context.Context, m program.Mitigator, open *sdktest.ReplayReadView, b program.Bindings) (plans []program.Plan) {
	for _, code := range []int32{0, 1, 2} {
		for _, row := range []any{int64(1), int64(0), true, int64(1) << 40} {
			if plan, err := BuildMitigationPlan(ctx, m, elicitView{ReplayReadView: open, code: code, row: row}, b, stalenessDiagnose); err == nil {
				plans = append(plans, plan)
			}
		}
	}
	return plans
}
