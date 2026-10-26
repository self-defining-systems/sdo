package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/jitview"
	"sdo.dev/operator/sdk/program"
)

const workingTreeRevisionPrefix = "working-tree:"

type workingProgram struct {
	spec        program.ProgramSpec
	canMitigate bool
	binary      string
	revision    string
	workspace   *builder.Workspace
}

func (p *workingProgram) Spec() program.ProgramSpec { return p.spec }
func (p *workingProgram) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	raw, err := jitview.Run(ctx, p.binary, workingRequest(ctx, "diagnose", p.spec.ID, bindings), view)
	if err != nil {
		return program.Verdict{}, err
	}
	var verdict program.Verdict
	err = json.Unmarshal(raw, &verdict)
	return verdict, err
}
func (p *workingProgram) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	raw, err := jitview.Run(ctx, p.binary, workingRequest(ctx, "mitigate", p.spec.ID, bindings), view)
	if err != nil {
		return program.Plan{}, err
	}
	var result PlanApplicationResult
	err = json.Unmarshal(raw, &result)
	return result.Plan, err
}

type workingProgramRegistry struct {
	accepted  program.ProgramRegistry
	overrides map[string]*workingProgram
	disabled  map[string]bool
	failures  map[string]string
	revisions map[string]string
}

func (r *workingProgramRegistry) List() []program.ProgramSpec {
	specs := map[string]program.ProgramSpec{}
	if r.accepted != nil {
		for _, spec := range r.accepted.List() {
			specs[spec.ID] = spec
		}
	}
	for id, p := range r.overrides {
		specs[id] = p.spec
	}
	var out []program.ProgramSpec
	for _, spec := range specs {
		if r.disabled[spec.ID] {
			continue
		}
		out = append(out, spec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (r *workingProgramRegistry) Diagnoser(id string) (program.Diagnoser, bool) {
	if r.disabled[id] {
		return nil, false
	}
	if p, ok := r.overrides[id]; ok {
		return p, true
	}
	if r.accepted == nil {
		return nil, false
	}
	return r.accepted.Diagnoser(id)
}
func (r *workingProgramRegistry) Mitigator(id string) (program.Mitigator, bool) {
	if r.disabled[id] {
		return nil, false
	}
	if p, ok := r.overrides[id]; ok {
		return p, p.canMitigate
	}
	if r.accepted == nil {
		return nil, false
	}
	return r.accepted.Mitigator(id)
}

// loadWorkingPrograms is called only by agent tools, never controller detection.
// A changed source must either compile or fail this call; stale code is never a
// fallback. The staged immutable source supplies both the binary and its digest.
func (l *ResponderLoop) loadWorkingPrograms(ctx context.Context, id string) error {
	if l.WorkingProgramRoot == "" {
		return nil
	}
	if !l.Request.ResponseDeadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, l.Request.ResponseDeadline)
		defer cancel()
	}
	// Registry versions are immutable once a tool starts using them. A timed
	// out diagnosis may still finish its read-only goroutine after the next edit.
	registry := &workingProgramRegistry{accepted: l.Programs, overrides: map[string]*workingProgram{}, revisions: map[string]string{}, disabled: map[string]bool{}, failures: map[string]string{}}
	if prior, ok := l.Programs.(*workingProgramRegistry); ok {
		registry.accepted = prior.accepted
		registry.overrides = maps.Clone(prior.overrides)
		registry.revisions = maps.Clone(prior.revisions)
		registry.disabled = maps.Clone(prior.disabled)
		registry.failures = maps.Clone(prior.failures)
	}
	l.Programs = registry
	ids := map[string]bool{}
	if id != "" {
		if !programIDPath.MatchString(id) {
			return fmt.Errorf("invalid playbook ID")
		}
		ids[id] = true
	} else {
		for _, spec := range registry.List() {
			ids[spec.ID] = true
		}
		entries, err := os.ReadDir(filepath.Join(l.WorkingProgramRoot, ".sdo/playbooks"))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			if _, err := os.Stat(filepath.Join(l.WorkingProgramRoot, ".sdo/playbooks", entry.Name(), "diagnose.go")); err == nil {
				ids[entry.Name()] = true
			}
		}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, programID := range ordered {
		if err := l.loadWorkingProgram(ctx, registry, programID, id != ""); err != nil {
			registry.disabled[programID] = true
			registry.failures[programID] = err.Error()
			l.invalidateProgramPlans(programID)
			if id != "" {
				return err
			}
		}
	}
	return nil
}
func (l *ResponderLoop) loadWorkingProgram(ctx context.Context, registry *workingProgramRegistry, id string, explicit bool) error {
	delete(registry.failures, id)
	base := filepath.Join(l.WorkingProgramRoot, ".sdo/playbooks", id)
	if _, err := os.Stat(filepath.Join(base, "diagnose.go")); os.IsNotExist(err) {
		// A registry may also contain built-in programs with no repository
		// source. Distinguish those from a deleted accepted playbook.
		if !gitRevision.MatchString(l.Request.SourceCommit) {
			return nil
		}
		listing, err := exec.CommandContext(ctx, "git", "-C", l.Request.RepositoryWorktree, "ls-tree", "--name-only", l.Request.SourceCommit, "--", ".sdo/playbooks/"+id+"/diagnose.go").Output()
		if err != nil {
			return l.shortProgramError(id, err)
		}
		if len(listing) == 0 && registry.overrides[id] == nil {
			return nil
		}
		registry.disabled[id] = true
		delete(registry.overrides, id)
		l.invalidateProgramPlans(id)
		if explicit {
			return fmt.Errorf("playbook %s: working diagnose.go is missing", id)
		}
		return nil
	} else if err != nil {
		return err
	}
	retired, err := layout.ReadPlaybookRetired(base)
	if err != nil {
		return l.shortProgramError(id, err)
	}
	if retired {
		registry.disabled[id] = true
		delete(registry.overrides, id)
		l.invalidateProgramPlans(id)
		if explicit {
			return fmt.Errorf("playbook %s is retired", id)
		}
		return nil
	}
	delete(registry.disabled, id)
	module := "app-diagnostics"
	if data, err := exec.CommandContext(ctx, "git", "-C", l.Request.RepositoryWorktree, "show", l.Request.SourceCommit+":.sdo/diagnostics/go.mod").Output(); err == nil {
		if match := programModule.FindSubmatch(data); len(match) == 2 {
			module = string(match[1])
		}
	}
	revision, err := workingProgramRevision(filepath.Join(l.WorkingProgramRoot, ".sdo"), module, id)
	if err != nil {
		return l.shortProgramError(id, err)
	}
	accepted, err := programDependencyRevision(ctx, l.Request.RepositoryWorktree, l.Request.SourceCommit, id)
	if err == nil && accepted == revision {
		if registry.overrides[id] != nil {
			delete(registry.overrides, id)
			l.invalidateProgramPlans(id)
		}
		registry.revisions[id] = revision
		return nil
	}
	revision = workingTreeRevisionPrefix + revision
	if prior := registry.overrides[id]; prior != nil && prior.revision == revision {
		return nil
	}
	operator := os.Getenv("SDO_OPERATOR_ROOT")
	if operator == "" {
		operator = "/opt/sdo/sdo/operator"
	}
	// Compilation has its own short budget, independent of diagnosis timeouts.
	buildCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	started := time.Now()
	l.mu.Unlock()
	workspace, binary, buildErr := builder.BuildWorkingProgram(buildCtx, builder.Config{ApplicationRoot: l.WorkingProgramRoot, OperatorRoot: operator, ProgramModule: module}, id)
	l.mu.Lock()
	cancel()
	slog.Info("responder_program_compile", "program_id", id, "compile_ms", time.Since(started).Milliseconds(), "success", buildErr == nil)
	if buildErr != nil {
		return l.shortProgramError(id, buildErr)
	}
	digest, err := workingProgramRevision(workspace.Directory, workspace.ModulePath, id)
	if err != nil {
		_ = workspace.Close()
		return l.shortProgramError(id, err)
	}
	specCtx, stop := context.WithTimeout(ctx, l.deadline())
	raw, err := jitview.Run(specCtx, binary, jitRequest{Command: "spec"}, nil)
	stop()
	var specs []jitSpec
	if err == nil {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		err = decoder.Decode(&specs)
	}
	if err != nil || len(specs) != 1 || specs[0].Spec.ID != id {
		_ = workspace.Close()
		return l.shortProgramError(id, errors.New("compiled program Spec.ID must match its playbook ID"))
	}
	p := &workingProgram{spec: specs[0].Spec, canMitigate: specs[0].CanMitigate, binary: binary, revision: workingTreeRevisionPrefix + digest, workspace: workspace}
	registry.overrides[id] = p
	registry.revisions[id] = p.revision
	// Keep older binaries until this incident ends: prepared plans and pending
	// verification can still refer to that immutable revision.
	l.workingPrograms = append(l.workingPrograms, p)
	l.invalidateProgramPlans(id)
	return nil
}

func (l *ResponderLoop) invalidateProgramPlans(id string) {
	prefix := id + "\x00"
	for key := range l.matchedViews {
		if strings.HasPrefix(key, prefix) {
			delete(l.matchedViews, key)
			delete(l.matchedPlans, key)
			delete(l.matchedPlanErrors, key)
		}
	}
	// Menu option IDs remain stable for unchanged bindings, but their parameter
	// descriptions must be rebuilt from the current Spec before use.
	for key, entry := range l.menuEntries {
		if entry.programID == id {
			if p, ok := l.Programs.Diagnoser(id); ok {
				entry.spec = p.Spec()
				l.menuEntries[key] = entry
			}
		}
	}
}
func (l *ResponderLoop) shortProgramError(id string, err error) error {
	text := err.Error()
	// Name logical source locations in responder evidence, never the incident's
	// physical checkout path.
	text = strings.ReplaceAll(text, filepath.Join(l.WorkingProgramRoot, ".sdo/playbooks", id)+string(filepath.Separator), id+"/")
	text = strings.ReplaceAll(text, filepath.Join(l.WorkingProgramRoot, ".sdo/lib")+string(filepath.Separator), "lib/")
	var stage *builder.StageError
	if errors.As(err, &stage) {
		text = stage.Output
	}
	lines := strings.Split(text, "\n")
	if len(lines) > 6 {
		lines = lines[:6]
	}
	text = strings.Join(lines, "\n")
	if len(text) > 1200 {
		text = text[:1200]
	}
	return fmt.Errorf("playbook %s compile failed: %s; fix the checkout source and retry", id, text)
}
func (l *ResponderLoop) programRevision(id string) string {
	if registry, ok := l.Programs.(*workingProgramRegistry); ok {
		if revision := registry.revisions[id]; revision != "" {
			return revision
		}
	}
	runs := []ProgramRun{{ProgramID: id}}
	fillProgramRevisions(context.Background(), runs, l.Request.RepositoryWorktree, l.Request.SourceCommit)
	return runs[0].ProgramRevision
}
func (l *ResponderLoop) stampProgramRuns(runs []ProgramRun) {
	for i := range runs {
		if runs[i].ProgramRevision == "" {
			runs[i].ProgramRevision = l.programRevision(runs[i].ProgramID)
		}
	}
}
func (l *ResponderLoop) CloseWorkingPrograms() {
	l.operationMu.Lock()
	defer l.operationMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, p := range l.workingPrograms {
		_ = p.workspace.Close()
	}
	l.workingPrograms = nil
}

func workingRequest(ctx context.Context, command, id string, bindings program.Bindings) jitRequest {
	deadline, _ := ctx.Deadline()
	return jitRequest{Command: command, ProgramID: id, Bindings: jitBindings(bindings), Deadline: deadline}
}

// A broken draft must not block unrelated SDK actions or another program.
func (l *ResponderLoop) loadSelectedWorkingPrograms(ctx context.Context, request ToolRequest) error {
	if request.ProgramID != "" {
		return l.loadWorkingPrograms(ctx, request.ProgramID)
	}
	ids := map[string]bool{}
	options := []string{}
	if request.Selection != nil {
		options = append(options, request.Selection.OptionId)
	}
	if request.SelectionSet != nil {
		for _, selection := range request.SelectionSet.Options {
			if selection != nil {
				options = append(options, selection.OptionId)
			}
		}
	}
	for _, option := range options {
		if entry, ok := l.menuEntries[option]; ok && entry.action == nil && entry.plan == nil {
			ids[entry.programID] = true
		}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	for _, id := range ordered {
		if err := l.loadWorkingPrograms(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
