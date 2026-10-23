package validate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

const ContextFile = "sdo-check-context.json"
const DecisionFile = "sdo-reflection-decision.json"

// PublicationContext contains only the publication inputs unavailable in files.
// Lifecycle facts are recomputed from Base, whose history is in the sandbox.
type PublicationContext struct {
	BaseCommit         string `json:"base_commit"`
	IncidentID         string `json:"incident_id"`
	VerifiedSourceTree string `json:"verified_source_tree,omitempty"`
}

func metadataPath(ctx context.Context, root, name string) (string, error) {
	dir, err := gitText(ctx, root, "rev-parse", "--absolute-git-dir")
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name), nil
}

func WritePublicationContext(ctx context.Context, root string, publication PublicationContext) error {
	path, err := metadataPath(ctx, root, ContextFile)
	if err != nil {
		return err
	}
	data, err := json.Marshal(publication)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0600)
}

// LoadPublicationContext reads a proposal's provisioned context for self-checks.
func LoadPublicationContext(ctx context.Context, root string) (*PublicationContext, error) {
	path, err := metadataPath(ctx, root, ContextFile)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var publication PublicationContext
	if err := json.Unmarshal(data, &publication); err != nil {
		return nil, fmt.Errorf("publication context: %w", err)
	}
	return &publication, nil
}

// LoadReflectionDecision reads the intended decision without adding authored
// metadata to the candidate tree. The file is the ReflectionResult the worker
// returns (the message its model-output schema is generated from), so the
// agent copies its returned result as is. The worker's actual result is supplied
// independently to publication and checked by the same rule.
func LoadReflectionDecision(ctx context.Context, root, path string) (*contracts.ReflectionDecision, error) {
	if path == "" {
		var err error
		path, err = metadataPath(ctx, root, DecisionFile)
		if err != nil {
			return nil, err
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := &contracts.ReflectionResult{}
	if err := protojson.Unmarshal(data, result); err != nil {
		return nil, fmt.Errorf("reflection decision: %w", err)
	}
	return &contracts.ReflectionDecision{
		DeclineReason: result.GetDeclineReason(), LearningDecision: result.GetLearningDecision(), NoChangeReason: result.GetNoChangeReason(),
		PlaybookIds: result.GetProposedArtifactIds(), SdkGaps: result.GetSdkGaps(),
	}, nil
}

func lifecycleChecks(ctx context.Context, r Request, paths []string) ([]Diagnostic, error) {
	var ds []Diagnostic
	add := func(rule, message, hint string) {
		ds = append(ds, Diagnostic{Rule: rule, Message: message, Hint: hint})
	}
	if r.Publication != nil {
		base, err := gitText(ctx, r.Root, "rev-parse", r.Base+"^{commit}")
		if err != nil {
			return nil, err
		}
		contextBase, err := gitText(ctx, r.Root, "rev-parse", r.Publication.BaseCommit+"^{commit}")
		if err != nil {
			return nil, err
		}
		if contextBase != base {
			return nil, errors.New("publication context differs from validation base")
		}
	}
	if len(paths) == 0 && !healthRevalidation(r, paths) && !(r.Phase == ReflectionPhase && r.Decision != nil && (r.Decision.GetLearningDecision() == "no_change" || r.Decision.GetLearningDecision() == "sdk_gap")) {
		add(RuleEmptyDiff, "proposal has no changed files", "return no_change instead of publishing")
	}
	// Upkeep carries schema migrations; the independent health judge may
	// refine health diagnostics after their first accepted registration.
	if r.Phase == InitialPhase && r.Actor != Upkeep && r.Actor != HealthJudge {
		base, err := exportBase(ctx, r.Root, r.Base)
		if err != nil {
			return nil, err
		}
		defer os.RemoveAll(base)
		registered, err := layout.HasDetectors(base)
		if err != nil {
			return nil, err
		}
		if registered {
			add(RuleBootstrapClosed, "initial phase is closed after detector manifests are accepted", "")
		}
	}
	if r.Phase == ReflectionPhase {
		incident := ""
		if r.Publication != nil {
			incident = r.Publication.IncidentID
		}
		history, err := gitText(ctx, r.Root, "log", r.Base, "--format=%B%x1e")
		if err != nil {
			return nil, err
		}
		accepted := false
		for _, message := range strings.Split(history, "\x1e") {
			message = "\n" + message + "\n"
			if incident != "" && strings.Contains(message, "\nSDO-Incident: "+incident+"\n") && strings.Contains(message, "\nSDO-Phase: outcome\n") {
				accepted = true
			}
		}
		if !accepted {
			add(RuleAcceptedOutcome, "reflection requires an accepted outcome commit", "use the broker-provisioned incident context and accepted base")
		}
		if r.Actor == Responder {
			ds = append(ds, ReflectionDiagnostics(r.Decision, paths)...)
		}
	}
	if r.Phase == InitialPhase && r.Actor == Deployer {
		source, architecture := false, false
		for _, p := range paths {
			source = source || !layout.IsMemoryPath(p)
			architecture = architecture || p == layout.ArchitectureFile
		}
		tree := ""
		if r.Publication != nil {
			tree = r.Publication.VerifiedSourceTree
		}
		if source && tree == "" {
			add(RuleVerifiedSource, "initial source must use PublishVerifiedSource", "trusted deployment verification must provision the candidate tree")
		}
		if tree != "" && (!source || architecture) {
			add(RuleVerifiedSource, "verified source publish requires source files without architecture", "")
		}
	}
	return ds, nil
}

// ReflectionDiagnostics binds a decision and logical IDs to actual file changes.
func ReflectionDiagnostics(d *contracts.ReflectionDecision, paths []string) []Diagnostic {
	fail := func(message string) []Diagnostic {
		return []Diagnostic{{Rule: RuleReflectionDecision, Message: message, Hint: "write the intended ReflectionDecision JSON to .git/" + DecisionFile + " before running memory check; use the same decision and logical IDs in the returned result"}}
	}
	if d == nil {
		return fail("reflection requires an explicit learning decision")
	}
	if d.GetDeclineReason() == "evidence_gap" && d.GetLearningDecision() == "no_change" && d.GetNoChangeReason() != "evidence gap" {
		return fail("an evidence gap no_change uses the canonical evidence gap reason")
	}
	if d.GetNoChangeReason() == "evidence gap" && d.GetDeclineReason() != "evidence_gap" {
		return fail("an evidence gap decline requires its typed decline_reason")
	}
	switch d.GetLearningDecision() {
	case "updated":
		if len(paths) == 0 {
			return fail("updated reflection requires a nonempty diff")
		}
	case "no_change", "sdk_gap":
		if strings.TrimSpace(d.GetNoChangeReason()) == "" {
			return fail("no-change or SDK-gap reflection requires a concrete reason")
		}
		if len(paths) != 0 || len(d.GetPlaybookIds()) != 0 {
			return fail("no-change or SDK-gap reflection cannot change files or claim playbook IDs")
		}
	default:
		return fail("reflection result requires an explicit learning decision")
	}
	seen := map[string]bool{}
	for _, id := range d.GetPlaybookIds() {
		if !layout.ValidPlaybookID(id) || seen[id] {
			return fail(fmt.Sprintf("invalid or repeated logical playbook ID %q", id))
		}
		seen[id] = true
		changed := false
		for _, p := range paths {
			kind, changedID, ok := layout.Classify(p)
			bookID, _, _ := strings.Cut(changedID, "/")
			if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookScript || kind == layout.KindPlaybookProgram) && bookID == id {
				changed = true
			}
		}
		if !changed {
			return fail(fmt.Sprintf("reported playbook %q is absent from the diff", id))
		}
	}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		id, _, _ = strings.Cut(id, "/")
		if ok && (kind == layout.KindPlaybook || kind == layout.KindPlaybookScript || kind == layout.KindPlaybookProgram) && !seen[id] {
			return fail(fmt.Sprintf("changed playbook %q is missing from the result", id))
		}
	}
	return nil
}

func sortDiagnostics(ds []Diagnostic) {
	sort.SliceStable(ds, func(i, j int) bool {
		if ds[i].Path != ds[j].Path {
			return ds[i].Path < ds[j].Path
		}
		if ds[i].Line != ds[j].Line {
			return ds[i].Line < ds[j].Line
		}
		return ds[i].Rule < ds[j].Rule
	})
}

func publicationChecks(ctx context.Context, r Request, structurallyValid bool) ([]Diagnostic, error) {
	paths, err := ChangedPaths(ctx, r.Root, r.Base)
	if err != nil {
		return nil, err
	}
	ds, err := lifecycleChecks(ctx, r, paths)
	if err != nil {
		return nil, err
	}
	// A scratch index includes new files and agent-local commits while leaving
	// the proposal index unchanged. Git itself defines the whitespace policy.
	if structurallyValid {
		whitespace, err := checkWhitespace(ctx, r)
		if err != nil {
			return nil, err
		}
		ds = append(ds, whitespace...)
	}
	if structurallyValid {
		touchesMemory := false
		for _, p := range paths {
			if layout.IsMemoryPath(p) {
				touchesMemory = true
			}
		}
		if touchesMemory && r.Phase == InitialPhase {
			registered, err := layout.HasDetectors(r.Root)
			if err != nil {
				return nil, err
			}
			touchesMemory = registered
			for _, name := range []string{layout.GoalFile, layout.ArchitectureFile} {
				if _, err := os.Stat(filepath.Join(r.Root, name)); errors.Is(err, os.ErrNotExist) {
					touchesMemory = false
				} else if err != nil {
					return nil, err
				}
			}
		}
		if touchesMemory {
			_, err := memory.CandidateSnapshot(r.Root, strings.Repeat("0", 40))
			if errors.Is(err, memory.ErrSnapshotBudget) {
				ds = append(ds, Diagnostic{Rule: RuleSnapshotBudget, Path: layout.Dir, Message: err.Error(), Hint: "shorten playbooks or scripts to fit the 512 KiB logical snapshot budget"})
			} else if err != nil && !(r.Phase == InitialPhase && errors.Is(err, memory.ErrSnapshotIncomplete)) {
				ds = append(ds, Diagnostic{Rule: RuleSnapshotProjection, Path: layout.Dir, Message: err.Error(), Hint: "memory must project into the logical worker task"})
			}
		}
	}
	return ds, nil
}

// PhaseDiagnostics is shared with publication before it selects a proposal.
func PhaseDiagnostics(phase Phase) []Diagnostic {
	if !knownPhase(phase) {
		return []Diagnostic{{Rule: RuleUnknownPhase, Message: fmt.Sprintf("unknown phase %q", phase), Hint: "phases are proposal, reflection, initial, and outcome"}}
	}
	return nil
}

// Only an input-bound judge refresh with existing health diagnostics may accept
// a no-op. Static validation still checks the entire candidate and independent
// compilation still runs before the broker records its acceptance commit.
func healthRevalidation(r Request, paths []string) bool {
	if len(paths) != 0 || r.Actor != HealthJudge || r.Phase != InitialPhase || r.Publication == nil || !strings.HasPrefix(r.Publication.IncidentID, "health-refresh-") {
		return false
	}
	detectors, err := layout.ReadDetectors(filepath.Join(r.Root, layout.DiagnosticsDir))
	if err != nil {
		return false
	}
	for _, det := range detectors {
		if det.Class == layout.ClassHealth {
			return true
		}
	}
	return false
}
