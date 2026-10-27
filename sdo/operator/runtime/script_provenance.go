package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// ScriptUnprovenancedError refuses executable bytes without successful original
// receipt lineage. Callers must hand over rather than retry the same revision.
type ScriptUnprovenancedError struct {
	ProgramID, Digest string
	Cause             error
}

func (e *ScriptUnprovenancedError) Error() string {
	return fmt.Sprintf("script_unprovenanced: program %s script %s: %v", e.ProgramID, e.Digest, e.Cause)
}
func (e *ScriptUnprovenancedError) Unwrap() error { return e.Cause }

// ScriptProvenance is supplied by the runtime, never by authored programs.
// Its zero value grants no script authority.
type ScriptProvenance struct {
	check func(context.Context, string, string) error
}

// ReplayScriptProvenance checks informational replay against that case's own
// receipts. Live callers use accepted Git memory instead.
func ReplayScriptProvenance(c sdktest.CorpusCase) ScriptProvenance {
	return ScriptProvenance{check: func(_ context.Context, id, script string) error {
		for _, owner := range c.RootCauseProgramIDs {
			if owner == id {
				_, err := sdktest.ScriptOrigin(program.RunScript{Script: script}, c)
				return err
			}
		}
		return fmt.Errorf("corpus does not record this program")
	}}
}

func (l *ResponderLoop) scriptProvenance(id string) ScriptProvenance {
	request := l.Request
	revision := l.programRevision(id)
	return ScriptProvenance{check: func(ctx context.Context, programID, script string) error {
		if programID != id {
			return fmt.Errorf("program identity changed")
		}
		if strings.HasPrefix(revision, workingTreeRevisionPrefix) {
			return fmt.Errorf("program revision is not accepted")
		}
		if request.RepositoryWorktree == "" || !gitRevision.MatchString(request.SourceCommit) || !programIDPath.MatchString(programID) {
			return fmt.Errorf("accepted memory unavailable")
		}
		// Read only immutable Git blobs from the incident's accepted commit. A dirty
		// checkout, another program's case, or newer memory cannot grant authority.
		git := func(args ...string) ([]byte, error) {
			return exec.CommandContext(ctx, "git", append([]string{"-C", request.RepositoryWorktree}, args...)...).Output()
		}
		files, err := git("ls-tree", "-r", "--name-only", request.SourceCommit, "--", ".sdo/corpus/")
		if err != nil {
			return err
		}
		for _, file := range strings.Split(string(files), "\n") {
			if path.Base(file) != "labels.json" {
				continue
			}
			data, err := git("show", request.SourceCommit+":"+file)
			if err != nil {
				return err
			}
			var labels struct {
				Programs   []string                 `json:"root_cause_program_ids"`
				Origins    map[string]string        `json:"script_origins"`
				Provenance sdktest.CorpusProvenance `json:"provenance"`
			}
			if err := json.Unmarshal(data, &labels); err != nil {
				return err
			}
			owns := false
			for _, owner := range labels.Programs {
				owns = owns || owner == programID
			}
			if !owns || labels.Origins[program.ScriptDigest(script)] == "" {
				continue
			}
			data, err = git("show", request.SourceCommit+":"+path.Join(path.Dir(file), "recordings.json"))
			if err != nil {
				return err
			}
			c := sdktest.CorpusCase{Provenance: labels.Provenance, ScriptOrigins: labels.Origins}
			if err := json.Unmarshal(data, &c.Recordings); err != nil {
				return err
			}
			if _, err := sdktest.ScriptOrigin(program.RunScript{Script: script}, c); err == nil {
				return nil
			}
		}
		return fmt.Errorf("no original successful timed receipt for these bytes in accepted program memory")
	}}
}

func checkPlanScriptProvenance(ctx context.Context, id string, plan program.Plan, origins []ScriptProvenance) error {
	for _, action := range plan.Actions {
		script, ok := concreteExternalAction(action).(program.RunScript)
		if !ok {
			continue
		}
		err := fmt.Errorf("accepted script provenance unavailable")
		if len(origins) == 1 && origins[0].check != nil {
			err = origins[0].check(ctx, id, script.Script)
		}
		if err != nil {
			return &ScriptUnprovenancedError{ProgramID: id, Digest: program.ScriptDigest(script.Script), Cause: err}
		}
	}
	return nil
}

func mitigationPlanningReason(err error, fallback string) string {
	var provenanceError *ScriptUnprovenancedError
	if errors.As(err, &provenanceError) {
		return "script_unprovenanced"
	}
	return fallback
}

// Stage reports already persist terminal attempts. Restore the planning refusal
// before diagnosis prepares any plans after controller/worker restart.
func (l *ResponderLoop) restoreScriptRefusals(report *contracts.StageReport) {
	for _, attempt := range report.GetMitigations() {
		if attempt.GetSkipReason() != "script_unprovenanced" || attempt.GetProgramRevision() == "" {
			continue
		}
		if l.scriptRefusals == nil {
			l.scriptRefusals = map[string]error{}
		}
		key := attempt.GetPlaybookId() + "\x00" + attempt.GetProgramRevision()
		l.scriptRefusals[key] = &ScriptUnprovenancedError{ProgramID: attempt.GetPlaybookId(), Cause: fmt.Errorf("previously refused accepted program revision")}
	}
}
