package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// learningReplayCredit supplements the broker's immutable acceptance evidence
// with an explicit binding-scoped replay receipt. It never changes the original
// outcome and never claims the learned program performed the original repair.
// The caller has checked independent success, introducing-commit provenance,
// captured hashes and equality of the accepted program dependency revisions.
func (d KubernetesJobDispatcher) learningReplayCredit(ctx context.Context, request IncidentRequest, learned memory.ProgramLearningEvidence, revision string, record *contracts.OutcomeRecord) (*contracts.OutcomeRecord, error) {
	if d.LoopFactory == nil {
		return nil, nil
	}
	loop := d.LoopFactory(request)
	if loop == nil || loop.Programs == nil {
		return nil, nil
	}
	diagnoser, ok := loop.Programs.Diagnoser(learned.ProgramID)
	if !ok {
		return nil, nil
	}
	git := func(args ...string) ([]byte, error) {
		return exec.CommandContext(ctx, "git", append([]string{"-C", request.RepositoryWorktree}, args...)...).Output()
	}
	listing, err := git("ls-tree", "-r", "--name-only", learned.AcceptedCommit, "--", layout.CorpusDir+"/")
	if err != nil {
		return nil, err
	}
	committed, err := git("show", "-s", "--format=%ct", learned.AcceptedCommit)
	if err != nil {
		return nil, err
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(string(committed)), 10, 64)
	if err != nil {
		return nil, err
	}
	acceptedAt := time.Unix(seconds, 0).UTC()
	for _, name := range strings.Split(strings.TrimSpace(string(listing)), "\n") {
		if !strings.HasSuffix(name, "/labels.json") {
			continue
		}
		raw, err := git("show", learned.AcceptedCommit+":"+name)
		if err != nil {
			return nil, err
		}
		var labels struct {
			RootCauseProgramIDs []string                 `json:"root_cause_program_ids"`
			Bindings            program.Bindings         `json:"bindings"`
			Provenance          sdktest.CorpusProvenance `json:"provenance"`
			Synthetic           string                   `json:"synthetic_no_match_reason"`
		}
		if json.Unmarshal(raw, &labels) != nil || labels.Provenance.IncidentID != learned.IncidentID || labels.Synthetic != "" {
			continue
		}
		selected := false
		for _, id := range labels.RootCauseProgramIDs {
			selected = selected || id == learned.ProgramID
		}
		if !selected {
			continue
		}
		if err := program.ValidateBindings(diagnoser.Spec(), labels.Bindings); err != nil {
			continue
		}
		c := sdktest.CorpusCase{Bindings: labels.Bindings}
		valid := true
		for _, item := range []struct {
			name, digest string
			target       any
		}{{"open.json", labels.Provenance.OpenSHA256, &c.Open}, {"fixed.json", labels.Provenance.FixedSHA256, &c.Fixed}, {"healthy.json", labels.Provenance.FixedSHA256, &c.Healthy}, {"recordings.json", "", &c.Recordings}} {
			data, err := git("show", learned.AcceptedCommit+":"+path.Join(path.Dir(name), item.name))
			if err != nil {
				valid = false
				break
			}
			if item.digest != "" && fmt.Sprintf("%x", sha256.Sum256(data)) != item.digest {
				valid = false
				break
			}
			if json.Unmarshal(data, item.target) != nil {
				valid = false
				break
			}
		}
		if !valid || c.Open.NamespaceName == "" || c.Open.NamespaceName != c.Fixed.NamespaceName || c.Fixed.NamespaceName != c.Healthy.NamespaceName {
			continue
		}
		open := program.RunDiagnose(ctx, diagnoser, &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, labels.Bindings, loop.deadline())
		fixed := program.RunDiagnose(ctx, diagnoser, &sdktest.ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}, labels.Bindings, loop.deadline())
		if open.Result != program.Match || fixed.Result != program.NoMatch {
			continue
		}
		run := ProgramRun{ProgramID: learned.ProgramID, ProgramRevision: revision, Bindings: cloneProgramBindings(labels.Bindings), Verdict: fixed}
		applicationID := "learned-repair-" + learned.ProgramID
		planID := "learned-plan-" + learned.ProgramID
		encode := func(value any) (string, error) { data, err := json.Marshal(value); return string(data), err }
		planRequest, err := encode(ToolRequest{Command: "plan", ProgramID: learned.ProgramID, Bindings: labels.Bindings})
		if err != nil {
			return nil, err
		}
		planResponse, err := encode(ToolResponse{Command: "plan", PlanID: planID})
		if err != nil {
			return nil, err
		}
		applyRequest, err := encode(ToolRequest{Command: "apply", PlanIDs: []string{planID}})
		if err != nil {
			return nil, err
		}
		applyResponse, err := encode(ToolResponse{Command: "apply", ApplicationID: applicationID, Healthy: true, ProgramRuns: []ProgramRun{run}})
		if err != nil {
			return nil, err
		}
		credited := proto.Clone(record).(*contracts.OutcomeRecord)
		credited.ProgramRuns = append(credited.ProgramRuns, programRunContract(run))
		credited.PlanApplications = append(credited.PlanApplications, &contracts.PlanApplication{ApplicationId: applicationID, ProgramIds: []string{learned.ProgramID}, Applied: true, CompletedAt: timestamppb.New(acceptedAt), Verification: &contracts.Verdict{Result: contracts.Verdict_RESULT_NO_MATCH}})
		// These transient plan/apply-shaped receipts encode the independently
		// validated learning replay for the unchanged trust predicate. They are
		// never published as live tool calls or written back to durable outcomes.
		summary := "broker-validated learning replay: open Match, fixed NoMatch; accepted " + learned.AcceptedCommit
		credited.ProgramToolCalls = append(credited.ProgramToolCalls, &contracts.ProgramToolCall{Command: "plan", At: timestamppb.New(acceptedAt), RequestJson: planRequest, ResponseJson: planResponse, Summary: summary}, &contracts.ProgramToolCall{Command: "apply", At: timestamppb.New(acceptedAt), RequestJson: applyRequest, ResponseJson: applyResponse, Summary: summary})
		return credited, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return nil, nil
}
