package lifecycle

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability"
)

// AgentNotConvergingError ends a lifecycle whose agent keeps failing the same
// way: its last two proposals were rejected for the same rules and paths, or a
// round spent its retry budget. Another round only repeats the failure and
// costs an agent session, so the run stops with the typed cause. Role is the
// agent's role (the owner of the failure): OwnerDesigner for the health judge,
// OwnerDeployer for the deployer. It is an *OperabilityError, so the terminal
// report, termination message and lane exit carry it unchanged.
type AgentNotConvergingError struct {
	Role operability.Owner
	// Repeated names what repeated: "rule@path" for validator rejections, the
	// correction text for feedback the agent could not act on.
	Repeated []string
	// Budget is true when the retry budget ended the run rather than a repeat.
	Budget bool
	*OperabilityError
}

func (e *AgentNotConvergingError) Error() string {
	why := "repeated the same rejection"
	if e.Budget {
		why = "spent its retry budget"
	}
	return fmt.Sprintf("agent_not_converging: the %s agent %s: %s", e.Role, why, strings.Join(e.Repeated, "; "))
}

func (e *AgentNotConvergingError) Unwrap() error { return e.OperabilityError }

// BaseInvalidError ends a lifecycle whose accepted base fails the shared
// validator before any agent edit. The agent cannot fix what it did not write:
// the cause is SDO's (owner operator), and no agent turn is spent.
type BaseInvalidError struct {
	// Diagnostics are the base's own failures, as "rule@path: message".
	Diagnostics []string
	*OperabilityError
}

func (e *BaseInvalidError) Error() string {
	return "base_invalid: the accepted base fails validation before any agent edit (SDO's defect, no agent turn spent): " + strings.Join(e.Diagnostics, "; ")
}

func (e *BaseInvalidError) Unwrap() error { return e.OperabilityError }

// terminalStop builds the typed stop: a one-check report (deployment stage,
// unavailable subject bound only to the commit) and the hold decision the
// harness reads. The decision is constructed, not recomputed, because the
// owner of a not-converging stop is the agent's role.
func terminalStop(owner operability.Owner, item operability.ItemID, id string, cause operability.Cause, commit, detail string, paths []string) *OperabilityError {
	subject := operability.Subject{AcceptedCommit: commit}.WithUnavailable()
	check := operability.Check{ID: string(item) + "." + id, Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: cause, Detail: operability.Clip(detail), Paths: paths}
	report, err := operability.New(operability.StageDeployment, subject).With(check)
	if err != nil {
		// The item and ID are constants of this package.
		panic(err)
	}
	return &OperabilityError{Report: report, Decision: operability.Decision{Kind: operability.Hold, Owner: owner, Checks: []operability.Check{check}}}
}

// NewAgentNotConvergingError builds the typed stop for an agent role (the owner).
func NewAgentNotConvergingError(role operability.Owner, commit string, repeated []string, budget bool) *AgentNotConvergingError {
	item := operability.O1
	if role == operability.OwnerDesigner {
		item = operability.O5
	}
	why := "same rejection twice in a row"
	if budget {
		why = "retry budget spent"
	}
	detail := fmt.Sprintf("%s agent: %s: %s", role, why, strings.Join(repeated, "; "))
	return &AgentNotConvergingError{Role: role, Repeated: repeated, Budget: budget,
		OperabilityError: terminalStop(role, item, "agent_not_converging", operability.CauseAgentNotConverging, commit, detail, nil)}
}

// NewBaseInvalidError builds the typed stop for a base that fails validation.
func NewBaseInvalidError(commit string, diagnostics []*contracts.MemoryDiagnostic) *BaseInvalidError {
	lines := make([]string, 0, len(diagnostics))
	var paths []string
	for _, d := range diagnostics {
		lines = append(lines, fmt.Sprintf("%s@%s: %s", d.GetRule(), d.GetPath(), d.GetMessage()))
		if d.GetPath() != "" && !slices.Contains(paths, d.GetPath()) && len(paths) < 8 {
			paths = append(paths, d.GetPath())
		}
	}
	return &BaseInvalidError{Diagnostics: lines,
		OperabilityError: terminalStop(operability.OwnerOperator, operability.O5, "base_invalid", operability.CauseBaseInvalid, commit, "base fails validation before any agent edit: "+strings.Join(lines, "; "), paths)}
}

// rejectionSignature is what one rejection says about the agent's work: the
// sorted full normalized diagnostics (rule, path, message) it carries, and the
// sorted "rule@path" set for naming them. Both are nil when the rejection says
// nothing about the agent's work: a lost worker is infrastructure, and an
// operability-gate correction has its own discriminator (reattribute) that
// turns a repeated designer firing into the application's fault.
func rejectionSignature(rejection *contracts.ProposalRejection) (full, named []string) {
	for _, d := range rejection.GetDiagnostics() {
		rule := d.GetRule()
		if strings.HasPrefix(rule, operabilityRulePrefix) || rule == vanishedWorkerRule || rule == jobFailedRule {
			return nil, nil
		}
		key := rule + "@" + d.GetPath()
		if !slices.Contains(named, key) {
			named = append(named, key)
		}
		if detail := key + ": " + normalizeDiagnosticMessage(d.GetMessage()); !slices.Contains(full, detail) {
			full = append(full, detail)
		}
	}
	slices.Sort(full)
	slices.Sort(named)
	return full, named
}

var (
	volatileTimestamp = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?`)
	volatileDigest    = regexp.MustCompile(`\b(?:[0-9a-f]{7,64}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b`)
	spaceRun          = regexp.MustCompile(`\s+`)
)

// normalizeDiagnosticMessage removes the parts of a diagnostic message that
// change between identical failures: timestamps, commit and content digests,
// UUIDs, and whitespace layout. Everything else (names, counts, wording) stays,
// so a changed failure of the same rule at the same path is a different
// rejection.
func normalizeDiagnosticMessage(message string) string {
	message = volatileTimestamp.ReplaceAllString(message, "<time>")
	message = volatileDigest.ReplaceAllStringFunc(message, func(m string) string {
		if strings.ContainsAny(m, "0123456789") {
			return "<digest>"
		}
		return m
	})
	return strings.TrimSpace(spaceRun.ReplaceAllString(message, " "))
}

// judgeStop says whether the judge's rejection log ends the run: the retry
// budget is spent, or the last two rejections carry the same normalized
// diagnostics (rule, path and message).
func judgeStop(log *contracts.ProposalRejectionLog, commit string) error {
	rejections := log.GetRejections()
	if n := len(rejections); n >= 2 {
		last, named := rejectionSignature(rejections[n-1])
		previous, _ := rejectionSignature(rejections[n-2])
		if len(last) > 0 && slices.Equal(last, previous) {
			return NewAgentNotConvergingError(operability.OwnerDesigner, commit, named, false)
		}
	}
	if log.GetTerminal() {
		var rules []string
		for _, d := range rejectionHistory(rejections) {
			key := d.GetRule() + "@" + d.GetPath()
			if !slices.Contains(rules, key) {
				rules = append(rules, key)
			}
		}
		return NewAgentNotConvergingError(operability.OwnerDesigner, commit, rules, true)
	}
	return nil
}

// feedbackTracker spots an agent given the same correction twice in a row.
type feedbackTracker struct {
	last map[operability.Owner]string
}

// repeated returns the stop when feedback equals the role's previous feedback.
func (t *feedbackTracker) repeated(role operability.Owner, commit, feedback string) error {
	if t.last == nil {
		t.last = map[operability.Owner]string{}
	}
	previous := t.last[role]
	t.last[role] = feedback
	if strings.TrimSpace(feedback) == "" || feedback != previous {
		return nil
	}
	return NewAgentNotConvergingError(role, commit, []string{operability.Clip(feedback)}, false)
}
