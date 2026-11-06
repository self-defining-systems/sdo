package operability

import (
	"fmt"
	"strings"
)

// DecisionKind is what a reader does with a report.
type DecisionKind string

const (
	// Accepted: every required item has checks and none fail or are pending.
	Accepted DecisionKind = "accepted"
	// Correct: an agent owner must fix the failing checks.
	Correct DecisionKind = "correct"
	// Hold: terminal; a person or an SDO defect, no agent turn.
	Hold DecisionKind = "hold"
	// Retry: infrastructure; try again without costing an attempt.
	Retry DecisionKind = "retry"
	// Wait: nothing failed but a check is still pending.
	Wait DecisionKind = "wait"
)

// Decision is the pure result of Decide. Checks are the failing (or, for Wait,
// pending) checks the Owner must act on.
//
// Cause is the worst failing cause: the lowest rank (infrastructure, then SDO
// defects, then deployer causes, then designer causes), the first in report
// order among equals. Owner is the route of that cause, so Cause is always one
// of the Owner's checks' causes. It is empty for Wait and Accepted.
type Decision struct {
	Kind   DecisionKind
	Owner  Owner
	Cause  Cause
	Checks []Check
}

// Decide recomputes acceptance for the required items in fresh onboarding.
func (r Report) Decide(required ...ItemID) Decision { return r.DecideIn(ModeFresh, required...) }

// DecideIn recomputes acceptance for the required items, routing failures for
// the mode. A required item with no checks is an internal failure: silence is
// not a pass. An item that is not in the report counts as having no checks.
func (r Report) DecideIn(mode Mode, required ...ItemID) Decision {
	var failing, pending []Check
	for _, id := range required {
		i := r.index(id)
		if i < 0 || len(r.Items[i].Checks) == 0 {
			failing = append(failing, Check{
				ID: string(id) + ".present", Status: StatusFail, Cause: CauseInternal,
				Detail: "no checks were recorded for required item " + string(id),
			})
			continue
		}
		for _, c := range r.Items[i].Checks {
			switch c.Status {
			case StatusPass, StatusNote, StatusSkipped:
			case StatusPending:
				pending = append(pending, c)
			default: // fail and any status this version does not know
				failing = append(failing, c)
			}
		}
	}
	if len(failing) > 0 {
		worst := failing[0].Cause
		for _, c := range failing[1:] {
			if rankOf(c.Cause) < rankOf(worst) {
				worst = c.Cause
			}
		}
		owner := RouteIn(mode, worst)
		var mine []Check
		for _, c := range failing {
			if RouteIn(mode, c.Cause) == owner {
				mine = append(mine, c)
			}
		}
		return Decision{Kind: kindFor(owner), Owner: owner, Cause: worst, Checks: mine}
	}
	if len(pending) > 0 {
		return Decision{Kind: Wait, Checks: pending}
	}
	return Decision{Kind: Accepted}
}

func kindFor(o Owner) DecisionKind {
	switch o {
	case OwnerRetry:
		return Retry
	case OwnerDeployer, OwnerDesigner, OwnerResponder:
		return Correct
	}
	return Hold
}

// Feedback words the failing checks that fresh onboarding routes to owner, one
// line per check, for an agent's correction field. It is empty when none do.
func (r Report) Feedback(owner Owner) string {
	var mine []Check
	for _, it := range r.Items {
		for _, c := range it.Checks {
			if c.Status == StatusFail && Route(c.Cause) == owner {
				mine = append(mine, c)
			}
		}
	}
	return Decision{Owner: owner, Checks: mine}.Feedback()
}

// Feedback words the decision's checks, one line each.
func (d Decision) Feedback() string {
	var b strings.Builder
	for _, c := range d.Checks {
		fmt.Fprintf(&b, "%s [%s]", c.ID, c.Cause)
		if c.Detail != "" {
			b.WriteString(": " + c.Detail)
		}
		if len(c.Paths) > 0 {
			b.WriteString(" (" + strings.Join(c.Paths, ", ") + ")")
		}
		b.WriteString("\n")
	}
	return b.String()
}
