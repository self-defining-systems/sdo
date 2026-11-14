// Package envcheck is the check an environment (a sandbox or the target) gets
// after a deploy. It assembles an operability report, the report harnesses
// already read, from a list of Items. Each Item turns what it observes into
// operability.Checks filed under its operability item (the check ID prefix),
// so a new check is one more Item in Items and never a new report format.
package envcheck

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/operability"
)

// Raise is one detector that is not clear.
type Raise struct {
	DetectorID string
	Reason     string
}

// Verdict is one evaluation of the environment's accepted detectors.
type Verdict struct {
	// Status is "clear", "pending", "unavailable" or a firing status.
	Status string
	Raised []Raise
}

// Evaluator evaluates the accepted detectors once against the environment.
type Evaluator interface {
	Evaluate(ctx context.Context) (Verdict, error)
}

// Input is everything the items observe: the commit, how its deploy ended and
// the means of looking at the environment afterwards.
type Input struct {
	Commit string
	// DeployErr is the deploy's error; nil when the rollout is ready.
	DeployErr error
	Evaluator Evaluator
	// Quick skips checks that take minutes (the quiet window).
	Quick bool
	// QuietWindow and QuietInterval bound no-raises-when-healthy.
	QuietWindow, QuietInterval time.Duration
	Now                        func() time.Time
	Sleep                      func(context.Context, time.Duration) error
}

// Item produces the checks of one report item.
type Item interface {
	Name() string
	Run(ctx context.Context, in Input) []operability.Check
}

// Items is the v1 list. Further items (metrics, routes, tests) are appended
// here by the packages that own them.
func Items() []Item { return []Item{DeployedHealthy{}, QuietWhenHealthy{}} }

// Run runs the items in order and assembles the report for the commit.
func Run(ctx context.Context, subject operability.Subject, in Input, items []Item) (operability.Report, error) {
	report := operability.New(operability.StageComplete, subject.WithUnavailable())
	for _, item := range items {
		var err error
		if report, err = report.With(item.Run(ctx, in)...); err != nil {
			return operability.Report{}, fmt.Errorf("item %s: %w", item.Name(), err)
		}
	}
	return report, nil
}

// Tally counts checks: green (pass, note, skipped) and red (everything else).
func Tally(r operability.Report) (green, red int) {
	for _, it := range r.Items {
		for _, c := range it.Checks {
			switch c.Status {
			case operability.StatusPass, operability.StatusNote, operability.StatusSkipped:
				green++
			default:
				red++
			}
		}
	}
	return green, red
}

// Render is the report as the commands print it: a tally and one line per red
// check.
func Render(r operability.Report) string {
	green, red := Tally(r)
	lines := []string{fmt.Sprintf("commit %s", short(r.Subject.AcceptedCommit)), fmt.Sprintf("check: green %d red %d", green, red)}
	var reds []string
	for _, it := range r.Items {
		for _, c := range it.Checks {
			switch c.Status {
			case operability.StatusPass, operability.StatusNote, operability.StatusSkipped:
			default:
				reds = append(reds, fmt.Sprintf("red %s: %s", c.ID, c.Detail))
			}
		}
	}
	sort.Strings(reds)
	return strings.Join(append(lines, reds...), "\n")
}

func short(commit string) string {
	if len(commit) > 8 {
		return commit[:8]
	}
	return commit
}

// DeployedHealthy is item 1: the commit rolled out and the accepted detectors
// are clear.
type DeployedHealthy struct{}

// Name implements Item.
func (DeployedHealthy) Name() string { return "deployed-healthy" }

const deployedID = "O1.deployed-healthy"

// Run implements Item.
func (DeployedHealthy) Run(ctx context.Context, in Input) []operability.Check {
	fail := func(cause operability.Cause, detail string) []operability.Check {
		return []operability.Check{{ID: deployedID, Phase: operability.PhaseLive, Status: operability.StatusFail, Cause: cause, Detail: operability.Clip(detail)}}
	}
	if in.DeployErr != nil {
		var policy *deployment.PolicyError
		if errors.As(in.DeployErr, &policy) {
			return fail(operability.CausePolicy, in.DeployErr.Error())
		}
		return fail(operability.CauseRollout, in.DeployErr.Error())
	}
	if in.Evaluator == nil {
		return fail(operability.CauseProbeUnavailable, "no detector evaluation is available")
	}
	verdict, err := in.Evaluator.Evaluate(ctx)
	if err != nil {
		return fail(operability.CauseProbeUnavailable, err.Error())
	}
	if verdict.Status != "clear" {
		return fail(operability.CauseSourceFault, "detectors report "+verdict.Status+describe(verdict.Raised))
	}
	return []operability.Check{{ID: deployedID, Phase: operability.PhaseLive, Status: operability.StatusPass, Detail: "rolled out; detectors clear"}}
}

// QuietWhenHealthy is item 5: no detector raises through the window while the
// environment is healthy. Quick skips it.
type QuietWhenHealthy struct{}

// Name implements Item.
func (QuietWhenHealthy) Name() string { return "quiet-when-healthy" }

const quietID = "O4.quiet-when-healthy"

// Run implements Item.
func (QuietWhenHealthy) Run(ctx context.Context, in Input) []operability.Check {
	check := func(status operability.Status, cause operability.Cause, detail string) []operability.Check {
		return []operability.Check{{ID: quietID, Phase: operability.PhaseLive, Status: status, Cause: cause, Detail: operability.Clip(detail)}}
	}
	switch {
	case in.DeployErr != nil:
		return check(operability.StatusSkipped, "", "nothing is deployed")
	case in.Quick:
		return check(operability.StatusSkipped, "", "skipped by --quick")
	case in.Evaluator == nil || in.Now == nil || in.Sleep == nil || in.QuietWindow <= 0 || in.QuietInterval <= 0:
		return check(operability.StatusFail, operability.CauseInternal, "the quiet window needs an evaluator, a clock, a window and an interval")
	}
	end := in.Now().Add(in.QuietWindow)
	samples := 0
	for {
		verdict, err := in.Evaluator.Evaluate(ctx)
		if err != nil {
			return check(operability.StatusFail, operability.CauseProbeUnavailable, err.Error())
		}
		samples++
		if verdict.Status != "clear" {
			return check(operability.StatusFail, operability.CauseDetectorFault,
				fmt.Sprintf("raised while healthy (sample %d): %s%s", samples, verdict.Status, describe(verdict.Raised)))
		}
		if !in.Now().Add(in.QuietInterval).Before(end) {
			return check(operability.StatusPass, "", fmt.Sprintf("no detector raised in %d samples over %s", samples, in.QuietWindow))
		}
		if err := in.Sleep(ctx, in.QuietInterval); err != nil {
			return check(operability.StatusFail, operability.CauseInfrastructure, err.Error())
		}
	}
}

func describe(raised []Raise) string {
	if len(raised) == 0 {
		return ""
	}
	parts := make([]string, 0, len(raised))
	for _, r := range raised {
		parts = append(parts, r.DetectorID+" ("+r.Reason+")")
	}
	return ": " + strings.Join(parts, "; ")
}
