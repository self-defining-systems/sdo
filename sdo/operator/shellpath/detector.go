package shellpath

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/sdk"
)

type Reader interface {
	Read(context.Context) (Report, error)
}

// Detector is a stable adapter over a changing accepted script set. Broker path
// ownership stays per script; every fired script has its own finding identity.
// The shared controller still owns incident scheduling and independent closure.
type Detector struct {
	Reader            Reader
	Namespace, Commit string
	mu                sync.Mutex
	latest            Report
	lastError         error
}

func (d *Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "shell.detectors", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Description: "Accepted shell detector results", Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning}, OriginatingCommit: d.Commit}
}
func (d *Detector) Detect(ctx context.Context, _ sdk.DetectionContext) ([]sdk.Finding, error) {
	report, err := d.Reader.Read(ctx)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.latest = report
	d.lastError = err
	if err != nil {
		return nil, err
	}
	findings := []sdk.Finding{}
	detectorErrors := []string{}
	for _, r := range report.Results {
		if r.Error != "" || r.TimedOut || r.ExitCode < 0 {
			detectorErrors = append(detectorErrors, r.Script+": detector error: "+r.Error)
			continue
		}
		if !r.Fired || r.ExitCode == 0 {
			continue
		}
		digest := sha256.Sum256([]byte(r.Script))
		findings = append(findings, sdk.Finding{DetectorID: d.Spec().ID, RuleID: fmt.Sprintf("script.%x", digest[:8]), Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Shell detector fired: " + r.Script, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: d.Namespace}, Evidence: fmt.Sprintf("script=%s exit_code=%d at=%s\nstdout tail:\n%s\nstderr tail:\n%s", r.Script, r.ExitCode, r.At.Format(time.RFC3339Nano), r.Stdout, r.Stderr), Metadata: map[string]any{"script": r.Script, "accepted_commit": report.AcceptedCommit, "activated_at": report.ActivatedAt.Format(time.RFC3339Nano)}})
	}
	if len(detectorErrors) > 0 && len(findings) == 0 {
		return nil, fmt.Errorf("%s", strings.Join(detectorErrors, "; "))
	}
	return findings, nil
}
func (d *Detector) Confirmed(sdk.DetectionContext) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastError != nil || d.latest.Error != "" || len(d.latest.Results) == 0 {
		return false
	}
	for _, r := range d.latest.Results {
		if r.ExitCode != 0 || r.Error != "" || r.TimedOut {
			return false
		}
	}
	return true
}
func (d *Detector) ConfirmationReason(sdk.DetectionContext) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var reasons []string
	for _, r := range d.latest.Results {
		if r.Error != "" || r.TimedOut || r.ExitCode < 0 {
			reasons = append(reasons, r.Script+": detector error: "+r.Error)
		} else if r.ExitCode != 0 && !r.Fired {
			reasons = append(reasons, r.Script+": awaiting second nonzero run")
		}
	}
	return strings.Join(reasons, "; ")
}
