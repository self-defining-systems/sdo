package operatorcli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
)

func failingReport(t *testing.T, cause operability.Cause) operability.Report {
	t.Helper()
	subject := operability.Subject{AcceptedCommit: "c", BaseTree: "b", EffectiveTree: "e", PatchesDigest: "p", RenderDigest: "r", EnvDigest: "v"}
	report, err := operability.New(operability.StageDeployment, subject).With(
		operability.Check{ID: "O1.render", Phase: operability.PhaseOffline, Status: operability.StatusFail, Cause: cause, Detail: "synthetic failure"},
		operability.Check{ID: "O2.images", Phase: operability.PhaseOffline, Status: operability.StatusPass},
		operability.Check{ID: "O6.scope", Phase: operability.PhaseOffline, Status: operability.StatusPass},
	)
	if err != nil {
		t.Fatal(err)
	}
	return report
}

// A run that ends on an operability decision leaves the report where the harness
// looks: the summary in the termination message (it must fit 4 KiB) and the
// full report on one stderr line.
func TestTerminalOperabilityWritesTheTerminationMessageAndTheLogLine(t *testing.T) {
	report := failingReport(t, operability.CauseAttribution)
	decision := report.Decide(operability.StageItems(operability.StageDeployment)...)
	path := filepath.Join(t.TempDir(), "termination-log")
	var stderr bytes.Buffer
	err := fmt.Errorf("deployment attempt 1: %w", &lifecycle.OperabilityError{Report: report, Decision: decision})
	if !reportTerminalOperability(err, &stderr, path, time.Unix(0, 0)) {
		t.Fatal("an operability error was not reported")
	}
	message, rerr := os.ReadFile(path)
	if rerr != nil || len(message) > terminationMessageLimit {
		t.Fatalf("termination message %d bytes, err %v", len(message), rerr)
	}
	var stop terminalStop
	if err := json.Unmarshal(message, &stop); err != nil {
		t.Fatal(err)
	}
	if stop.Decision != string(operability.Hold) || stop.Owner != string(operability.OwnerOperator) || stop.Summary.Items[0].FirstFailedCheck != "O1.render" || len(stop.Report) != 0 {
		t.Fatalf("termination summary = %+v", stop)
	}
	line := strings.TrimPrefix(strings.TrimSpace(stderr.String()), "SDO-OPERABILITY ")
	if line == strings.TrimSpace(stderr.String()) || strings.Contains(line, "\n") {
		t.Fatalf("stderr = %q, want one SDO-OPERABILITY line", stderr.String())
	}
	var full terminalStop
	if err := json.Unmarshal([]byte(line), &full); err != nil {
		t.Fatal(err)
	}
	if _, err := operability.Decode(full.Report); err != nil {
		t.Fatalf("logged report does not decode: %v", err)
	}
}

// A provided plan's stop carries the report and is decided for the provided
// mode (a deployment fault is the person's).
func TestTerminalOperabilityDecidesAProvidedPlanForThePerson(t *testing.T) {
	report := failingReport(t, operability.CauseRender)
	var stderr bytes.Buffer
	err := &lifecycle.ProvidedPlanError{Stage: "deploy", Reason: "x", Report: &report}
	if !reportTerminalOperability(err, &stderr, "", time.Unix(0, 0)) || !strings.Contains(stderr.String(), `"owner":"human"`) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

// Errors without a report are not operability stops and write nothing.
func TestTerminalOperabilityIgnoresOtherErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termination-log")
	var stderr bytes.Buffer
	for _, err := range []error{errors.New("plain"), &lifecycle.ProvidedPlanError{Stage: "verify", Reason: "x"}} {
		if reportTerminalOperability(err, &stderr, path, time.Unix(0, 0)) {
			t.Fatalf("%v was reported as an operability stop", err)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) || stderr.Len() != 0 {
		t.Fatalf("termination file err=%v stderr=%q, want nothing written", err, stderr.String())
	}
}
