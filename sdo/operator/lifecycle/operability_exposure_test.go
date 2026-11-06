package lifecycle_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/operability"
)

// exposingInstaller records the order of exposure and installation.
type exposingInstaller struct {
	events    []string
	published []operability.Report
	namespace string
}

func (i *exposingInstaller) Install(context.Context, string, string) error {
	i.events = append(i.events, "install")
	return nil
}

func (i *exposingInstaller) PublishOperability(_ context.Context, namespace string, report operability.Report) error {
	i.events = append(i.events, "publish")
	i.namespace = namespace
	i.published = append(i.published, report)
	return nil
}

// The accepted health commit's report, read back from the broker, is exposed
// before the controller is installed, so the summary exists when the lane
// reports installed.
func TestVerifiedReportIsExposedBeforeTheControllerIsInstalled(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	installer := &exposingInstaller{}
	h.coordinator.Installer = installer
	if _, err := h.coordinator.Run(context.Background(), config()); err != nil {
		t.Fatal(err)
	}
	staged := h.coordinator.Broker.(*fakeBroker).candidates[0]
	if strings.Join(installer.events, ",") != "publish,install" || installer.namespace != config().Namespace {
		t.Fatalf("events = %v in namespace %q, want publish then install in the application namespace", installer.events, installer.namespace)
	}
	report := installer.published[0]
	if report.Stage != operability.StageComplete || report.Subject.AcceptedCommit != staged.Commit {
		t.Fatalf("exposed %s report of %s, want the complete report of the staged commit %s", report.Stage, report.Subject.AcceptedCommit, staged.Commit)
	}
}

// A lane that cannot show its report does not report installed.
func TestMissingReportStopsBeforeInstall(t *testing.T) {
	h := newHarness(t, &fakeVerifier{verdicts: []lifecycle.HealthVerdict{clear}})
	installer := &exposingInstaller{}
	h.coordinator.Installer = installer
	h.coordinator.Broker.(*fakeBroker).hideReports = true
	if _, err := h.coordinator.Run(context.Background(), config()); err == nil || !strings.Contains(err.Error(), "no operability report") {
		t.Fatalf("Run = %v, want a missing-report error", err)
	}
	if len(installer.events) != 0 {
		t.Fatalf("events = %v, want nothing installed or exposed", installer.events)
	}
}

type exposingStrategy struct {
	capturingStrategy
	control string
	at      time.Time
}

func (s *exposingStrategy) PublishOperability(_ context.Context, control string, _ operability.Report, observedAt time.Time) error {
	s.control, s.at = control, observedAt
	return nil
}

// The adapter exposes in the control namespace when configured, else the
// application namespace, stamped by its clock; a strategy that cannot expose
// exposes nothing.
func TestAdapterExposesInTheControlNamespace(t *testing.T) {
	now := time.Date(2026, 11, 6, 1, 2, 3, 0, time.UTC)
	strategy := &exposingStrategy{}
	adapter := lifecycle.NativeInstallAdapter{Strategy: strategy, Now: func() time.Time { return now }}
	if err := adapter.PublishOperability(context.Background(), "app-ns", operability.Report{}); err != nil || strategy.control != "app-ns" || !strategy.at.Equal(now) {
		t.Fatalf("control=%q at=%v err=%v", strategy.control, strategy.at, err)
	}
	adapter.Config = install.Config{ControlNamespace: "sdo-control"}
	if err := adapter.PublishOperability(context.Background(), "app-ns", operability.Report{}); err != nil || strategy.control != "sdo-control" {
		t.Fatalf("control=%q err=%v", strategy.control, err)
	}
	if err := (lifecycle.NativeInstallAdapter{Strategy: strategy}).PublishOperability(context.Background(), "ns", operability.Report{}); err == nil {
		t.Fatal("exposed without a clock")
	}
	if err := (lifecycle.NativeInstallAdapter{Strategy: &capturingStrategy{}}).PublishOperability(context.Background(), "ns", operability.Report{}); err != nil {
		t.Fatalf("a strategy without exposure = %v", err)
	}
}
