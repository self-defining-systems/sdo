package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/contracts/settings"
	"sdo.dev/operator/evidencesink"
	"sdo.dev/operator/install"
	"sdo.dev/operator/operatorcli/internal/processio"
)

// EvidenceSinkConfig runs the evidence mirror: the installer's sidecar beside the controller and operate
// containers, reading the repository volume and writing the runner's sink volume.
type EvidenceSinkConfig struct {
	RuntimeRoot, LedgerRoot, SinkRoot, RunID string
	Interval                                 time.Duration
	Once                                     bool
}

func ParseEvidenceSink(args []string) (EvidenceSinkConfig, error) {
	if len(args) == 0 || args[0] != "evidence-sink" {
		return EvidenceSinkConfig{}, errors.New("expected evidence-sink subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator evidence-sink", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	runtimeRoot := flags.String("runtime-root", "/workspace/.sdo-runtime", "runtime evidence root on the repository volume")
	ledgerRoot := flags.String("ledger-root", "/workspace/worktrees/native-ledger", "native broker ledger directory")
	sinkRoot := flags.String("sink-root", install.EvidenceSinkMountPath, "mounted evidence sink")
	runID := flags.String("run-id", "", "run directory name inside the sink")
	interval := flags.Duration("interval", 10*time.Second, "time between mirror passes")
	once := flags.Bool("once", false, "run one pass and exit")
	if err := flags.Parse(args[1:]); err != nil {
		return EvidenceSinkConfig{}, err
	}
	if flags.NArg() != 0 {
		return EvidenceSinkConfig{}, fmt.Errorf("unexpected evidence-sink arguments: %v", flags.Args())
	}
	for _, root := range []string{*runtimeRoot, *sinkRoot} {
		if !filepath.IsAbs(root) {
			return EvidenceSinkConfig{}, errors.New("evidence-sink roots must be absolute")
		}
	}
	if *ledgerRoot != "" && !filepath.IsAbs(*ledgerRoot) {
		return EvidenceSinkConfig{}, errors.New("evidence-sink ledger root must be absolute")
	}
	if !evidencesink.ValidRunID(*runID) {
		return EvidenceSinkConfig{}, fmt.Errorf("invalid evidence run ID %q", *runID)
	}
	if *interval < time.Second {
		return EvidenceSinkConfig{}, errors.New("evidence-sink interval must be at least one second")
	}
	return EvidenceSinkConfig{RuntimeRoot: *runtimeRoot, LedgerRoot: *ledgerRoot, SinkRoot: *sinkRoot, RunID: *runID, Interval: *interval, Once: *once}, nil
}

// EvidenceSinkClock is the mirror loop's time seam.
type EvidenceSinkClock struct {
	Now       func() time.Time
	WaitUntil func(context.Context, time.Time) error
}

// RunEvidenceSink mirrors until ctx ends, then runs one last pass so evidence written just before termination is
// kept. A failed pass is reported and retried; it never stops the mirror.
func RunEvidenceSink(ctx context.Context, cfg EvidenceSinkConfig, clock EvidenceSinkClock, log io.Writer) error {
	if clock.Now == nil {
		clock.Now = processio.Now
	}
	if clock.WaitUntil == nil {
		clock.WaitUntil = processio.SleepUntil
	}
	// An unusable sink (for example a host directory the controller uid cannot write) is reported and retried; it
	// never blocks or fails the Pod's main container.
	var mirror *evidencesink.Mirror
	pass := func(final bool) {
		label := "pass"
		if final {
			label = "final pass"
		}
		if mirror == nil {
			dir, err := evidencesink.OpenDir(cfg.SinkRoot, cfg.RunID, clock.Now())
			if err != nil {
				fmt.Fprintf(log, "evidence-sink %s: open sink: %v\n", label, err)
				return
			}
			mirror = &evidencesink.Mirror{RuntimeRoot: cfg.RuntimeRoot, LedgerRoot: cfg.LedgerRoot, Sink: dir, Now: clock.Now}
		}
		run := mirror.Pass
		if final {
			run = mirror.FinalPass
		}
		if err := run(); err != nil {
			fmt.Fprintf(log, "evidence-sink %s: %v\n", label, err)
		}
	}
	for {
		if cfg.Once || ctx.Err() != nil {
			pass(true)
			return nil
		}
		pass(false)
		if err := clock.WaitUntil(ctx, clock.Now().Add(cfg.Interval)); err != nil {
			// Termination: one last pass keeps evidence written just before it and seals what is ready.
			pass(true)
			return nil
		}
	}
}

// RunEvidenceSinkProcess is the process entry point: SIGTERM ends the loop after a final pass.
func RunEvidenceSinkProcess(args []string, log io.Writer) error {
	cfg, err := ParseEvidenceSink(args)
	if err != nil {
		return err
	}
	ctx, stop := processio.TerminationContext(context.Background())
	defer stop()
	return RunEvidenceSink(ctx, cfg, EvidenceSinkClock{}, log)
}

// resolveEvidenceSink is the compact sink JSON of the configured sink, or "" when none is configured.
func resolveEvidenceSink(e settings.Evidence, controlNamespace string) (string, error) {
	// Validate already refused a run ID without a sink and a sink with both forms.
	if e.HostPath == "" && e.Volume == "" {
		return "", nil
	}
	sink := install.EvidenceSink{RunID: e.RunID}
	if sink.RunID == "" {
		sink.RunID = controlNamespace
	}
	if e.HostPath != "" {
		kind := corev1.HostPathDirectoryOrCreate
		sink.Volume.HostPath = &corev1.HostPathVolumeSource{Path: e.HostPath, Type: &kind}
	} else {
		decoder := json.NewDecoder(strings.NewReader(e.Volume))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&sink.Volume); err != nil {
			return "", fmt.Errorf("decode evidence.volume: %w", err)
		}
	}
	return sink.Format()
}
