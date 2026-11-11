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
	pass := func(label string) {
		if mirror == nil {
			dir, err := evidencesink.OpenDir(cfg.SinkRoot, cfg.RunID, clock.Now())
			if err != nil {
				fmt.Fprintf(log, "evidence-sink %s: open sink: %v\n", label, err)
				return
			}
			mirror = &evidencesink.Mirror{RuntimeRoot: cfg.RuntimeRoot, LedgerRoot: cfg.LedgerRoot, Sink: dir, Now: clock.Now}
		}
		if err := mirror.Pass(); err != nil {
			fmt.Fprintf(log, "evidence-sink %s: %v\n", label, err)
		}
	}
	for {
		pass("pass")
		if cfg.Once || ctx.Err() != nil {
			return nil
		}
		if err := clock.WaitUntil(ctx, clock.Now().Add(cfg.Interval)); err != nil {
			// Termination: one last pass keeps evidence written just before it.
			pass("final pass")
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

// evidenceSinkFlags are the runner's sink choices on host and install-existing.
type evidenceSinkFlags struct {
	hostPath, volume, runID *string
}

func registerEvidenceSinkFlags(flags *flag.FlagSet) evidenceSinkFlags {
	return evidenceSinkFlags{
		hostPath: flags.String("evidence-sink-host-path", "", "node directory that receives run evidence (writable by uid 65532); outlives the control namespace"),
		volume:   flags.String("evidence-sink-volume", "", "evidence sink as a Kubernetes VolumeSource JSON (for example an NFS or Retain PersistentVolume claim)"),
		runID:    flags.String("evidence-run-id", "", "run directory inside the evidence sink (default: the control namespace)"),
	}
}

// resolve returns the compact sink JSON for install.Config.EvidenceSink, or "" when no sink is configured.
func (f evidenceSinkFlags) resolve(controlNamespace string) (string, error) {
	if *f.hostPath == "" && *f.volume == "" {
		if *f.runID != "" {
			return "", errors.New("--evidence-run-id requires an evidence sink")
		}
		return "", nil
	}
	if *f.hostPath != "" && *f.volume != "" {
		return "", errors.New("use one of --evidence-sink-host-path and --evidence-sink-volume")
	}
	sink := install.EvidenceSink{RunID: *f.runID}
	if sink.RunID == "" {
		sink.RunID = controlNamespace
	}
	if *f.hostPath != "" {
		kind := corev1.HostPathDirectoryOrCreate
		sink.Volume.HostPath = &corev1.HostPathVolumeSource{Path: *f.hostPath, Type: &kind}
	} else {
		decoder := json.NewDecoder(strings.NewReader(*f.volume))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&sink.Volume); err != nil {
			return "", fmt.Errorf("decode --evidence-sink-volume: %w", err)
		}
	}
	return sink.Format()
}
