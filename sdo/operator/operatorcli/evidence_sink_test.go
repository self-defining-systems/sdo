package operatorcli

import (
	"bytes"
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/evidencesink"
	"sdo.dev/operator/install"
)

func TestEvidenceSinkFlagsResolveToInstallConfig(t *testing.T) {
	parse := func(args ...string) (string, error) {
		flags := flag.NewFlagSet("t", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		sinkFlags := registerEvidenceSinkFlags(flags)
		if err := flags.Parse(args); err != nil {
			return "", err
		}
		return sinkFlags.resolve("app-sdo")
	}
	if got, err := parse(); err != nil || got != "" {
		t.Fatalf("no flags = %q, %v; want no sink", got, err)
	}
	encoded, err := parse("--evidence-sink-host-path", "/var/sdo-evidence")
	if err != nil {
		t.Fatal(err)
	}
	sink, err := install.ParseEvidenceSink(encoded)
	if err != nil || sink.RunID != "app-sdo" || sink.Volume.HostPath == nil || sink.Volume.HostPath.Path != "/var/sdo-evidence" {
		t.Fatalf("host path sink = %+v, %v", sink, err)
	}
	encoded, err = parse("--evidence-sink-volume", `{"nfs":{"server":"nfs.example","path":"/evidence"}}`, "--evidence-run-id", "row-7")
	if err != nil {
		t.Fatal(err)
	}
	if sink, err = install.ParseEvidenceSink(encoded); err != nil || sink.RunID != "row-7" || sink.Volume.NFS == nil {
		t.Fatalf("volume sink = %+v, %v", sink, err)
	}
	for _, args := range [][]string{
		{"--evidence-run-id", "row-7"},
		{"--evidence-sink-host-path", "/a", "--evidence-sink-volume", `{"nfs":{"server":"s","path":"/e"}}`},
		{"--evidence-sink-volume", `{"emptyDir":{}}`},
		{"--evidence-sink-volume", `{"hostPath":{"path":"/e"},"bogus":1}`},
	} {
		if _, err := parse(args...); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

func TestEvidenceSinkLoopRunsAFinalPassOnTermination(t *testing.T) {
	root := t.TempDir()
	runtimeRoot, sinkRoot := filepath.Join(root, ".sdo-runtime"), filepath.Join(root, "sink")
	for _, directory := range []string{filepath.Join(runtimeRoot, "usage"), sinkRoot} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := ParseEvidenceSink([]string{"evidence-sink", "--runtime-root", runtimeRoot, "--ledger-root", "", "--sink-root", sinkRoot, "--run-id", "run-1"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	waits := 0
	clock := EvidenceSinkClock{
		Now: func() time.Time { return now },
		WaitUntil: func(ctx context.Context, until time.Time) error {
			waits++
			now = until
			if waits == 2 {
				// Evidence written just before termination must reach the sink.
				if err := os.WriteFile(filepath.Join(runtimeRoot, "usage", "late.jsonl"), []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				cancel()
				return ctx.Err()
			}
			return nil
		},
	}
	var log bytes.Buffer
	if err := RunEvidenceSink(ctx, cfg, clock, &log); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(sinkRoot, "run-1", "usage", "late.jsonl")); err != nil {
		t.Fatalf("final pass missed late evidence: %v (log %q)", err, log.String())
	}
	if _, err := os.Stat(filepath.Join(sinkRoot, "run-1", evidencesink.LayoutFile)); err != nil {
		t.Fatal(err)
	}
}

func TestEvidenceSinkLoopSurvivesAnUnusableSink(t *testing.T) {
	root := t.TempDir()
	cfg := EvidenceSinkConfig{RuntimeRoot: filepath.Join(root, ".sdo-runtime"), SinkRoot: filepath.Join(root, "missing"), RunID: "run-1", Interval: time.Second, Once: true}
	var log bytes.Buffer
	if err := RunEvidenceSink(context.Background(), cfg, EvidenceSinkClock{Now: func() time.Time { return time.Unix(0, 0) }}, &log); err != nil {
		t.Fatalf("an unusable sink stopped the mirror: %v", err)
	}
	if !strings.Contains(log.String(), "open sink") {
		t.Fatalf("log = %q", log.String())
	}
}
