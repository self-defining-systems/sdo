package runtime

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/checktransport"
)

// Real worker RPC client -> real incident-bound server -> real shared broker
// Check -> publication. A forged report is never the authority on either edge.
func TestReflectionTrustedCheckTransportToPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	calls := 0
	b, err := broker.New(root, t.TempDir(), func(context.Context, string, []string) error { calls++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	b.ValidationToolchain = "synthetic-immutable-toolchain"
	if _, err := b.RecordOutcome(ctx, "rpc", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "rpc", Classification: "success"}); err != nil {
		t.Fatal(err)
	}
	p, err := b.CreateProposalForPhase(ctx, "rpc", broker.ReflectionPhase)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(p.Path, ".sdo/playbooks/base/README.md")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString("\nSynthetic refined repair.\n")
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "tools")
	d := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}}
	executor := reflectionCheckExecutor{Directory: directory, Handle: reflectionCheckHandler(b, "", "rpc"), Executor: reflectionTaskExecutorFunc(func(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
		report, err := checktransport.Check(ctx, directory, d)
		if err != nil {
			return nil, err
		}
		if len(report.GetDiagnostics()) > 0 {
			t.Errorf("check rejected: %v", report)
		}
		// Decisions can be larger than the responder tool transport's default
		// 64 KiB. They must reach validation, rather than fail transport truncation.
		large := &contracts.ReflectionDecision{LearningDecision: "no_change", NoChangeReason: strings.Repeat("synthetic ", 16*1024)}
		rejected, err := checktransport.Check(ctx, directory, large)
		if err != nil {
			return nil, err
		}
		if len(rejected.GetDiagnostics()) == 0 {
			t.Error("no_change accepted the edited tree")
		}
		return &contracts.AgentResult{TaskId: task.GetTaskId()}, nil
	})}
	if _, err := executor.Execute(ctx, &contracts.AgentTask{TaskId: "synthetic-check"}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("self check calls=%d", calls)
	}
	if _, err := b.PublishReflection(ctx, "rpc", d); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("publication calls=%d", calls)
	}
}

func TestImmutableValidationImageRequiresCompleteDigest(t *testing.T) {
	for _, test := range []struct {
		image string
		want  bool
	}{
		{"validator:mutable", false},
		{"validator@sha256:bad", false},
		{"validator@sha256:" + strings.Repeat("z", 64), false},
		{"validator@sha256:" + strings.Repeat("a", 64), true},
	} {
		if got := immutableValidationImage(test.image); got != test.want {
			t.Errorf("%q immutable=%v", test.image, got)
		}
	}
}
