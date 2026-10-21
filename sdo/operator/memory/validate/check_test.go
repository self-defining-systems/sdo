package validate

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckRequiresAllAcceptanceInputs(t *testing.T) {
	for _, test := range []struct {
		name string
		req  Request
	}{
		{"base", Request{Root: t.TempDir(), Actor: Responder, Phase: ReflectionPhase}},
		{"actor", Request{Root: t.TempDir(), Base: "HEAD", Phase: ReflectionPhase}},
		{"phase", Request{Root: t.TempDir(), Base: "HEAD", Actor: Responder}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Check(context.Background(), test.req, func(context.Context, Request) ([]Diagnostic, error) {
				t.Fatal("runner called without inputs")
				return nil, nil
			}); err == nil {
				t.Fatal("missing input accepted")
			}
		})
	}
	if _, err := Check(context.Background(), Request{Root: t.TempDir(), Base: "HEAD", Actor: Responder, Phase: ReflectionPhase}, nil); err == nil {
		t.Fatal("missing runner accepted")
	}
}

func TestCheckCannotSelectPartialLayers(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, ".sdo/notes.txt", "stray")
	called := false
	diagnostics, err := Check(context.Background(), Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, Layers: Compile}, func(context.Context, Request) ([]Diagnostic, error) { called = true; return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("runner executed on invalid static tree")
	}
	if got := strings.Join(ruleSet(diagnostics), ","); !strings.Contains(got, RuleUnknownPath) || !strings.Contains(got, RuleOwnershipDenied) {
		t.Fatalf("layers skipped: %s", got)
	}
}

func TestCheckReturnsCompileDiagnosticsAndInfrastructureErrors(t *testing.T) {
	root := goodTree(t)
	commit(t, root, "base")
	write(t, root, "app.txt", "repair\n")
	request := Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ProposalPhase}
	diagnostics, err := Check(context.Background(), request, func(context.Context, Request) ([]Diagnostic, error) {
		return []Diagnostic{{Rule: RuleDetectorTest, Message: "failed detector test"}}, nil
	})
	if err != nil || len(diagnostics) != 1 || diagnostics[0].Rule != RuleDetectorTest {
		t.Fatalf("compile result lost: %v %v", diagnostics, err)
	}
	_, err = Check(context.Background(), request, func(context.Context, Request) ([]Diagnostic, error) { return nil, context.DeadlineExceeded })
	if err != context.DeadlineExceeded {
		t.Fatalf("infrastructure failure lost: %v", err)
	}
}

func TestCompileCandidateRequiresToolchainSourcesAndConfiguredBaseline(t *testing.T) {
	operatorRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []Request{
		{Root: t.TempDir()},
		{Root: t.TempDir(), OperatorRoot: t.TempDir()},
		{Root: t.TempDir(), OperatorRoot: operatorRoot, HealthyBaseline: filepath.Join(t.TempDir(), "absent")},
	} {
		if diagnostics, err := CompileCandidate(context.Background(), request); err == nil || len(diagnostics) > 0 {
			t.Fatalf("missing input must be infrastructure failure: %v %v", diagnostics, err)
		}
	}
	t.Setenv("PATH", t.TempDir())
	if _, err := CompileCandidate(context.Background(), Request{Root: t.TempDir(), OperatorRoot: operatorRoot}); err == nil {
		t.Fatal("missing Go toolchain accepted")
	}
}
