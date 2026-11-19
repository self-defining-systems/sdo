package program_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var classStore = program.DatabaseTarget{
	Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-a-0"}, Container: "store", Database: "app",
}

func withUser(user string) program.DatabaseTarget {
	t := classStore
	t.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: user + "_USER"}, Password: program.CredentialRef{Env: user + "_PASS"}}
	return t
}

// catalogView refuses (or fails) the admin account and answers the root account.
type catalogView struct {
	program.ReadView
	adminErr error
	users    []program.DatabaseUser
}

func (v catalogView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if t.Auth != nil && t.Auth.Username.Env == "ADMIN_USER" {
		return nil, v.adminErr
	}
	return v.users, nil
}

// fallbackDiagnoser tries the admin account, falls back to root and decides
// from what it saw: Match when the application account is missing.
type fallbackDiagnoser struct{ match bool }

func (fallbackDiagnoser) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "fallback"} }

func (d fallbackDiagnoser) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	users, err := v.DatabaseUsers(withUser("ADMIN"))
	if err != nil {
		users, err = v.DatabaseUsers(withUser("ROOT"))
		if err != nil {
			return program.Verdict{}, err
		}
	}
	for _, u := range users {
		if u.Name == "app" {
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	if d.match {
		return program.Verdict{Result: program.Match}, nil
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func diagnoseWith(t *testing.T, adminErr error, d program.Diagnoser) program.Verdict {
	t.Helper()
	view := catalogView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView(), adminErr: adminErr}
	return program.RunDiagnose(context.Background(), d, view, program.Bindings{}, time.Second)
}

func hasReadFailureEvidence(v program.Verdict, method string) bool {
	for _, e := range v.Evidence {
		if e.Fact == "read unavailable" && strings.Contains(e.Observed, method) {
			return true
		}
	}
	return false
}

func TestRefusedReadHandledByProgramIsNotUnknown(t *testing.T) {
	got := diagnoseWith(t, fmt.Errorf("login: %w", sdk.ErrRefused), fallbackDiagnoser{match: true})
	if got.Result != program.Match {
		t.Fatalf("a refused login the program fell back from must not make Unknown: %s %+v", got.Result, got.Evidence)
	}
	if hasReadFailureEvidence(got, "DatabaseUsers") {
		t.Fatalf("a refusal is an observation, not a failed read: %+v", got.Evidence)
	}
}

func TestRefusedReadThenNoMatchStaysNoMatch(t *testing.T) {
	got := diagnoseWith(t, fmt.Errorf("login: %w", sdk.ErrRefused), fallbackDiagnoser{match: false})
	if got.Result != program.NoMatch {
		t.Fatalf("got %s", got.Result)
	}
}

func TestUnavailableReadThenNoMatchIsUnknown(t *testing.T) {
	got := diagnoseWith(t, fmt.Errorf("timeout: %w", sdk.ErrUnavailable), fallbackDiagnoser{match: false})
	if got.Result != program.Unknown {
		t.Fatalf("clearing on missing evidence must be Unknown, got %s", got.Result)
	}
	if !strings.Contains(got.Evidence[0].Observed, "DatabaseUsers") {
		t.Fatalf("Unknown must name the failed read: %+v", got.Evidence)
	}
}

func TestUnclassifiedReadThenNoMatchIsUnknown(t *testing.T) {
	if got := diagnoseWith(t, fmt.Errorf("boom"), fallbackDiagnoser{match: false}); got.Result != program.Unknown {
		t.Fatalf("an unclassified failure is unavailable, got %s", got.Result)
	}
}

func TestUnavailableReadThenMatchStandsWithEvidence(t *testing.T) {
	got := diagnoseWith(t, fmt.Errorf("timeout: %w", sdk.ErrUnavailable), fallbackDiagnoser{match: true})
	if got.Result != program.Match {
		t.Fatalf("a positive finding stands over a failed read: %s %+v", got.Result, got.Evidence)
	}
	if !hasReadFailureEvidence(got, "DatabaseUsers") {
		t.Fatalf("the failed read must ride with the Match: %+v", got.Evidence)
	}
}

func TestInvalidReadIsUnknownEvenWithMatch(t *testing.T) {
	got := diagnoseWith(t, fmt.Errorf("secret: %w", sdk.ErrInvalidRead), fallbackDiagnoser{match: true})
	if got.Result != program.Unknown {
		t.Fatalf("an invalid read is a program bug, got %s", got.Result)
	}
}

func TestEveryFailedReadIsKept(t *testing.T) {
	d := twoFailures{}
	got := program.RunDiagnose(context.Background(), d, failingDiffs{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView()}, program.Bindings{}, time.Second)
	if got.Result != program.Match {
		t.Fatalf("got %s", got.Result)
	}
	if !hasReadFailureEvidence(got, "DiffAgainstSource") || !hasReadFailureEvidence(got, "DiffAgainstBaseline") {
		t.Fatalf("both failed reads must be listed, not only the first: %+v", got.Evidence)
	}
}

type failingDiffs struct{ program.ReadView }

func (failingDiffs) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, fmt.Errorf("no source: %w", sdk.ErrUnavailable)
}
func (failingDiffs) DiffAgainstBaseline(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, fmt.Errorf("no baseline: %w", sdk.ErrUnavailable)
}

type twoFailures struct{}

func (twoFailures) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "two"} }
func (twoFailures) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "cm-a"}
	_, _ = v.DiffAgainstSource(ref)
	_, _ = v.DiffAgainstBaseline(ref)
	return program.Verdict{Result: program.Match}, nil
}
