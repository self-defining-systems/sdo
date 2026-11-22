//go:build integration

package bench

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The settle and the teardown's idle wait ask `idle` twice in a row, a poll apart. A subject whose idle answer cannot
// change in that gap (`describe`: idle_confirm "0s") is not asked after a wait: on SREGym lanes the gap was a fixed
// 10 s in every row's settle and in every release.

func idleHook(t *testing.T, body string) *Prepared {
	t.Helper()
	dir := t.TempDir()
	hook := filepath.Join(dir, "hook.sh")
	must(t, os.WriteFile(hook, []byte("#!/bin/sh\n[ \"$1\" = idle ] || exit 0\n"+body), 0o755))
	return &Prepared{Hooks: "t", HooksPath: dir, Command: hook, Ready: true}
}

func TestSettleAsksIdleAgainAtOnceWhenTheSubjectSaysItsAnswerIsFinal(t *testing.T) {
	p := idleHook(t, "exit 0\n")
	none := Seconds(0)
	r := &stepRunner{dir: t.TempDir(), poll: 5 * time.Second, hookLog: io.Discard}
	r.s.Cap, r.s.IdleConfirm = Seconds(time.Hour.Seconds()), &none
	start := time.Now()
	r.settle(context.Background(), p, time.Now(), time.Time{})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("settle took %s although the subject's idle answer is final", took)
	}
	if r.res.Capped || r.res.SettleEnd == nil {
		t.Fatalf("settle did not finish: capped=%v end=%v", r.res.Capped, r.res.SettleEnd)
	}
}

func TestSettleKeepsItsPollForASubjectThatDoesNotSayItsAnswerIsFinal(t *testing.T) {
	p := idleHook(t, "exit 0\n")
	r := &stepRunner{dir: t.TempDir(), poll: 700 * time.Millisecond, hookLog: io.Discard}
	r.s.Cap = Seconds(time.Hour.Seconds())
	start := time.Now()
	r.settle(context.Background(), p, time.Now(), time.Time{})
	if took := time.Since(start); took < 700*time.Millisecond {
		t.Fatalf("settle confirmed idle after %s, less than the poll", took)
	}
}

func TestABusyAnswerIsStillPolledAtThePollWhenTheSubjectSaysItsIdleIsFinal(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "busy-once")
	p := idleHook(t, "if [ ! -e "+flag+" ]; then touch "+flag+"; echo 'alarm: busy'; exit 1; fi\nexit 0\n")
	none := Seconds(0)
	r := &stepRunner{dir: t.TempDir(), poll: 600 * time.Millisecond, hookLog: io.Discard}
	r.s.Cap, r.s.IdleConfirm = Seconds(time.Hour.Seconds()), &none
	start := time.Now()
	r.settle(context.Background(), p, time.Now(), time.Time{})
	if took := time.Since(start); took < 600*time.Millisecond {
		t.Fatalf("settle retried a busy answer after %s, less than the poll", took)
	}
}

func TestTeardownIdleAsksAgainAtOnceWhenTheSubjectSaysItsAnswerIsFinal(t *testing.T) {
	p := idleHook(t, "exit 0\n")
	none := Seconds(0)
	r := &stepRunner{dir: t.TempDir(), poll: 5 * time.Second, hookLog: io.Discard}
	r.s.Cap, r.s.IdleConfirm = Seconds(time.Minute.Seconds()), &none
	start := time.Now()
	if !r.waitIdle(context.Background(), p) {
		t.Fatal("waitIdle failed")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("waitIdle took %s although the subject's idle answer is final", took)
	}
}

func TestDescribeCarriesTheIdleConfirmGap(t *testing.T) {
	if d := (Described{IdleConfirm: "0s"}); true {
		gap, ok := d.idleConfirm()
		if !ok || gap != 0 {
			t.Errorf("idle_confirm 0s = (%s, %v), want (0, true)", gap, ok)
		}
	}
	if _, ok := (Described{}).idleConfirm(); ok {
		t.Error("a subject that says nothing keeps the poll")
	}
	if _, ok := (Described{IdleConfirm: "soon"}).idleConfirm(); ok {
		t.Error("an unreadable gap must keep the poll")
	}
}

func TestAStepCarriesTheIdleConfirmOfItsExperimentsSubject(t *testing.T) {
	m := &Manager{exps: map[string]*Experiment{
		"final": {Name: "final", Spec: &Spec{Operator: Operator{Hooks: "h"}, Described: &Described{IdleConfirm: "0s"}}},
		"plain": {Name: "plain", Spec: &Spec{Operator: Operator{Hooks: "h"}, Described: &Described{}}},
	}}
	if got := m.idleConfirmFor("final"); got == nil || *got != 0 {
		t.Errorf("final: %v, want a gap of 0", got)
	}
	if got := m.idleConfirmFor("plain"); got != nil {
		t.Errorf("plain: %v, want none", *got)
	}
	if got := m.idleConfirmFor("gone"); got != nil {
		t.Errorf("an experiment that is gone has no gap, got %v", *got)
	}
}

func TestDescribeAsksTheSubjectForItsIdleConfirmGapAndRefusesAnUnreadableOne(t *testing.T) {
	f := newFixture(t)
	inv, err := LoadInventory(string(f.root))
	must(t, err)
	st := t.TempDir()
	t.Setenv("HOOK_STATE", st)
	s := Spec{Name: "e", Namespace: "app1", Operator: Operator{Hooks: "test", Revision: "4d2915b3f"}}

	must(t, os.WriteFile(filepath.Join(st, "describe"), []byte(`{"max_startup":"0s","idle_confirm":"0s"}`), 0o644))
	d, err := NewRenderedChecks(io.Discard).describe(context.Background(), inv, s)
	must(t, err)
	if gap, ok := d.idleConfirm(); d == nil || !ok || gap != 0 {
		t.Fatalf("description = %+v, want an idle_confirm of 0s", d)
	}

	must(t, os.WriteFile(filepath.Join(st, "describe"), []byte(`{"idle_confirm":"soon"}`), 0o644))
	if _, err := NewRenderedChecks(io.Discard).describe(context.Background(), inv, s); err == nil || !strings.Contains(err.Error(), "idle_confirm") {
		t.Fatalf("an unreadable idle_confirm must be refused naming the field, got %v", err)
	}
}
