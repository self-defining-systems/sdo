//go:build integration

package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type recorded struct {
	check, host string
	ok          bool
	detail      string
}

func recorder(out *[]recorded) addFn {
	var mu sync.Mutex
	return func(_ int, check, host string, ok bool, f string, a ...any) {
		mu.Lock()
		defer mu.Unlock()
		*out = append(*out, recorded{check, host, ok, fmt.Sprintf(f, a...)})
	}
}

// A spec whose operator declares the no-model agent (model: stub) makes no credential call, whatever its provider
// says: the check passes with a note that says why it was skipped. A real model is still checked.
func TestStubAgentSkipsTheCredentialCall(t *testing.T) {
	mk := func(name, model string) specFile {
		return specFile{Spec: Spec{Name: name, Operator: Operator{Hooks: "sdo", Revision: "1da0aac6", Settings: map[string]any{
			"provider": "codex", "model": model}}}}
	}
	specs := []specFile{mk("stubbed", "stub"), mk("real", "gpt-x")}
	cs := credentialChecks(specs)
	if len(cs) != 1 || cs[0].label() != "real" {
		t.Fatalf("credential checks %+v, want only the real model's", cs)
	}
	var got []recorded
	runs := 0
	run := func(context.Context, string, string) (string, error) { runs++; return "", nil }
	inv := Inventory{Hosts: map[string]Host{"h1": {}}}
	legacySubject{}.Host(context.Background(), inv, "h1", specs[:1], "", run, recorder(&got))
	if runs != 0 {
		t.Fatalf("the stub spec ran %d host commands (a provider call)", runs)
	}
	if len(got) != 1 || !got[0].ok || !strings.Contains(got[0].detail, "skipped") || !strings.Contains(got[0].check, "stubbed") {
		t.Fatalf("results %+v, want one passing credential note", got)
	}
}

// The memory check needs the pin's hooks checkout on the manager's host. Where there is none it is "not checkable
// here", a note that passes, not a failure.
func TestMemoryCheckWithoutAHooksCheckoutHereIsANote(t *testing.T) {
	l := newFlab(t, "h1=c1")
	inv, err := LoadInventory(string(l.root))
	must(t, err)
	base := t.TempDir()
	must(t, os.WriteFile(filepath.Join(base, "baseline.json"), []byte(`{"namespace":"app1","bundle":"b.bundle","branch":"main","commit":"abc"}`), 0o644))
	s := specFile{Spec: Spec{Name: "e1", Namespace: "app1", Operator: Operator{Hooks: "test", Revision: "ffffff0", Baseline: base}}}
	var got []recorded
	var wg sync.WaitGroup
	run := func(context.Context, string, string) (string, error) {
		t.Fatal("nothing runs without a checkout")
		return "", nil
	}
	legacySubject{}.Local(context.Background(), inv, []specFile{s}, PreflightOptions{}, run, recorder(&got), &wg)
	wg.Wait()
	if len(got) != 1 || !got[0].ok || !strings.Contains(got[0].detail, "not checkable here") {
		t.Fatalf("results %+v, want one passing note", got)
	}
}
