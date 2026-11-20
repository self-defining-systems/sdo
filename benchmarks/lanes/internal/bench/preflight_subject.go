package bench

// The subject's share of preflight (docs/contract.md, the subject verb `preflight`). A hooks checkout that
// implements the verb answers for itself; one that does not (the answer is "unsupported") gets the SDO operator's
// checks, which are the behavior preflight has always had. Nothing here runs for a spec with no hooks.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// addFn records one preflight check result.
type addFn func(order int, check, host string, ok bool, f string, a ...any)

// subjectChecks are the checks the subject adapter owns.
type subjectChecks interface {
	// Images are the images the subject needs on every host that runs the specs.
	Images(specs []specFile) []string
	// Host runs the subject's checks on one host for the specs placed there.
	Host(ctx context.Context, inv Inventory, h string, hs []specFile, pre string, run runFn, add addFn)
	// Local runs the checks that read the manager's own host (the baselines); it starts its work on wg.
	Local(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn, add addFn, wg *sync.WaitGroup)
}

// hookSubject asks each pinned hooks checkout's `preflight` verb first and falls back to legacySubject for the
// checkouts that do not implement it. A subject that answers `preflight` owns every check of its specs; the platform
// runs none for them. For the others the subject's `check-input` checks the baseline and seed, and only a pin without
// that verb gets the built-in memory check.
type hookSubject struct{ probes *sync.Map }

func newHookSubject() hookSubject { return hookSubject{probes: &sync.Map{}} }

type probeEntry struct {
	once      sync.Once
	checks    []subjectCheck
	supported bool
	err       error
}

// probe asks one checkout's `preflight` verb on host, once per host and checkout.
func (hs hookSubject) probe(ctx context.Context, run runFn, host, dir, cmd, pre string) ([]subjectCheck, bool, error) {
	ask := func(e *probeEntry) {
		out, err := run(ctx, host, pre+hookPreflightScript(dir, cmd))
		e.checks, e.supported, e.err = parseHookPreflight(out, err)
	}
	if hs.probes == nil {
		var e probeEntry
		ask(&e)
		return e.checks, e.supported, e.err
	}
	v, _ := hs.probes.LoadOrStore(host+"|"+dir, &probeEntry{})
	e := v.(*probeEntry)
	e.once.Do(func() { ask(e) })
	return e.checks, e.supported, e.err
}

func (hookSubject) Images(specs []specFile) []string { return legacySubject{}.Images(specs) }

func (hs hookSubject) Host(ctx context.Context, inv Inventory, h string, onHost []specFile, pre string, run runFn, add addFn) {
	var rest []specFile
	seen := map[string]bool{}
	for _, s := range onHost {
		dir := inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, h)
		cmd := inv.HookCommand[s.Operator.Hooks]
		if s.Operator.Hooks == "" || dir == "" || cmd == "" {
			rest = append(rest, s)
			continue
		}
		if seen[dir] {
			continue
		}
		seen[dir] = true
		checks, supported, perr := hs.probe(ctx, run, h, dir, cmd, pre)
		if !supported {
			rest = append(rest, s)
			continue
		}
		if perr != nil {
			add(2, "subject-preflight", h, false, "%s: %v", filepath.Base(dir), perr)
			continue
		}
		for _, c := range checks {
			add(2, c.Check, h, c.OK, "%s: %s", filepath.Base(dir), c.Detail)
		}
	}
	if len(rest) > 0 {
		legacySubject{}.Host(ctx, inv, h, rest, pre, run, add)
	}
}

// Local checks the spec inputs the manager's own host holds (the baselines and seeds).
func (hs hookSubject) Local(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn, add addFn, wg *sync.WaitGroup) {
	local := inv.LocalHost()
	var rest []specFile
	seen := map[string]bool{}
	for _, s := range specs {
		dir, cmd := "", inv.HookCommand[s.Operator.Hooks]
		if local != "" {
			dir = inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
		}
		if s.Operator.Hooks == "" || dir == "" || cmd == "" {
			rest = append(rest, s)
			continue
		}
		if _, supported, _ := hs.probe(ctx, run, local, dir, cmd, envPrefix(inv.Hosts[local])); supported {
			continue // the subject answered preflight: its checks are its own
		}
		for _, in := range s.inputPaths() {
			key := dir + "|" + in.Kind + "|" + in.Path
			if seen[key] {
				continue
			}
			seen[key] = true
			wg.Add(1)
			go func(s specFile, kind, path string) {
				defer wg.Done()
				res, supported, err := hookCheckInput(ctx, inv, s.Spec, kind, path, "preflight", opt.AllowMemoryDiag, os.Stderr)
				label := "input[" + kind + ":" + s.Name + "]"
				switch {
				case err != nil:
					add(6, label, local, false, "%s %s: %v", kind, path, err)
				case !supported:
					legacySubject{}.Local(ctx, inv, []specFile{s}, opt, run, add, wg)
				case res.Err != nil:
					add(6, label, local, false, "%s %s: %v", kind, path, res.Err)
				case res.Ran:
					add(6, label, local, true, "%s %s: %s", kind, path, res.Detail)
				default:
					add(6, label, local, true, "%s", res.Note)
				}
			}(s, in.Kind, in.Path)
		}
	}
	if len(rest) > 0 {
		legacySubject{}.Local(ctx, inv, rest, opt, run, add, wg)
	}
}

// inputPaths are the spec's baseline and seed, relative paths resolved against the spec file.
func (s specFile) inputPaths() []struct{ Kind, Path string } {
	in := subjectInputs(s.Spec)
	for i := range in {
		if !filepath.IsAbs(in[i].Path) {
			in[i].Path = filepath.Join(s.dir, in[i].Path)
		}
	}
	return in
}

// subjectCheck is one entry of a subject preflight answer: {"checks":[{"check":"...","ok":true,"detail":"..."}]}.
type subjectCheck struct {
	Check  string `json:"check"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// hookPreflightScript runs `<command> preflight` in the hooks checkout and reports its exit status and the first
// and last line of its stdout.
func hookPreflightScript(dir, command string) string {
	return fmt.Sprintf(`cd %s && o=$(%s preflight 2>/dev/null); rc=$?; echo "RC $rc"; echo "FIRST $(printf '%%s\n' "$o" | sed -n 1p)"; echo "LAST $(printf '%%s\n' "$o" | tail -n 1)"`,
		shellQuote(dir), command)
}

// parseHookPreflight reads hookPreflightScript's output. supported is false when the hook has no preflight verb.
func parseHookPreflight(out string, err error) (checks []subjectCheck, supported bool, perr error) {
	if err != nil {
		return nil, true, fmt.Errorf("probe failed: %v", err)
	}
	r := HookResult{Exit: -1}
	for _, l := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(l, "RC "):
			fmt.Sscanf(strings.TrimPrefix(l, "RC "), "%d", &r.Exit)
		case strings.HasPrefix(l, "FIRST "):
			r.First = strings.TrimSpace(strings.TrimPrefix(l, "FIRST "))
		case strings.HasPrefix(l, "LAST "):
			r.Last = strings.TrimSpace(strings.TrimPrefix(l, "LAST "))
		}
	}
	if r.Unsupported() {
		return nil, false, nil
	}
	if r.Exit != 0 && r.Last == "" {
		return nil, true, fmt.Errorf("preflight exit %d", r.Exit)
	}
	var v struct {
		Checks []subjectCheck `json:"checks"`
	}
	if jerr := json.Unmarshal([]byte(r.Last), &v); jerr != nil {
		return nil, true, fmt.Errorf("preflight: last line is not JSON: %q", clip(r.Last, 120))
	}
	if r.Exit != 0 {
		v.Checks = append(v.Checks, subjectCheck{Check: "subject-preflight", Detail: fmt.Sprintf("exit %d", r.Exit)})
	}
	return v.Checks, true, nil
}
