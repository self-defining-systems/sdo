package bench

// The subject's share of preflight (docs/contract.md, the subject verb `preflight`). A hooks checkout that
// implements the verb answers for itself; one that does not (the answer is "unsupported") gets the SDO operator's
// checks, which are the behavior preflight has always had. Nothing here runs for a spec with no hooks.

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
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

// sdoSubject is the default subject checks: the SDO operator's image set, credential call, sink, proto binding
// and baseline memory.
type sdoSubject struct{}

func (sdoSubject) Images(specs []specFile) []string {
	set := map[string]bool{}
	for _, s := range specs {
		if s.Operator.Hooks == "" {
			continue
		}
		tag := settingString(s.Operator.Settings, "image_tag", "fz-"+short8(s.Operator.Revision))
		for _, n := range []string{"controller", "responder", "script-tools"} {
			set["sdo-"+n+":"+tag] = true
		}
	}
	var out []string
	for i := range set {
		out = append(out, i)
	}
	return out
}

func (sdoSubject) Host(ctx context.Context, inv Inventory, h string, hs []specFile, pre string, run runFn, add addFn) {
	var out string
	var err error
	// credentials
	for _, c := range credentialChecks(hs) {
		t0 := time.Now()
		out, err := run(ctx, h, pre+c.script())
		ok, d := judgeCall(out, err)
		add(1, "credential["+c.label()+"]", h, ok, "%s %s (%s, %.0fs)", c.Provider, c.Model, d, time.Since(t0).Seconds())
	}

	// sink and proto: in each pinned hooks checkout on the host
	seen := map[string]bool{}
	for _, s := range hs {
		if s.Operator.Hooks == "" {
			continue
		}
		dir := inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, h)
		if dir == "" || seen[dir] {
			continue
		}
		seen[dir] = true
		out, err = run(ctx, h, pre+inDir(dir, sinkSnippet))
		ok, d := judgeSnippet(out, err)
		add(2, "sink-claude-transcripts", h, ok, "%s: %s", filepath.Base(dir), d)
		out, err = run(ctx, h, pre+inDir(dir, protoSnippet))
		ok, d = judgeSnippet(out, err)
		add(3, "proto-binding-source", h, ok, "%s: %s", filepath.Base(dir), d)
	}

}

func (sdoSubject) Local(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn, add addFn, wg *sync.WaitGroup) {
	// 6 memory, on the manager's own host (the baselines are there).
	if local := inv.LocalHost(); local != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			names := map[string][]string{}
			for _, s := range specs {
				if b := s.baselineDir(); b != "" && s.Operator.Hooks != "" {
					k := b + "|" + inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
					names[k] = append(names[k], s.Name)
				}
			}
			seen := map[string]bool{}
			for _, s := range specs {
				b := s.baselineDir()
				if b == "" || s.Operator.Hooks == "" {
					continue
				}
				hooks := inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
				key := b + "|" + hooks
				if seen[key] {
					continue
				}
				seen[key] = true
				label := "memory[" + strings.Join(names[key], ",") + "]"
				bl, err := readBaselineJSON(b)
				if err != nil {
					add(6, label, local, false, "%v", err)
					continue
				}
				if bl.Namespace != s.Namespace {
					add(6, label, local, false, "baseline.json namespace %q, spec namespace %q", bl.Namespace, s.Namespace)
					continue
				}
				if hooks == "" {
					add(6, label, local, false, "no hooks checkout on %s to build sdo-operator", local)
					continue
				}
				out, err := run(ctx, local, envPrefix(inv.Hosts[local])+memoryScript(b, bl, hooks))
				ok, d := judgeMemory(out, err, bl.Commit, opt.AllowMemoryDiag)
				add(6, label, local, ok, "baseline %s %s", short(bl.Commit), d)
			}
		}()
	} else if anyBaseline(specs) {
		add(6, "memory", "-", false, "needs a local host in hosts.yaml (the baselines are read there)")
	}
}

// hookSubject asks each pinned hooks checkout's `preflight` verb first and falls back to sdoSubject for the
// checkouts that do not implement it.
type hookSubject struct{}

func (hookSubject) Images(specs []specFile) []string { return sdoSubject{}.Images(specs) }

func (hookSubject) Host(ctx context.Context, inv Inventory, h string, hs []specFile, pre string, run runFn, add addFn) {
	var rest []specFile
	seen := map[string]bool{}
	for _, s := range hs {
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
		out, err := run(ctx, h, pre+hookPreflightScript(dir, cmd))
		checks, supported, perr := parseHookPreflight(out, err)
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
		sdoSubject{}.Host(ctx, inv, h, rest, pre, run, add)
	}
}

func (hookSubject) Local(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn, add addFn, wg *sync.WaitGroup) {
	sdoSubject{}.Local(ctx, inv, specs, opt, run, add, wg)
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
