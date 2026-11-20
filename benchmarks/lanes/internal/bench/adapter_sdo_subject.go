package bench

// The built-in subject adapter for the SDO operator, for hooks checkouts that predate the contract verbs `preflight`
// (and `describe`, docs/contract.md): its preflight checks (images, responder credential call, evidence sink, proto
// binding, baseline memory). A pin that implements the verb answers for itself and none of this runs; it is deleted
// when every pin does.

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// legacySubject is the default subject checks: the SDO operator's image set, credential call, sink, proto binding
// and baseline memory.
type legacySubject struct{}

func (legacySubject) Images(specs []specFile) []string {
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

func (legacySubject) Host(ctx context.Context, inv Inventory, h string, hs []specFile, pre string, run runFn, add addFn) {
	var out string
	var err error
	// credentials
	for _, c := range credentialChecks(hs) {
		t0 := time.Now()
		out, err := run(ctx, h, pre+c.script())
		ok, d := judgeCall(out, err)
		add(1, "credential["+c.label()+"]", h, ok, "%s %s (%s, %.0fs)", c.Provider, c.Model, d, time.Since(t0).Seconds())
	}

	if stubs := stubAgentSpecs(hs); len(stubs) > 0 {
		add(1, "credential["+strings.Join(stubs, ",")+"]", h, true, "skipped: the agent is the no-model stub (operator model %q), no provider is called", stubModel)
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

func (legacySubject) Local(ctx context.Context, inv Inventory, specs []specFile, opt PreflightOptions, run runFn, add addFn, wg *sync.WaitGroup) {
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
					// The check builds sdo-operator from the pin's checkout; without one here it cannot run, which is
					// no verdict on the memory: a note, not a failure.
					add(6, label, local, true, "not checkable here: no hooks checkout of the pin on %s to build sdo-operator", local)
					continue
				}
				out, err := run(ctx, local, envPrefix(inv.Hosts[local])+memoryScript(b, bl, hooks))
				ok, d := judgeMemory(out, err, bl.Commit, opt.AllowMemoryDiag)
				add(6, label, local, ok, "baseline %s %s", short(bl.Commit), d)
			}
			// The seed memory is checked the same way, at the spec's pinned SDO revision: an invalid one would
			// crash-loop the controller on a built lane.
			seedSeen := map[string]bool{}
			for _, s := range specs {
				if s.Operator.Seed == "" || s.Operator.Hooks == "" {
					continue
				}
				seed := s.Operator.Seed
				if !filepath.IsAbs(seed) {
					seed = filepath.Join(s.dir, seed)
				}
				hooks := inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
				key := seed + "|" + hooks
				if seedSeen[key] {
					continue
				}
				seedSeen[key] = true
				label := "memory-seed[" + s.Name + "]"
				r := checkMemoryDir(ctx, run, local, envPrefix(inv.Hosts[local]), hooks, seed, opt.AllowMemoryDiag)
				switch {
				case r.Err != nil:
					add(6, label, local, false, "seed %s: %v", seed, r.Err)
				case r.Ran:
					add(6, label, local, true, "seed %s: %s", seed, r.Detail)
				default:
					add(6, label, local, true, "%s", r.Note)
				}
			}
		}()
	} else if anyBaseline(specs) {
		add(6, "memory", "-", false, "needs a local host in hosts.yaml (the baselines are read there)")
	}
}

// ---- 1 credentials --------------------------------------------------------------------------------------------

type credCheck struct {
	Provider, Model, Effort, AuthFile, Image string
	// Env is the agent's extra environment (operator settings config.agent.extra_env, "K=V"): a provider reached
	// through another backend (CLAUDE_CODE_USE_VERTEX=1 with GOOGLE_APPLICATION_CREDENTIALS) is only reached with it.
	Env   []string
	Specs []string
}

// agentCredentialsDir is where the agent pod mounts the credential file (.credentials.json for claude, auth.json for
// codex); an extra_env entry may name a path in it (GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json).
const agentCredentialsDir = "/sdo/credentials"

// agentExtraEnv is settings config.agent.extra_env: a list of "K=V" strings, or a map.
func agentExtraEnv(set map[string]any) []string {
	cfg, _ := set["config"].(map[string]any)
	agent, _ := cfg["agent"].(map[string]any)
	var out []string
	switch e := agent["extra_env"].(type) {
	case []any:
		for _, v := range e {
			if s, ok := v.(string); ok && strings.Contains(s, "=") {
				out = append(out, s)
			}
		}
	case map[string]any:
		for k, v := range e {
			out = append(out, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(out)
	}
	return out
}

func (c credCheck) label() string { return strings.Join(c.Specs, ",") }

// stubModel is the operator setting `model: stub` that declares a no-model agent (a scripted stand-in): it calls no
// provider, so preflight makes no credential call for it, whatever its provider setting says.
const stubModel = "stub"

// isStubAgent reports whether the operator settings declare the no-model agent.
func isStubAgent(set map[string]any) bool { return settingString(set, "model", "") == stubModel }

// stubAgentSpecs names the specs with a hooks operator whose agent is the no-model stand-in.
func stubAgentSpecs(specs []specFile) []string {
	var out []string
	for _, s := range specs {
		if s.Operator.Hooks != "" && !s.describedOrLegacy().NeedsCredentials {
			out = append(out, s.Name)
		}
	}
	return out
}

// credentialChecks lists the distinct (provider, model, effort, credential file, image) the specs' lanes will use.
// A spec with a stub agent has none: no model is called.
func credentialChecks(specs []specFile) []credCheck {
	idx := map[string]int{}
	var out []credCheck
	for _, s := range specs {
		if s.Operator.Hooks == "" || !s.describedOrLegacy().NeedsCredentials {
			continue
		}
		set := s.Operator.Settings
		provider := settingString(set, "provider", "codex")
		auth := "~/.codex/auth.json"
		if provider == "claude" {
			auth = "~/.claude/.credentials.json"
		}
		c := credCheck{Provider: provider, Model: settingString(set, "model", ""), Effort: settingString(set, "effort", ""),
			AuthFile: settingString(set, "auth_file", auth),
			Image:    "sdo-responder:" + settingString(set, "image_tag", "fz-"+short8(s.Operator.Revision)), Env: agentExtraEnv(set)}
		k := fmt.Sprint(c.Provider, c.Model, c.Effort, c.AuthFile, c.Image, c.Env)
		if i, ok := idx[k]; ok {
			out[i].Specs = append(out[i].Specs, s.Name)
			continue
		}
		idx[k] = len(out)
		c.Specs = []string{s.Name}
		out = append(out, c)
	}
	return out
}

// homePath makes a "~/x" path usable inside a double-quoted shell word.
func homePath(p string) string {
	if strings.HasPrefix(p, "~/") {
		return `"$HOME/` + strings.ReplaceAll(p[2:], `"`, `\"`) + `"`
	}
	return shellQuote(p)
}

// script runs one tiny call in the responder image as the agent pod makes it: the lane's credential file mounted
// read-only where the pod mounts it (agentCredentialsDir) and copied into the CLI's home as the agent does, with the
// agent's extra environment. The file is never printed.
func (c credCheck) script() string {
	var inner, file string
	if c.Provider == "claude" {
		file = ".credentials.json"
		inner = `mkdir -p $HOME/.claude && cp ` + agentCredentialsDir + `/` + file + ` $HOME/.claude/.credentials.json && claude -p`
		if c.Model != "" {
			inner += " --model " + shellQuote(c.Model)
		}
		if c.Effort != "" {
			inner += " --effort " + shellQuote(c.Effort)
		}
		inner += ` "Reply with the single word ok"`
	} else {
		file = "auth.json"
		inner = `mkdir -p $HOME/.codex && cp ` + agentCredentialsDir + `/` + file + ` $HOME/.codex/auth.json && CODEX_HOME=$HOME/.codex codex exec --skip-git-repo-check`
		if c.Model != "" {
			inner += " -m " + shellQuote(c.Model)
		}
		if c.Effort != "" {
			inner += " -c " + shellQuote("model_reasoning_effort="+c.Effort)
		}
		inner += ` "Reply with the single word ok"`
	}
	var env strings.Builder
	for _, e := range c.Env {
		env.WriteString("-e " + shellQuote(e) + " ")
	}
	return fmt.Sprintf(`timeout 100 docker run --rm -v %s:%s/%s:ro -e HOME=/tmp/h %s--entrypoint sh %s -c %s 2>&1`,
		homePath(c.AuthFile), agentCredentialsDir, file, env.String(), shellQuote(c.Image), shellQuote(inner))
}

var okWord = regexp.MustCompile(`(?i)(^|\W)ok(\W|$)`)

// judgeCall: the call must exit 0 and its last line must contain "ok"; otherwise its last line is the reason
// (for example "You've hit your weekly limit").
func judgeCall(out string, err error) (bool, string) {
	last := lastLine(out)
	if err != nil {
		if last == "" {
			last = err.Error()
		}
		return false, "call failed: " + tail(last, 160)
	}
	if !okWord.MatchString(last) {
		return false, "unexpected answer: " + tail(last, 160)
	}
	return true, "answered ok"
}

func lastLine(s string) string {
	last := ""
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			last = l
		}
	}
	return last
}

// ---- 2 sink, 3 proto --------------------------------------------------------------------------------------------

// sinkSnippet runs the pinned sink's transcript counter over a synthetic Claude session file.
const sinkSnippet = `import pathlib, sys, tempfile
from benchmarks.fuzzops import sink
with tempfile.TemporaryDirectory() as t:
    d = pathlib.Path(t)
    f = d / "claude" / "projects" / "-synthetic" / "session.jsonl"
    f.parent.mkdir(parents=True)
    f.write_text('{"type": "user"}\n')
    n = sink.count_rollouts(d)
if n != 1:
    print("PREFLIGHT counted %d transcripts in a synthetic Claude session, want 1" % n)
    sys.exit(1)
print("PREFLIGHT ok: counts a Claude session file")`

// protoSnippet validates a ProgramRun for every binding source the pinned code can emit (the constants of its
// candidate producer, plus the known set) with SDO's own protovalidate rules, as an incident request carries them.
const protoSnippet = `import pathlib, re, sys
from sdo.contracts import proto
from sdo.contracts._gen.sdodev.contracts.v1alpha1 import program_pb2 as p
src = pathlib.Path("sdo/operator/runtime/program_candidates.go")
text = src.read_text() if src.is_file() else ""
sources = sorted(set(re.findall(r'BindingSource\w*\s*=\s*"(\w+)"', text)) | {"finding", "hint", "sweep", "suspect", "credited"})
bad = []
for s in sources:
    run = p.ProgramRun(program_id="synthetic", binding_source=s)
    run.verdict.result = 1
    run.duration.FromMilliseconds(5)
    try:
        proto.validate(run)
    except ValueError as e:
        bad.append("%s (%s)" % (s, str(e)[-60:]))
if bad:
    print("PREFLIGHT rejected binding sources: " + "; ".join(bad))
    sys.exit(1)
print("PREFLIGHT ok: %d binding sources valid" % len(sources))`

// judgeSnippet: a snippet exits 0 and prints a "PREFLIGHT" line.
func judgeSnippet(out string, err error) (bool, string) {
	line := ""
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "PREFLIGHT ") {
			line = strings.TrimPrefix(l, "PREFLIGHT ")
		}
	}
	if err != nil {
		if line == "" {
			line = lastLine(out)
		}
		if line == "" {
			line = err.Error()
		}
		return false, tail(line, 220)
	}
	if line == "" {
		return false, "no result line: " + tail(out, 120)
	}
	return true, line
}

// ---- describe, for a pin that predates the verb --------------------------------------------------------------------

// DefaultStartupBound is SDO's own response.startup_bound default: how long a new install may report busy
// ("startup baseline pending") before its start-up period ends.
const DefaultStartupBound = 30 * time.Minute

var preContractNoted sync.Map

// notePreContract says once per hooks checkout and verb that the pin predates the verb, so the built-in answer can be
// deleted when no pin needs it.
func notePreContract(log io.Writer, s Spec, verb string) {
	if log == nil {
		return
	}
	if _, seen := preContractNoted.LoadOrStore(s.Operator.Hooks+"@"+s.Operator.Revision+" "+verb, true); !seen {
		fmt.Fprintf(log, "note: pre-contract pin: hooks %s %s answers no %s verb; using the built-in SDO answer\n", s.Operator.Hooks, s.Operator.Revision, verb)
	}
}

// legacyDescribe is what `describe` answers for the SDO operator: the start-up bound in its settings (default
// 30m), a model unless its agent is the no-model stub, and a seed that needs a baseline.
func legacyDescribe(s Spec) Described {
	bound := DefaultStartupBound
	if v, ok := s.Operator.Settings["startup_bound"].(string); ok {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			bound = d
		}
	}
	return Described{MaxStartup: bound.String(), NeedsCredentials: !isStubAgent(s.Operator.Settings),
		Inputs: map[string]InputRule{"seed": {Requires: []string{"baseline"}}}}
}
