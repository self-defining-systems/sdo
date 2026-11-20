package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// The subject's `check-input` verb (docs/contract.md): lanes passes the kind and the path of an input of the spec and
// the subject says whether it can use it. Lanes knows nothing about what the input is.

// inputResult is the outcome of checking one input.
type inputResult struct {
	Ran    bool   // the check ran (Err says whether it passed)
	Err    error  // the check ran and failed
	Note   string // why it did not run; nothing is wrong with the input
	Detail string // what a passing check says
}

// subjectInputs are the spec's inputs the subject checks, by kind.
func subjectInputs(s Spec) []struct{ Kind, Path string } {
	var out []struct{ Kind, Path string }
	for _, kind := range []string{"baseline", "seed"} {
		if p := s.operatorInput(kind); p != "" && !isPlaceholder(p) {
			out = append(out, struct{ Kind, Path string }{kind, p})
		}
	}
	return out
}

// hookCheckInput asks the spec's pinned hooks checkout on the manager's host to check one input. supported is false
// when there is no such checkout here or the pin predates the verb.
func hookCheckInput(ctx context.Context, inv Inventory, s Spec, kind, path, phase string, allow []string, log io.Writer) (res inputResult, supported bool, err error) {
	local := inv.LocalHost()
	dir, cmd := "", inv.HookCommand[s.Operator.Hooks]
	if local != "" {
		dir = inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
	}
	if dir == "" || cmd == "" {
		return inputResult{}, false, nil
	}
	tmp, err := os.MkdirTemp("", "bench-input-")
	if err != nil {
		return inputResult{}, true, err
	}
	defer os.RemoveAll(tmp)
	opPath := filepath.Join(tmp, "operator.json")
	op := HookOperator{Hooks: s.Operator.Hooks, Revision: s.Operator.Revision, Settings: s.Operator.Settings, Baseline: s.Operator.Baseline, Seed: s.Operator.Seed}
	if err := writeJSON(opPath, op); err != nil {
		return inputResult{}, true, err
	}
	h := Hook{Dir: dir, Command: cmd, Env: []string{"BENCH_EXPERIMENT=" + s.Name, "FUZZOPS_NAMESPACE=" + s.Namespace, "BENCH_OPERATOR=" + opPath,
		"BENCH_OUT=" + tmp, "BENCH_INPUT_KIND=" + kind, "BENCH_INPUT_PATH=" + path, "BENCH_INPUT_PHASE=" + phase, "BENCH_INPUT_ALLOW=" + strings.Join(allow, ",")}}
	r := h.Run(ctx, "check-input", nil, nil)
	if r.Unsupported() || (r.OK() && r.Last == "") {
		notePreContract(log, s, "check-input")
		return inputResult{}, false, nil
	}
	var ans struct {
		OK     bool   `json:"ok"`
		Detail string `json:"detail"`
		Note   string `json:"note"`
	}
	if jerr := json.Unmarshal([]byte(r.Last), &ans); jerr != nil || (!r.OK() && ans.Detail == "" && ans.Note == "") {
		return inputResult{}, true, fmt.Errorf("hooks %s: %s", s.Operator.Hooks, r.Why("check-input"))
	}
	switch {
	case !ans.OK:
		return inputResult{Ran: true, Err: errors.New(strings.TrimSpace(ans.Detail))}, true, nil
	case ans.Note != "":
		return inputResult{Note: ans.Note}, true, nil
	}
	return inputResult{Ran: true, Detail: ans.Detail}, true, nil
}

// checkSpecSubjectInputs has the subject check the spec's baseline and seed (at add time: no cluster is known), and
// refuses the spec if it cannot use one. A pin that predates the verb gets the built-in check, noted once.
func checkSpecSubjectInputs(ctx context.Context, inv Inventory, s Spec, c *RenderedChecks) error {
	if s.Operator.Hooks == "" {
		return nil
	}
	for _, in := range subjectInputs(s) {
		key := s.Operator.Hooks + "@" + s.Operator.Revision + "|" + in.Kind + "|" + in.Path
		r, ok := c.mem[key]
		if !ok {
			var supported bool
			var err error
			r, supported, err = hookCheckInput(ctx, inv, s, in.Kind, in.Path, "add", nil, c.Log)
			if err != nil {
				return err
			}
			if !supported {
				r = legacyCheckSpecInput(ctx, inv, s, "operator."+in.Kind, in.Path)
			}
			c.mem[key] = r
			if r.Note != "" && c.Log != nil {
				fmt.Fprintf(c.Log, "note: %s operator.%s: %s\n", s.Name, in.Kind, r.Note)
			}
		}
		if r.Err != nil {
			return fmt.Errorf("operator.%s %s: input check at %s failed: %w", in.Kind, in.Path, short(s.Operator.Revision), r.Err)
		}
	}
	return nil
}
