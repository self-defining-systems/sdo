package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// The subject's `describe` verb (docs/contract.md): what the platform needs to know about the subject under test,
// answered by the subject so that lanes never reads the subject's own settings.

// InputRule is what the subject requires of one of its inputs.
type InputRule struct {
	// Requires names inputs that must be set whenever this one is.
	Requires []string `json:"requires,omitempty"`
}

// Described is the answer of `describe`.
type Described struct {
	// MaxStartup is how long the subject may report busy after a fresh install (a duration; "" or "0s": no start-up
	// period). A lane is never failed for a wait shorter than it.
	MaxStartup string `json:"max_startup,omitempty"`
	// NeedsCredentials says whether the subject calls a model provider, so preflight should check credentials.
	NeedsCredentials bool `json:"needs_credentials"`
	// VerifyEvidence declares the verb verify-evidence: a lane is freed only once it answered "verified"
	// (evidencehold.go). Absent: the verb is never called and holds nothing.
	VerifyEvidence bool `json:"verify_evidence,omitempty"`
	// Inputs are the rules on the spec's named inputs (operator.baseline, operator.seed), by input name.
	Inputs map[string]InputRule `json:"inputs,omitempty"`
}

func (d Described) startup() time.Duration {
	v, err := time.ParseDuration(d.MaxStartup)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// operatorInput is the path the spec sets for the named input ("" when unset).
func (s Spec) operatorInput(name string) string {
	switch name {
	case "baseline":
		return s.Operator.Baseline
	case "seed":
		return s.Operator.Seed
	}
	return ""
}

// describedOrLegacy is the subject's description: the one stored when the spec was added, else the built-in answer
// for a pin that predates the verb.
func (s Spec) describedOrLegacy() Described {
	if s.Described != nil {
		return *s.Described
	}
	return legacyDescribe(s)
}

// checkInputRules applies the subject's rules on its inputs.
func checkInputRules(s Spec) error {
	d := s.describedOrLegacy()
	for name, rule := range d.Inputs {
		if s.operatorInput(name) == "" {
			continue
		}
		for _, req := range rule.Requires {
			if s.operatorInput(req) == "" {
				return fmt.Errorf("operator.%s needs operator.%s", name, req)
			}
		}
	}
	return nil
}

// describe asks the spec's pinned hooks checkout on the manager's host for its description. nil, nil: the spec has no
// hooks, the host has no checkout of the pin to ask, or the pin predates the verb (said once on the log, so the
// built-in answer can be deleted later). One answer is kept per checkout and settings.
func (c *RenderedChecks) describe(ctx context.Context, inv Inventory, s Spec) (*Described, error) {
	if s.Operator.Hooks == "" {
		return nil, nil
	}
	local := inv.LocalHost()
	dir := ""
	if local != "" {
		dir = inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
	}
	cmd := inv.HookCommand[s.Operator.Hooks]
	if dir == "" || cmd == "" {
		return nil, nil
	}
	set, _ := json.Marshal(s.Operator.Settings)
	key := dir + "|" + string(set)
	return c.desc.do(key, func() (*Described, error) { return describeHook(ctx, Hook{Dir: dir, Command: cmd}, s, c.Log) })
}

func describeHook(ctx context.Context, h Hook, s Spec, log io.Writer) (*Described, error) {
	tmp, err := os.MkdirTemp("", "bench-describe-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	opPath := filepath.Join(tmp, "operator.json")
	if err := writeJSON(opPath, HookOperator{Hooks: s.Operator.Hooks, Revision: s.Operator.Revision, Settings: s.Operator.Settings}); err != nil {
		return nil, err
	}
	h.Env = append(h.Env, "BENCH_EXPERIMENT="+s.Name, "FUZZOPS_NAMESPACE="+s.Namespace, "BENCH_OPERATOR="+opPath, "BENCH_OUT="+tmp)
	r := h.Run(ctx, "describe", nil, nil)
	if r.Unsupported() || (r.OK() && r.Last == "") {
		notePreContract(log, s, "describe")
		return nil, nil
	}
	if !r.OK() {
		return nil, fmt.Errorf("hooks %s: %s", s.Operator.Hooks, r.Why("describe"))
	}
	var d Described
	if err := json.Unmarshal([]byte(r.Last), &d); err != nil {
		return nil, fmt.Errorf("hooks %s: describe: last line is not JSON: %q", s.Operator.Hooks, clip(r.Last, 120))
	}
	if d.MaxStartup != "" {
		if _, err := time.ParseDuration(d.MaxStartup); err != nil {
			return nil, fmt.Errorf("hooks %s: describe: max_startup %q: %v", s.Operator.Hooks, d.MaxStartup, err)
		}
	}
	return &d, nil
}
