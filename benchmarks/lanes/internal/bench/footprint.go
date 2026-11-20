package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
)

// The workload's `footprint` verb (docs/contract.md): the lane size it wants for its app, answered as
// {"cpus": 8, "memory": "30Gi"}. Lanes asks at add, on the manager's host, for a spec that sets no `lane`, and
// records the answer in the spec of the lock, so every later size decision reads that.

// footprint asks the spec's workload for its lane size. nil, nil: the spec sets its own, the built-in adapter carries its
// own sizes, the host has no checkout of the pin to ask, or the workload does not implement the verb.
func (c *RenderedChecks) footprint(ctx context.Context, inv Inventory, s Spec) (*LaneLimit, error) {
	if _, builtin := builtinWorkloads[s.Workload]; builtin || s.Lane != nil {
		return nil, nil
	}
	local := inv.LocalHost()
	dir := ""
	if local != "" {
		dir = inv.FuzzopsPath(s.Fuzzops, local)
	}
	if dir == "" {
		return nil, nil
	}
	key := s.Workload + "|" + dir + "|" + s.App
	if l, ok := c.foot[key]; ok {
		return l, nil
	}
	var out bytes.Buffer
	env := []string{"BENCH_PACK=" + filepath.Join(dir, s.App), "BENCH_EXPERIMENT=" + s.Name}
	code, _, err := NewWorkload(s.Workload, dir).Call(ctx, WorkloadCall{Verb: VerbFootprint, Env: env, Stdout: &out, Log: &bytes.Buffer{}})
	var l *LaneLimit
	if err == nil && code == 0 {
		var got LaneLimit
		if jerr := json.Unmarshal([]byte(lastNonEmpty(out.Bytes())), &got); jerr != nil {
			return nil, fmt.Errorf("workload %s: footprint: last line is not {cpus, memory}: %q", s.Workload, clip(lastNonEmpty(out.Bytes()), 120))
		}
		if bad := got.validate(); len(bad) > 0 {
			return nil, fmt.Errorf("workload %s: footprint: %v", s.Workload, bad)
		}
		l = &got
	}
	c.foot[key] = l
	return l, nil
}
