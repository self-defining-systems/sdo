package bench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/deploy"
)

// The tools check (kubectl, docker, ip, and kind on a kind host) must run in exactly the environment a step runs in, or
// it passes for a host whose steps then fail with exit 127. A step's environment on host h for a spec of workload w is:
//
//   - the PATH and variables the ssh session gives (the check runs through the same ssh);
//   - h.Env with w's workload_env on top (Host.EnvFor), applied to every command the step runs;
//   - on a reserved host, the step process starts under the helper's lane-exec, which sets the PATH itself (so a helper
//     installed before it kept /usr/local/bin loses kubectl and kind for every step, though a plain ssh command finds them).
//
// toolsProbe builds that, and nothing more: no PATH directory is added.

// toolsProbe is the script that prints "missing TOOL" for each platform tool a step of workload on host h cannot find.
func toolsProbe(h Host, workload string) string {
	var b strings.Builder
	for _, kv := range h.EnvFor(workload) {
		k, v, _ := strings.Cut(kv, "=")
		fmt.Fprintf(&b, "export %s=%s; ", k, shellQuote(v))
	}
	b.WriteString(toolsScript(h))
	inner := b.String()
	if h.Reserve != nil && len(h.Clusters) > 0 {
		return "sudo -n --preserve-env " + HelperPath + " lane-exec " + shellQuote(h.Clusters[0]) + " -- sh -c " + shellQuote(inner)
	}
	return inner
}

// missingTools are the tools a probe's answer names.
func missingTools(out string) []string {
	var missing []string
	for _, l := range strings.Split(out, "\n") {
		if t, ok := strings.CutPrefix(strings.TrimSpace(l), "missing "); ok {
			missing = append(missing, t)
		}
	}
	return missing
}

// hostToolProblem asks host about the tools of its steps for workload: "" when all resolve or the host cannot be asked
// (reachability has its own alarm).
func (inv Inventory) hostToolProblem(ctx context.Context, host, workload string, run runFn) string {
	c, cancel := context.WithTimeout(ctx, clusterSourceTimeout)
	defer cancel()
	out, err := run(c, host, toolsProbe(inv.Hosts[host], workload))
	if err != nil {
		return ""
	}
	if m := missingTools(out); len(m) > 0 {
		return fmt.Sprintf("host %s: %s not found on the step PATH", host, strings.Join(m, ", "))
	}
	return ""
}

// ToolProblems are the usable hosts of the spec whose steps would not find a platform tool.
func (inv Inventory) ToolProblems(ctx context.Context, s Spec, run runFn) []string {
	var out []string
	for _, host := range inv.UsableHosts(s) {
		if p := inv.hostToolProblem(ctx, host, s.Workload, run); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// HostToolProblems are the same for host status, for each workload in workloads ("" for the default).
func (inv Inventory) HostToolProblems(ctx context.Context, host string, workloads []string, run runFn) []string {
	seen := map[string]bool{}
	var out []string
	if len(workloads) == 0 {
		workloads = []string{""}
	}
	sort.Strings(workloads)
	for _, w := range workloads {
		if seen[w] {
			continue
		}
		seen[w] = true
		if p := inv.hostToolProblem(ctx, host, w, run); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// helperOutOfDate is what host status says of a reserved host whose installed helper is not the one in this binary.
const helperOutOfDate = "helper out of date: reinstall with lanes host install-helper"

// HelperProblem is helperOutOfDate when host (a reserved host) has a helper installed that differs from the one embedded in
// this binary, or none; "" when it is current, the host has no reserve, or it cannot be asked. A stale helper sets the
// PATH of every step of the host and hid itself until a build failed (exit 127).
func (inv Inventory) HelperProblem(ctx context.Context, host string, run runFn) string {
	if inv.Hosts[host].Reserve == nil {
		return ""
	}
	want, err := deploy.Files.ReadFile("host/lane-reserve")
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(want)
	c, cancel := context.WithTimeout(ctx, clusterSourceTimeout)
	defer cancel()
	out, err := run(c, host, "if test -r "+shellQuote(HelperPath)+"; then sha256sum "+shellQuote(HelperPath)+" | cut -d' ' -f1; else echo missing; fi")
	if err != nil {
		return ""
	}
	if strings.TrimSpace(out) != hex.EncodeToString(sum[:]) {
		return helperOutOfDate
	}
	return ""
}

// toolWarnTTL is how long a host's tool findings stand before status asks again.
const toolWarnTTL = 5 * time.Minute
