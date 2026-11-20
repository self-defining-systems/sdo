package bench

// The built-in memory check of the SDO operator, for hooks checkouts that predate the contract verb `check-input`: it
// builds sdo-operator from the pin and runs `memory migrate` over a baseline or seed directory. Deleted when every pin
// answers `check-input`.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// memoryRunner runs the memory check script on a host (a variable so tests need no sdo-operator build).
var memoryRunner = HostRunner

// checkMemoryDir runs SDO's memory check (`sdo-operator memory migrate`: the startup upkeep and the accepted-memory
// audit the controller runs on its first read) over the memory in dir, with the sdo-operator built from hooksDir, the
// pinned SDO checkout. A directory that is not there or has no baseline.json is not a memory bundle yet (a seed an
// earlier arm will produce): a note. Without a checkout of the pin on the host the check cannot run: a note, never a
// failure.
func checkMemoryDir(ctx context.Context, run runFn, host, envp, hooksDir, dir string, allow []string) inputResult {
	if _, err := os.Stat(filepath.Join(dir, "baseline.json")); err != nil {
		return inputResult{Note: fmt.Sprintf("%s is not a memory bundle yet (no baseline.json): not checked", dir)}
	}
	bl, err := readBaselineJSON(dir)
	if err != nil {
		return inputResult{Note: fmt.Sprintf("%v: not checked", err)}
	}
	if hooksDir == "" {
		return inputResult{Note: fmt.Sprintf("not checkable here: no hooks checkout of the pin on %s to build sdo-operator", host)}
	}
	out, err := run(ctx, host, envp+memoryScript(dir, bl, hooksDir))
	ok, d := judgeMemory(out, err, bl.Commit, allow)
	if !ok {
		return inputResult{Ran: true, Err: fmt.Errorf("%s", strings.TrimSpace(d))}
	}
	return inputResult{Ran: true, Detail: d}
}

// ---- 6 memory -----------------------------------------------------------------------------------------------------

type baselineJSON struct {
	Namespace string `json:"namespace"`
	Bundle    string `json:"bundle"`
	Branch    string `json:"branch"`
	Commit    string `json:"commit"`
}

func readBaselineJSON(dir string) (baselineJSON, error) {
	var b baselineJSON
	raw, err := os.ReadFile(filepath.Join(dir, "baseline.json"))
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, fmt.Errorf("%s/baseline.json: %v", dir, err)
	}
	if b.Bundle == "" || b.Branch == "" || b.Commit == "" {
		return b, fmt.Errorf("%s/baseline.json: bundle, branch and commit are required", dir)
	}
	return b, nil
}

// memoryScript clones the baseline's bundle, prints its head, builds the pinned sdo-operator and runs
// `sdo-operator memory migrate` on the clone: what the controller does on its first memory read (upkeep to the
// current schema), then the same accepted-memory audit as `memory check --accepted`, without replay.
// It runs under the controller image's Go limits (GOMAXPROCS and -p of Dockerfile.runtime): validation measures the
// child's whole process group against a 3 GiB limit, and an unlimited build on a many-core host exceeds it.
func memoryScript(baselineDir string, b baselineJSON, hooksDir string) string {
	opRoot := shellQuote(filepath.Join(hooksDir, "sdo", "operator"))
	return fmt.Sprintf(`export GOMAXPROCS=2 GOFLAGS="-p=2 -trimpath"; T=$(mktemp -d) || exit 2; trap 'rm -rf "$T"' EXIT; `+
		`git clone -q -b %[1]s %[2]s "$T/m" || { echo "PREFLIGHT clone failed"; exit 2; }; echo "HEAD $(git -C "$T/m" rev-parse HEAD)"; `+
		`(cd %[3]s && go build -o "$T/sdo-operator" ./cmd/sdo-operator) || { echo "PREFLIGHT go build sdo-operator failed"; exit 2; }; `+
		`mkdir "$T/w"; SDO_OPERATOR_ROOT=%[3]s "$T/sdo-operator" memory migrate "$T/m" --worktree-root "$T/w" --replay off >"$T/out" 2>&1; rc=$?; sed 's/^/CHECK /' "$T/out"; echo "EXIT $rc"; true`,
		shellQuote(b.Branch), shellQuote(filepath.Join(baselineDir, b.Bundle)), opRoot)
}

// judgeMemory: the bundle's head is the commit baseline.json names and the memory check printed no diagnostics.
func judgeMemory(out string, err error, commit string, allow []string) (bool, string) {
	if err != nil {
		return false, "probe failed: " + tail(err.Error()+" "+out, 200)
	}
	head, exit := "", "?"
	var diag []string
	for _, l := range strings.Split(out, "\n") {
		switch {
		case strings.HasPrefix(l, "EXIT "):
			exit = strings.TrimSpace(strings.TrimPrefix(l, "EXIT "))
		case strings.HasPrefix(l, "HEAD "):
			head = strings.TrimSpace(strings.TrimPrefix(l, "HEAD "))
		case strings.HasPrefix(l, "CHECK "):
			diag = append(diag, strings.TrimPrefix(l, "CHECK "))
		case strings.HasPrefix(l, "PREFLIGHT "):
			return false, strings.TrimPrefix(l, "PREFLIGHT ")
		}
	}
	if head != commit {
		return false, fmt.Sprintf("bundle head %s, baseline.json says %s", short(head), short(commit))
	}
	var left []string
	for _, d := range diag {
		allowed := false
		for _, a := range allow {
			allowed = allowed || strings.Contains(d, a)
		}
		if !allowed {
			left = append(left, d)
		}
	}
	if exit != "0" && (len(left) > 0 || len(diag) == 0) {
		return false, "memory migrate+check exit " + exit + ": " + clip(strings.Join(left, " | "), 200)
	}
	if len(diag) > 0 {
		return true, fmt.Sprintf("bundle head matches baseline.json; memory check clean (%d allowed diagnostics)", len(diag))
	}
	return true, "bundle head matches baseline.json; memory check clean"
}

// specMemoryAllow: the add-time check has no cluster, so it cannot know which telemetry sources the install will bind;
// the controller binds them at install. An unbound source is therefore no failure here (preflight takes the same
// substring with --allow-memory-diag).
var specMemoryAllow = []string{"telemetry.unbound"}

// legacyCheckSpecInput runs SDO's memory check over one input of the spec (baseline or seed) at its pinned SDO
// revision, on the manager's host when it has that hooks checkout, for a pin without `check-input`. An invalid one is
// refused naming the memory and the failed check, instead of crash-looping the controller on a built lane.
func legacyCheckSpecInput(ctx context.Context, inv Inventory, s Spec, what, dir string) inputResult {
	local := inv.LocalHost()
	hooks := ""
	if local != "" {
		hooks = inv.HooksPath(s.Operator.Hooks, s.Operator.Revision, local)
	}
	return checkMemoryDir(ctx, memoryRunner(inv), local, envPrefix(inv.Hosts[local]), hooks, dir, specMemoryAllow)
}
