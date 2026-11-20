package bench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// memoryRunner runs the memory check script on a host (a variable so tests need no sdo-operator build).
var memoryRunner = HostRunner

// memResult is the outcome of the memory check of one directory in the baseline's format.
type memResult struct {
	Ran    bool   // the check ran (Err says whether it passed)
	Err    error  // the check ran and failed
	Note   string // why it did not run; nothing is wrong with the memory
	Detail string // what a passing check says
}

// checkMemoryDir runs SDO's memory check (`sdo-operator memory migrate`: the startup upkeep and the accepted-memory
// audit the controller runs on its first read) over the memory in dir, with the sdo-operator built from hooksDir, the
// pinned SDO checkout. A directory that is not there or has no baseline.json is not a memory bundle yet (a seed an
// earlier arm will produce): a note. Without a checkout of the pin on the host the check cannot run: a note, never a
// failure.
func checkMemoryDir(ctx context.Context, run runFn, host, envp, hooksDir, dir string, allow []string) memResult {
	if _, err := os.Stat(filepath.Join(dir, "baseline.json")); err != nil {
		return memResult{Note: fmt.Sprintf("%s is not a memory bundle yet (no baseline.json): not checked", dir)}
	}
	bl, err := readBaselineJSON(dir)
	if err != nil {
		return memResult{Note: fmt.Sprintf("%v: not checked", err)}
	}
	if hooksDir == "" {
		return memResult{Note: fmt.Sprintf("not checkable here: no hooks checkout of the pin on %s to build sdo-operator", host)}
	}
	out, err := run(ctx, host, envp+memoryScript(dir, bl, hooksDir))
	ok, d := judgeMemory(out, err, bl.Commit, allow)
	if !ok {
		return memResult{Ran: true, Err: fmt.Errorf("%s", strings.TrimSpace(d))}
	}
	return memResult{Ran: true, Detail: d}
}
