package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// pinMismatch says why a workload binary reporting v cannot run work pinned to rev ("" when it can): runs must be
// measured by the pinned revision, from an unmodified checkout. A build step applies the same rule, so a spec that
// would fail every build is refused at add instead.
func pinMismatch(v Version, rev string) string {
	switch {
	case rev == "":
		return ""
	case v.Modified:
		return fmt.Sprintf("workload was built from a modified checkout (%s): runs must be measured by the pinned revision", v.Revision)
	case !strings.HasPrefix(v.Revision, rev):
		return fmt.Sprintf("workload is at %s, not the pinned %s (rebuild or update its checkout)", v.Revision, rev)
	}
	return ""
}

// addHostRunner runs a script on a host for the add-time pin check (a var so tests do not dial hosts).
var addHostRunner = HostRunner

// addPinTimeout bounds the pin check on one host.
const addPinTimeout = 30 * time.Second

// checkPins refuses a spec whose built-in fuzzops workload binary on any usable host does not report the pinned
// revision, before a lane is spent on it. A host that cannot be asked is skipped (said on log); a spec none of
// whose hosts can be asked is refused. cache remembers answers per host and pin within one add.
func checkPins(ctx context.Context, inv Inventory, s Spec, log io.Writer, cache map[string]string) error {
	run := addHostRunner(inv)
	asked := 0
	hosts := inv.UsableHosts(s)
	for _, h := range hosts {
		fz := inv.FuzzopsPath(s.Fuzzops, h)
		key := h + "|" + fz + "|" + s.Fuzzops
		why, seen := cache[key]
		if !seen {
			c, cancel := context.WithTimeout(ctx, addPinTimeout)
			out, err := run(c, h, IdentityCommand(s.Workload, fz))
			cancel()
			var v Version
			switch {
			case err != nil:
				why = "?" + err.Error()
			case json.Unmarshal([]byte(lastNonEmpty([]byte(out))), &v) != nil:
				why = "?unreadable version: " + clip(out, 100)
			default:
				why = pinMismatch(v, s.Fuzzops)
			}
			cache[key] = why
		}
		switch {
		case strings.HasPrefix(why, "?"):
			if log != nil {
				fmt.Fprintf(log, "pin check: host %s not asked: %s\n", h, clip(why[1:], 160))
			}
		case why != "":
			return fmt.Errorf("host %s: %s", h, why)
		default:
			asked++
		}
	}
	_, builtin := builtinWorkloads[s.Workload] // an adapter executable that cannot be asked here is checked by its build step
	if len(hosts) > 0 && asked == 0 && builtin {
		return fmt.Errorf("no host of %v could be asked which workload revision its checkout is", hosts)
	}
	return nil
}
