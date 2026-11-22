package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
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

// maxAddProbes bounds the hosts a pin check asks at once.
const maxAddProbes = 4

// checkPins refuses a spec whose built-in fuzzops workload binary on any usable host does not report the pinned
// revision, before a lane is spent on it. A host that cannot be asked is skipped (said on log); a spec none of
// whose hosts can be asked is refused. cache remembers answers per host and pin within one session; the hosts are asked
// together (at most maxAddProbes at a time), each bounded by addPinTimeout.
func checkPins(ctx context.Context, inv Inventory, s Spec, log io.Writer, cache *memoMap[string]) error {
	run := addHostRunner(inv)
	hosts := inv.UsableHosts(s)
	whys := make([]string, len(hosts))
	sem := make(chan struct{}, maxAddProbes)
	var wg sync.WaitGroup
	for i, h := range hosts {
		fz := inv.FuzzopsPath(s.Fuzzops, h)
		key := h + "|" + fz + "|" + s.Fuzzops
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			whys[i], _ = cache.do(key, func() (string, error) {
				c, cancel := context.WithTimeout(ctx, addPinTimeout)
				out, err := run(c, h, IdentityCommand(s.Workload, fz, inv.Hosts[h].EnvFor(s.Workload)))
				cancel()
				var v Version
				switch {
				case err != nil:
					return "?" + err.Error(), nil
				case json.Unmarshal([]byte(lastNonEmpty([]byte(out))), &v) != nil:
					return "?unreadable version: " + clip(out, 100), nil
				}
				return pinMismatch(v, s.Fuzzops), nil
			})
		}()
	}
	wg.Wait()
	asked := 0
	for i, h := range hosts {
		why := whys[i]
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
