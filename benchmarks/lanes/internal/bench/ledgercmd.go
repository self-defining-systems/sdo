package bench

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// LedgerCommand is `fuzzops-bench ledger [--state DIR] show | prune | adopt LANE CPUS MEMORY`, run on the host: what the
// lane ledger holds, dropping the lanes whose cluster is gone (the allocator does that itself when it is asked for
// cores; this is for looking and for forcing it), and registering a lane that exists without having been allocated
// (built before lane limits) so that new lanes do not take its cores.
func LedgerCommand(args []string, out, errw io.Writer) int {
	fs := flag.NewFlagSet("ledger", flag.ContinueOnError)
	fs.SetOutput(errw)
	state := fs.String("state", "", "the ledger directory (hosts.yaml lanes.state; default: the user's own, "+DefaultLedgerDir()+")")
	shared := fs.Bool("shared", false, "trust a directory others can write (hosts.yaml lanes.state_shared); needs --state")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	pos := fs.Args()
	if *shared && *state == "" {
		fmt.Fprintln(errw, "ledger: --shared needs --state (the per-user default is never shared)")
		return 2
	}
	a := CPUAllocator{Dir: *state, Shared: *shared, Log: func(f string, args ...any) { fmt.Fprintf(errw, "ledger: "+f+"\n", args...) }, Live: func() (map[string]bool, error) {
		ctx, cancel := context.WithTimeout(context.Background(), clusterCheckTimeout)
		defer cancel()
		b, err := exec.CommandContext(ctx, "kind", "get", "clusters").Output()
		if err != nil {
			return nil, err
		}
		live := map[string]bool{}
		for _, n := range strings.Fields(string(b)) {
			live[n] = true
		}
		return live, nil
	}}
	switch {
	case len(pos) == 1 && pos[0] == "show":
		es, err := a.Entries()
		if err != nil {
			fmt.Fprintln(errw, err)
			return 1
		}
		var lanes []string
		for l := range es {
			lanes = append(lanes, l)
		}
		sort.Strings(lanes)
		for _, l := range lanes {
			e := es[l]
			fmt.Fprintf(out, "%-14s cpus %-12s memory %5.1fGiB  since %s created=%v\n", l, FormatCPUList(e.CPUs), float64(e.Memory)/(1<<30), e.Since.Format(time.RFC3339), e.Created)
		}
		if len(lanes) == 0 {
			fmt.Fprintln(out, "no lane holds cores")
		}
	case len(pos) == 1 && pos[0] == "prune":
		dropped, err := a.Prune()
		if err != nil {
			fmt.Fprintln(errw, err)
			return 1
		}
		fmt.Fprintf(out, "dropped %d lane(s): %s\n", len(dropped), strings.Join(dropped, " "))
	case len(pos) == 4 && pos[0] == "adopt":
		cpus, err := ParseCPUList(pos[2])
		mem, err2 := ParseSize(pos[3])
		if err != nil || err2 != nil {
			fmt.Fprintln(errw, "adopt LANE CPUS MEMORY:", err, err2)
			return 2
		}
		if err := a.Adopt(pos[1], cpus, mem); err != nil {
			fmt.Fprintln(errw, err)
			return 1
		}
		fmt.Fprintf(out, "lane %s holds cpus %s, memory %s\n", pos[1], FormatCPUList(cpus), pos[3])
	default:
		fmt.Fprintln(errw, "usage: fuzzops-bench ledger [--state DIR] show | prune | adopt LANE CPUS MEMORY")
		return 2
	}
	return 0
}
