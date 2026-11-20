package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// verbHelp is what `lanes VERB --help` prints. It needs no root and no hosts.yaml: help is answered before either is read.
type verbHelp struct {
	desc  string // one line
	usage string // the synopsis after "lanes "
	flags string // the flags the verb reads, one per line, already indented; "" when it has none of its own
}

const rootFlag = "  --root R   the results root (default $FUZZOPS_BENCH_ROOT)\n"

// helpTable has one entry per verb run dispatches; TestEveryVerbPrintsItsOwnHelp runs each with --help.
var helpTable = map[string]verbHelp{
	"serve": {
		desc:  "run the manager in the foreground: it reconciles every 10 s; steps run on without it",
		usage: "serve --root R",
		flags: rootFlag,
	},
	"manager": {
		desc:  "run the manager as a systemd --user unit named from the root (restarts on failure); stopping it leaves steps running",
		usage: "manager start|stop|restart|status|logs --root R [--no-follow] [-n N]",
		flags: rootFlag +
			"  --no-follow   logs: print the last lines and exit (default: keep following bench.log)\n" +
			"  -n N          logs: how many lines to start with (default 50)\n",
	},
	"version": {
		desc:  "print the revision this binary was built from",
		usage: "version",
	},
	"add": {
		desc:  "validate a spec file and add it as an experiment",
		usage: "add FILE --root R",
		flags: rootFlag,
	},
	"remove": {
		desc:  "drop an experiment's queued runs; runs in flight finish and results stay",
		usage: "remove NAME [--teardown] --root R",
		flags: rootFlag + "  --teardown   delete the lanes' clusters even when the spec keeps its lanes\n",
	},
	"pause": {
		desc:  "stop new runs of an experiment",
		usage: "pause NAME --root R",
		flags: rootFlag,
	},
	"resume": {
		desc:  "restart new runs of a paused experiment",
		usage: "resume NAME --root R",
		flags: rootFlag,
	},
	"status": {
		desc:  "show experiments, lanes and hosts",
		usage: "status [NAME] --root R",
		flags: rootFlag,
	},
	"results": {
		desc:  "grade the done runs of an experiment into <root>/NAME/results",
		usage: "results NAME [--with BASELINE-EXP ...] --root R",
		flags: rootFlag + "  --with EXP   a baseline experiment whose noop runs grade this one (repeatable)\n",
	},
	"lane": {
		desc:  "lane maintenance; `lane reset LANE` returns a down lane to free after a human looked",
		usage: "lane reset LANE --root R",
		flags: rootFlag,
	},
	"gc": {
		desc:  "on the manager's next pass, delete lane clusters and host staging nothing needs (results stay)",
		usage: "gc --root R",
		flags: rootFlag,
	},
	"preflight": {
		desc:  "check credentials, sink, images, memory, capacity and pack before any lane is built",
		usage: "preflight [--spec FILE|DIR ...] [--max-load N] [--allow-memory-diag S ...] --root R",
		flags: rootFlag +
			"  --spec P                a spec file or a directory of specs (repeatable; default <root>/specs)\n" +
			"  --max-load N            highest accepted load5 per core (default 1.0)\n" +
			"  --allow-memory-diag S   a memory-check diagnostic substring that does not fail the check (repeatable)\n",
	},
	"step": {
		desc:  "run one step to its end (the manager starts it detached on the lane's host)",
		usage: "step STEP.json",
	},
	"ledger": {
		desc:  "the host's lane ledger (cores and memory each lane holds); run on the host",
		usage: "ledger [--state DIR] show | prune | adopt LANE CPUS MEMORY",
		flags: "  --state DIR   the ledger's state directory\n",
	},
	"host": {
		desc:  "the hosts.yaml reserve: block: apply it through the root helper, undo it, show it, record an existing one",
		usage: "host reserve HOST|all | unreserve HOST [--drain] | status [HOST] | adopt HOST [--cpus LIST] [--prev SLICE=LIST]... | install-helper HOST [--sudo-password-file PATH] --root R",
		flags: rootFlag +
			"  --drain                    unreserve: proceed although lanes are running\n" +
			"  --cpus LIST                adopt: the block that exists\n" +
			"  --prev SLICE=LIST          adopt: what that slice's AllowedCPUs was before (repeatable)\n" +
			"  --sudo-password-file PATH  install-helper: feed sudo -S from this file\n",
	},
	"plan": {
		desc:  "the plan engine: arms x items x reps with verified-artifact edges (docs/plan.md)",
		usage: "plan add FILE [--dry-run [--json]] | pause|resume|remove [--keep] NAME | skip [--cancel] NAME ITEM | unskip NAME ITEM | status [NAME] [--json] | results NAME [--tsv] [--final] [--done] [--grade-fields a,b] --root R",
		flags: rootFlag +
			"  --dry-run        add: validate fully and print the expanded plan; store nothing\n" +
			"  --json           status, add --dry-run: print JSON\n" +
			"  --keep           remove: only retire the plan; its experiments and name stay\n" +
			"  --cancel         remove, skip: also cancel the runs in flight\n" +
			"  --tsv            results: tab-separated rows with a header (default JSON lines)\n" +
			"  --final          results: only the last attempt of each node\n" +
			"  --done           results: only done rows\n" +
			"  --grade-fields   results --tsv: top-level grade fields to add as columns (comma-separated)\n",
	},
	"probe": {
		desc:  "how a host's steps stand, as JSON (the manager runs it over ssh)",
		usage: "probe --staging S [--fuzzops F,..] LANE=STEP ...",
		flags: "  --staging S   the host's staging directory\n  --fuzzops F   fuzzops checkouts whose in-flight markers to list (comma-separated)\n",
	},
	"gc-staging": {
		desc:  "delete staging directories nothing uses, as JSON (the manager runs it over ssh)",
		usage: "gc-staging --staging S [KEEP ...]",
		flags: "  --staging S   the host's staging directory\n",
	},
}

// wantsHelp reports whether args ask for help.
func wantsHelp(args []string) bool {
	for _, a := range args {
		switch a {
		case "-h", "--help", "-help":
			return true
		}
	}
	return false
}

// printHelp writes verb's help; false when verb has none.
func printHelp(verb string, w io.Writer) bool {
	h, ok := helpTable[verb]
	if !ok {
		return false
	}
	fmt.Fprintf(w, "lanes %s: %s\n\nusage: lanes %s\n", verb, h.desc, h.usage)
	if h.flags != "" {
		fmt.Fprintf(w, "\nflags:\n%s", h.flags)
	}
	return true
}

// printOverview lists every verb with its one-line description.
func printOverview(w io.Writer) {
	names := make([]string, 0, len(helpTable))
	for n := range helpTable {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintln(w, "usage: lanes VERB [--root R] ...   (lanes VERB --help for a verb's flags)")
	for _, n := range names {
		fmt.Fprintf(w, "  %-11s %s\n", n, strings.TrimSpace(helpTable[n].desc))
	}
}
