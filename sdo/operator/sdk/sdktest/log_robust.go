package sdktest

import (
	"context"
	"fmt"
	"strings"

	"sdo.dev/operator/sdk/program"
)

// BusyLogTail is how many newest log lines a Diagnose is still expected to decide on: a read returns only the newest
// 64 KiB, which on a busy pod can be a few dozen lines.
const BusyLogTail = 20

func newestLines(text string, n int) string {
	if n <= 0 {
		return text
	}
	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	if len(lines) <= n {
		return text
	}
	return strings.Join(lines[len(lines)-n:], "\n") + "\n"
}

// CheckDiagnoseLogRobust runs Diagnose on the case's opening state twice, once on the recorded logs and once with every
// Logs result cut to its newest BusyLogTail lines. A verdict that flips from Match means the program gates on a log
// line that a busy pod's bounded read drops, so it would miss the fault on a repeat.
func CheckDiagnoseLogRobust(d program.Diagnoser, c CorpusCase) error {
	run := func(tail int) (program.Verdict, error) {
		return d.Diagnose(context.Background(), &ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open"), LogTail: tail}, c.Bindings)
	}
	full, err := run(0)
	if err != nil || full.Result != program.Match {
		return nil
	}
	cut, err := run(BusyLogTail)
	if err == nil && cut.Result == program.Match {
		return nil
	}
	return fmt.Errorf("program.log-gated-diagnose: Diagnose matches on the recorded logs but not on their newest %d lines, so a busy pod's bounded log read would hide the fault on a repeat. Match on the state the fix changed (an object, a catalog row) and use logs only as evidence; return Unknown, not NoMatch, when only logs could confirm", BusyLogTail)
}
