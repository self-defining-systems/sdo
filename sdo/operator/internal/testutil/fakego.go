package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil/pathio"
)

// GoRule makes a matching Go toolchain stage fail. A rule matches when the
// stage's verb (first non-flag argument: mod, build, vet, test) equals Verb,
// and every non-empty condition holds. An unmatched stage succeeds.
type GoRule struct {
	Verb string
	// Covers is a workspace-relative directory. The rule only matches a stage
	// whose package patterns select it (or a directory below it): ./..., ./dir/... or ./dir.
	Covers string
	// Marker is a workspace-relative file that must exist when the stage runs.
	Marker string
	// ArgContains is a substring the stage's full argument string must carry.
	ArgContains string
	// ArgAbsent is a substring the stage's full argument string must not carry.
	ArgAbsent string
	Output    string
}

// FakeGo puts a synthetic `go` first on PATH and returns the file where it
// appends one `<workdir>\t<args>` line per stage. It stands in for the compiler
// edge in contracts about stage plans, stage classification and diagnostics:
// stages pass unless a rule fails them, and `build -o X` leaves an empty
// executable at X. Real compiler conformance stays with the integration tests.
// The stand-in is a POSIX shell script, so it also works through child processes
// such as the operator CLI binary.
func FakeGo(t testing.TB, rules ...GoRule) (stageLog string) {
	t.Helper()
	dir := t.TempDir()
	stageLog = filepath.Join(dir, "stages.log")
	if err := os.WriteFile(stageLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	var script strings.Builder
	fmt.Fprintf(&script, "#!/bin/sh\nprintf '%%s\\t%%s\\n' \"$PWD\" \"$*\" >> %s\n", shellQuote(stageLog))
	script.WriteString(`verb=
for a in "$@"; do case "$a" in -*) ;; *) verb=$a; break;; esac; done
prev=
for a in "$@"; do
  if [ "$prev" = "-o" ]; then : > "$a"; chmod +x "$a"; fi
  prev=$a
done
covers() {
  [ -z "$1" ] && return 0
  for p in $ARGS_PATTERNS; do
    case "$p" in
      ./...) return 0 ;;
      ./*/...) base=${p#./}; base=${base%/...}
        case "$1" in "$base"|"$base"/*) return 0 ;; esac ;;
      ./*) [ "$1" = "${p#./}" ] && return 0 ;;
    esac
  done
  return 1
}
ARGS_PATTERNS=
for a in "$@"; do case "$a" in ./*) ARGS_PATTERNS="$ARGS_PATTERNS $a";; esac; done
args="$*"
norun=
prev=
for a in "$@"; do
  case "$prev|$a" in "-run|^$") norun=1;; esac
  case "$a" in "-run=^$") norun=1;; esac
  prev=$a
done
`)
	for _, rule := range rules {
		fmt.Fprintf(&script, "if [ %s = \"$verb\" ]", shellQuote(rule.Verb))
		if rule.Covers != "" {
			fmt.Fprintf(&script, " && covers %s", shellQuote(rule.Covers))
		}
		if rule.Verb == "test" {
			// A test stage that selects no test (-run '^$') cannot fail on one.
			script.WriteString(` && [ -z "$norun" ]`)
		}
		if rule.Marker != "" {
			fmt.Fprintf(&script, " && [ -e %s ]", shellQuote(rule.Marker))
		}
		if rule.ArgContains != "" {
			fmt.Fprintf(&script, " && case \"$args\" in *%s*) true;; *) false;; esac", shellQuote(rule.ArgContains))
		}
		if rule.ArgAbsent != "" {
			fmt.Fprintf(&script, " && case \"$args\" in *%s*) false;; *) true;; esac", shellQuote(rule.ArgAbsent))
		}
		fmt.Fprintf(&script, "; then printf '%%s\\n' %s; exit 1; fi\n", shellQuote(rule.Output))
	}
	script.WriteString("exit 0\n")
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(script.String()), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", pathio.Prepend(dir))
	return stageLog
}

// GoStages returns the argument lines of the stages a FakeGo log recorded.
func GoStages(t testing.TB, stageLog string) []string {
	t.Helper()
	data, err := os.ReadFile(stageLog)
	if err != nil {
		t.Fatal(err)
	}
	var stages []string
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		_, args, _ := strings.Cut(line, "\t")
		stages = append(stages, args)
	}
	return stages
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
