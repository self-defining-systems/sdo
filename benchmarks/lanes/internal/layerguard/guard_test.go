//go:build integration

package layerguard

// The lanes layer owns resources (hosts, reservations, ledger, lanes, pools, isolation) and dispatch (plans, steps,
// placement, retries, edges, alarms, rows). It must not know what a run does or how it is graded (the workload
// adapter) or what the subject is (the subject adapter). This test counts the lines of the lanes Go packages that
// name SDO, an app or an SDO configuration field, and compares them with allowlist.json, a ratchet that only shrinks:
// a count above the allowlist fails (a new leak), and a count below it fails too (lower the allowlist, so the
// leak cannot come back). Adapter registrations (files named adapter_*.go) and testdata are outside the count; the
// registrations are listed in the allowlist and the list only shrinks. Run with -update-layer to lower the counts.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var update = flag.Bool("update-layer", false, "lower the allowlist counts to what the code has now (never raises one)")
var dump = flag.Bool("dump-layer", false, "print the counts the code has now, as allowlist files (to seed an entry)")

// terms are the names the layer must not know. Each is matched case-insensitively per line.
var terms = []struct{ name, re string }{
	{"sdo", `sdo`},
	{"fuzzops", `fuzzops`},
	{"app-names", `gitlab|hotel|astronomy|sregym|social-?network`},
	{"playbook", `playbook`},
	{"incident", `incident`},
	{"operator", `operator`},
	{"sdo-config", `startup_bound|image_tag|extra_env|auth_file|baseline\.json|\.Baseline|\.Seed\b|"stub"|memory migrate`},
}

type fileEntry struct {
	Reason string         `json:"reason"`
	Terms  map[string]int `json:"terms"`
}

type allowlist struct {
	Files    map[string]fileEntry `json:"files"`
	Adapters map[string]string    `json:"adapter_files"` // adapter_*.go: file -> what it adapts and when it goes
}

const root = "../.." // benchmarks/lanes

var importPath = regexp.MustCompile(`self-defining-systems/sdo`)

func count(t *testing.T) (map[string]map[string]int, []string) {
	t.Helper()
	got := map[string]map[string]int{}
	var adapters []string
	res := make([]*regexp.Regexp, len(terms))
	for i, tm := range terms {
		res[i] = regexp.MustCompile(`(?i)` + tm.re)
	}
	for _, dir := range []string{"cmd", "internal", "deploy"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, p)
			if d.IsDir() {
				if d.Name() == "testdata" || d.Name() == "layerguard" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			if strings.HasPrefix(d.Name(), "adapter_") {
				adapters = append(adapters, rel)
				return nil
			}
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			for _, line := range strings.Split(string(b), "\n") {
				line = importPath.ReplaceAllString(line, "")
				for i, re := range res {
					if re.MatchString(line) {
						if got[rel] == nil {
							got[rel] = map[string]int{}
						}
						got[rel][terms[i].name]++
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(adapters)
	return got, adapters
}

//ci:whole-tree
func TestLayerHoldsNoSubjectOrWorkloadKnowledge(t *testing.T) {
	raw, err := os.ReadFile("allowlist.json")
	if err != nil {
		t.Fatal(err)
	}
	var al allowlist
	if err := json.Unmarshal(raw, &al); err != nil {
		t.Fatal(err)
	}
	got, adapters := count(t)
	if *dump {
		out, _ := json.MarshalIndent(got, "", "  ")
		fmt.Println(string(out))
		fmt.Println(adapters)
		return
	}
	if *update {
		lowerAllowlist(t, &al, got, adapters)
		return
	}
	var problems []string
	for f, ts := range got {
		ent, ok := al.Files[f]
		for name, n := range ts {
			switch allowed := ent.Terms[name]; {
			case !ok || n > allowed:
				problems = append(problems, fmt.Sprintf("%s: %d lines name %q, %d allowed: the lanes layer must not know the subject or the workload; put it behind a hook verb", f, n, name, allowed))
			case n < allowed:
				problems = append(problems, fmt.Sprintf("%s: %q is down to %d (allowlist %d): lower the allowlist (go test ./internal/layerguard -update-layer)", f, name, n, allowed))
			}
		}
	}
	for f, ent := range al.Files {
		if ent.Reason == "" {
			problems = append(problems, f+": an allowlist entry needs a reason")
		}
		for name, allowed := range ent.Terms {
			if got[f][name] == 0 && allowed > 0 {
				problems = append(problems, fmt.Sprintf("%s: %q no longer appears (allowlist %d): remove it", f, name, allowed))
			}
		}
	}
	for _, a := range adapters {
		if _, ok := al.Adapters[a]; !ok {
			problems = append(problems, a+": an adapter registration not in adapter_files (the list only shrinks; a new adapter needs its own review)")
		}
	}
	for a := range al.Adapters {
		if !contains(adapters, a) {
			problems = append(problems, a+": listed in adapter_files but gone: remove it")
		}
	}
	sort.Strings(problems)
	if len(problems) > 0 {
		t.Fatalf("%d layer violations:\n%s", len(problems), strings.Join(problems, "\n"))
	}
}

func contains(s []string, x string) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

// lowerAllowlist writes the allowlist with every count lowered to the code's; an entry with nothing left goes, an
// adapter file that is gone goes, and nothing is ever raised or added.
func lowerAllowlist(t *testing.T, al *allowlist, got map[string]map[string]int, adapters []string) {
	t.Helper()
	for f, ent := range al.Files {
		for name, allowed := range ent.Terms {
			switch n := got[f][name]; {
			case n == 0:
				delete(ent.Terms, name)
			case n < allowed:
				ent.Terms[name] = n
			}
		}
		if len(ent.Terms) == 0 {
			delete(al.Files, f)
		}
	}
	for a := range al.Adapters {
		if !contains(adapters, a) {
			delete(al.Adapters, a)
		}
	}
	out, err := json.MarshalIndent(al, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("allowlist.json", append(out, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
