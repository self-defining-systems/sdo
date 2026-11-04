package validate

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"sdo.dev/operator/memory/layout"
)

// faultMarker in the cache detector makes the fake compile report a
// healthy-baseline violation whose message is the rest of the marker line.
const faultMarker = "// fires on healthy: "

// healthyReplayCompile stands in for the compile runner: it reports what the
// tree at request.Root would report on the healthy replay, and records which
// trees it was asked to compile.
type healthyReplayCompile struct {
	mu    sync.Mutex
	roots []string
}

func (c *healthyReplayCompile) run(_ context.Context, request Request) ([]Diagnostic, error) {
	c.mu.Lock()
	c.roots = append(c.roots, request.Root)
	c.mu.Unlock()
	source, err := os.ReadFile(filepath.Join(request.Root, gateCache))
	if err != nil {
		return nil, err
	}
	_, message, found := strings.Cut(string(source), faultMarker)
	if !found {
		return nil, nil
	}
	message, _, _ = strings.Cut(message, "\n")
	return []Diagnostic{{Rule: RuleDetectorHealthyBaseline, Path: layout.DetectorDir(layout.ClassIncident, "cache"), Message: message}}, nil
}

func (c *healthyReplayCompile) baseRuns(candidate string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	runs := 0
	for _, root := range c.roots {
		if root != candidate {
			runs++
		}
	}
	return runs
}

func fireOnHealthy(message string) func(*testing.T, string) {
	return func(t *testing.T, root string) {
		data, err := os.ReadFile(filepath.Join(root, gateCache))
		if err != nil {
			t.Fatal(err)
		}
		source := string(data)
		if before, _, found := strings.Cut(source, faultMarker); found {
			source = before
		}
		write(t, root, gateCache, strings.TrimRight(source, "\n")+"\n\n"+faultMarker+message+"\n")
	}
}

func sharedLibraryEdit(t *testing.T, root string) {
	write(t, root, ".sdo/lib/shared/shared.go", "package shared\n\nconst Name = \"shared\"\n")
}

// A healthy-baseline fault is pre-existing only when the accepted base tree,
// replayed on the same healthy sample, fails with the same detector and
// violation. Check establishes that fact by compiling the base; which paths
// the candidate changed decides nothing.
func TestCheckEstablishesPreexistingHealthyBaselineFaultsAtTheBase(t *testing.T) {
	const fault = `detector "cache" reported an active finding on healthy baseline snapshot healthy.json`
	for _, tc := range []struct {
		name     string
		base     func(*testing.T, string)
		edit     func(*testing.T, string)
		blocking bool
	}{
		{"playbook-only change; the base fails the same way", fireOnHealthy(fault), gatePlaybookEdit, false},
		{"shared library changed; the base fails the same way", fireOnHealthy(fault), func(t *testing.T, r string) {
			gatePlaybookEdit(t, r)
			sharedLibraryEdit(t, r)
		}, false},
		{"the base is quiet; the candidate's edit introduced the fault", nil, func(t *testing.T, r string) {
			gatePlaybookEdit(t, r)
			fireOnHealthy(fault)(t, r)
		}, true},
		{"the base fails differently", fireOnHealthy("another violation"), func(t *testing.T, r string) {
			gatePlaybookEdit(t, r)
			fireOnHealthy(fault)(t, r)
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("XDG_CACHE_HOME", t.TempDir())
			root := goodTree(t)
			gateOutcome(gateClean())(t, root)
			if tc.base != nil {
				tc.base(t, root)
			}
			commit(t, root, gateOutcomeMessage)
			tc.edit(t, root)
			compile := &healthyReplayCompile{}
			diagnostics, notes := checkReflection(t, root, compile)
			blocked := slices.ContainsFunc(diagnostics, func(d Diagnostic) bool { return d.Rule == RuleDetectorHealthyBaseline })
			if blocked != tc.blocking {
				t.Fatalf("healthy-baseline blocking = %v, want %v: %v", blocked, tc.blocking, diagnostics)
			}
			noted := slices.ContainsFunc(notes, func(d Diagnostic) bool {
				return d.Rule == RuleDetectorHealthyBaseline && strings.Contains(d.Message, "pre-existing") && strings.Contains(d.Message, fault)
			})
			if noted == tc.blocking {
				t.Fatalf("pre-existing note = %v, want %v: %v", noted, !tc.blocking, notes)
			}
			if compile.baseRuns(root) != 1 {
				t.Fatalf("base replays = %d, want 1: %v", compile.baseRuns(root), compile.roots)
			}
		})
	}
}

// The base result is a function of the base commit and the healthy sample, so
// repeated checks against one base replay the base once.
func TestCheckReplaysTheBaseOncePerBaseCommitAndSample(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	const fault = "fires"
	root := goodTree(t)
	gateOutcome(gateClean())(t, root)
	fireOnHealthy(fault)(t, root)
	commit(t, root, gateOutcomeMessage)
	gatePlaybookEdit(t, root)
	sharedLibraryEdit(t, root)
	capture := t.TempDir()
	write(t, capture, "healthy/healthy.json", "{}")
	compile := &healthyReplayCompile{}
	for range 2 {
		diagnostics, _ := checkReflection(t, root, compile, func(r *Request) { r.CaptureDirectory = capture })
		if len(diagnostics) != 0 {
			t.Fatalf("pre-existing fault rejected the candidate: %v", diagnostics)
		}
	}
	if runs := compile.baseRuns(root); runs != 1 {
		t.Fatalf("base replays = %d, want 1", runs)
	}
	write(t, capture, "healthy/healthy.json", `{"changed": true}`)
	checkReflection(t, root, compile, func(r *Request) { r.CaptureDirectory = capture })
	if runs := compile.baseRuns(root); runs != 2 {
		t.Fatalf("base replays after the sample changed = %d, want 2", runs)
	}
}

func checkReflection(t *testing.T, root string, compile *healthyReplayCompile, options ...func(*Request)) ([]Diagnostic, []Diagnostic) {
	t.Helper()
	var notes []Diagnostic
	request := Request{Root: root, Base: "HEAD", Actor: Responder, Phase: ReflectionPhase, CoverageGaps: &notes,
		Decision: gateUpdated("base"), Publication: &PublicationContext{BaseCommit: "HEAD", IncidentID: gateIncident}}
	for _, option := range options {
		option(&request)
	}
	diagnostics, err := Check(context.Background(), request, compile.run)
	if err != nil {
		t.Fatal(err)
	}
	return diagnostics, notes
}
