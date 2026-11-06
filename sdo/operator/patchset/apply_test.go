package patchset

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/patchset/internal/gitio"
)

const baseTree = "0123456789abcdef0123456789abcdef01234567"

// fakeEdge scripts git by subcommand so the classification logic runs without
// a process. forward and reverse are the exit codes of `apply --check` and
// `apply --check --reverse`.
type fakeEdge struct {
	forward, reverse int
	listed           string // ls-files output
	calls            [][]string
	cleaned          bool
	runErr           error
}

func (f *fakeEdge) Scratch() (string, func(), error) {
	return "/scratch", func() { f.cleaned = true }, nil
}

func (f *fakeEdge) Run(_ context.Context, c gitio.Call) (gitio.Output, error) {
	if f.runErr != nil {
		return gitio.Output{}, f.runErr
	}
	args := c.Args[slices.Index(c.Args, "core.hooksPath=/dev/null")+1:]
	f.calls = append(f.calls, args)
	switch args[0] {
	case "rev-parse":
		if slices.Contains(args, "--absolute-git-dir") {
			return gitio.Output{Stdout: []byte("/store/.git\n")}, nil
		}
		return gitio.Output{Stdout: []byte("/store/.git/objects\n")}, nil
	case "cat-file":
		return gitio.Output{Stdout: []byte("tree\n")}, nil
	case "write-tree":
		return gitio.Output{Stdout: []byte(strings.Repeat("b", 40) + "\n")}, nil
	case "ls-files":
		return gitio.Output{Stdout: []byte(f.listed)}, nil
	case "apply":
		if slices.Contains(args, "--check") {
			if slices.Contains(args, "--reverse") {
				return gitio.Output{ExitCode: f.reverse, Stderr: []byte("reverse")}, nil
			}
			return gitio.Output{ExitCode: f.forward, Stderr: []byte("error: patch failed: charts/x/job.yaml:1")}, nil
		}
	}
	return gitio.Output{}, nil
}

func oneSeries(t *testing.T) Series {
	t.Helper()
	s, err := ParseSeries([]File{file("0001-a.patch", valid())})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestApplyClassifiesFailures(t *testing.T) {
	cases := []struct {
		name string
		edge fakeEdge
		kind Kind
		path string
	}{
		{"conflict", fakeEdge{forward: 1, reverse: 1, listed: "charts/x/job.yaml\x00"}, KindConflict, "charts/x/job.yaml"},
		{"upstreamed", fakeEdge{forward: 1, reverse: 0}, KindUpstreamed, "charts/x/job.yaml"},
		{"missing", fakeEdge{forward: 1, reverse: 1, listed: ""}, KindMissingFile, "charts/x/job.yaml"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			edge := c.edge
			_, err := NewApplier(&edge).Apply(context.Background(), Store{Dir: "/store"}, baseTree, oneSeries(t))
			pe := mustKind(t, err, c.kind)
			if pe.Patch != "0001-a.patch" || !slices.Contains(pe.Paths, c.path) {
				t.Fatalf("error = %+v", pe)
			}
			if !edge.cleaned {
				t.Fatal("scratch directory leaked on failure")
			}
		})
	}
}

func TestApplyConflictDetailCarriesGitsRejection(t *testing.T) {
	edge := &fakeEdge{forward: 1, reverse: 1, listed: "charts/x/job.yaml\x00"}
	_, err := NewApplier(edge).Apply(context.Background(), Store{Dir: "/store"}, baseTree, oneSeries(t))
	if pe := mustKind(t, err, KindConflict); !strings.Contains(pe.Detail, "patch failed: charts/x/job.yaml:1") {
		t.Fatalf("detail = %q", pe.Detail)
	}
}

func TestApplyEmptySeriesStillReadsAndWritesTheTree(t *testing.T) {
	edge := &fakeEdge{}
	a, err := NewApplier(edge).Apply(context.Background(), Store{Dir: "/store"}, baseTree, Series{})
	if err != nil {
		t.Fatal(err)
	}
	if a.Tree != strings.Repeat("b", 40) || a.ObjectDir != "/scratch/objects" || a.Alternate != "/store/.git/objects" {
		t.Fatalf("applied = %+v", a)
	}
	a.Close()
	if !edge.cleaned {
		t.Fatal("Close did not remove the scratch directory")
	}
}

func TestApplyRefusesBadInputsAndSurfacesInfrastructureErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := NewApplier(&fakeEdge{}).Apply(ctx, Store{}, baseTree, Series{}); err == nil {
		t.Fatal("empty store accepted")
	}
	if _, err := NewApplier(&fakeEdge{}).Apply(ctx, Store{Dir: "/s"}, "HEAD", Series{}); err == nil {
		t.Fatal("non-object base accepted")
	}
	boom := errors.New("git missing")
	_, err := NewApplier(&fakeEdge{runErr: boom}).Apply(ctx, Store{Dir: "/s"}, baseTree, Series{})
	var pe *PatchError
	if !errors.Is(err, boom) || errors.As(err, &pe) {
		t.Fatalf("infrastructure failure must not look like a patch error: %v", err)
	}
}

// The command line is the determinism contract: zero configuration inputs.
func TestApplyUsesConfigurationFreeGit(t *testing.T) {
	edge := &fakeEdge{}
	if _, err := NewApplier(edge).Apply(context.Background(), Store{Dir: "/s"}, baseTree, oneSeries(t)); err != nil {
		t.Fatal(err)
	}
	var applied [][]string
	for _, c := range edge.calls {
		if c[0] == "apply" {
			applied = append(applied, c)
		}
	}
	if len(applied) != 2 {
		t.Fatalf("want a check and an apply, got %v", applied)
	}
	for _, c := range applied {
		for _, want := range []string{"--cached", "--whitespace=nowarn", "-p1"} {
			if !slices.Contains(c, want) {
				t.Fatalf("%v lacks %s", c, want)
			}
		}
		for _, banned := range []string{"-C1", "-C0", "--3way", "--reject", "--ignore-whitespace", "--unsafe-paths"} {
			if slices.Contains(c, banned) {
				t.Fatalf("%v uses %s", c, banned)
			}
		}
	}
}
