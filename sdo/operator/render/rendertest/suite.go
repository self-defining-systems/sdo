package rendertest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/render"
)

// Case is one conformance fixture.
type Case struct {
	Name   string
	FS     MapFS
	Env    render.Env // Secrets is overridden per property
	Config proto.Message
	// Want lists the expected desired objects as "Kind namespace/name"
	// (compared as a set). Nil skips the comparison.
	Want []string
	// WantErr, when set, makes the case an error case: the render must fail
	// with an error containing it, under Env as given.
	WantErr string
	// WantLimit additionally requires the error to wrap render.ErrLimit.
	WantLimit bool
	// Limits, when set, replaces the catalog's read limits for this case.
	Limits *render.Limits
	// SecretOnly lists files that only a Secret consumes. A SecretsOmit render
	// must declare them (Stat, so they stay attributable inputs) but not read
	// them.
	SecretOnly []string
}

// Failure is one violated property.
type Failure struct {
	Case     string
	Property string
	Err      error
}

func (f Failure) Error() string { return fmt.Sprintf("%s: %s: %v", f.Case, f.Property, f.Err) }

// TestRenderer runs the renderer family contract against id in c, one subtest
// per case. See Check for the properties.
func TestRenderer(t *testing.T, c *render.Catalog, id string, cases []Case) {
	t.Helper()
	if len(cases) == 0 {
		t.Fatalf("renderer %q has no conformance cases", id)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, f := range Check(c, id, []Case{tc}) {
				t.Errorf("%s: %v", f.Property, f.Err)
			}
		})
	}
}

// Check runs the contract and returns the violations. The properties:
//
//	determinism       repeated renders over equal FS/Env/config are deep-equal
//	inputs            Inputs are truthful: files exist with the recorded Git
//	                  blob ID, listed paths are directories, absent paths do not
//	                  exist
//	inputs-modes      the Input paths are equal under SecretsOmit and
//	                  SecretsInclude, so Secret-only changes stay attributable
//	secret-identity   object refs are equal under SecretsOmit and SecretsInclude
//	secret-omit       SecretsOmit output carries no Secret data and does not
//	                  read any SecretOnly file
//	errors            an error case fails with WantErr (and ErrLimit when set)
//	output-limit      a render whose encoded objects exceed Limits.OutputBytes
//	                  fails with *render.OutputLimitError (matching ErrLimit)
//	phases            phase names are unique and Desired excludes Ephemeral
//	want              the desired objects equal Want
func Check(c *render.Catalog, id string, cases []Case) []Failure {
	var failures []Failure
	for _, tc := range cases {
		cat := c
		if tc.Limits != nil {
			cat = c.WithLimits(*tc.Limits)
		}
		fail := func(property string, err error) {
			failures = append(failures, Failure{Case: tc.Name, Property: property, Err: err})
		}
		failures = append(failures, checkCase(cat, id, tc, fail)...)
	}
	return failures
}

func checkCase(c *render.Catalog, id string, tc Case, fail func(string, error)) []Failure {
	config, err := configAny(tc.Config)
	if err != nil {
		fail("config", err)
		return nil
	}
	omit := tc.Env
	omit.Secrets = render.SecretsOmit
	include := tc.Env
	include.Secrets = render.SecretsInclude
	ctx := context.Background()

	if tc.WantErr != "" {
		_, err := c.Render(ctx, tc.FS, tc.Env, id, config)
		switch {
		case err == nil:
			fail("errors", fmt.Errorf("render succeeded, want error containing %q", tc.WantErr))
		case !strings.Contains(err.Error(), tc.WantErr):
			fail("errors", fmt.Errorf("error %q does not contain %q", err, tc.WantErr))
		case tc.WantLimit && !errors.Is(err, render.ErrLimit):
			fail("errors", fmt.Errorf("error %q does not wrap render.ErrLimit", err))
		}
		return nil
	}

	first, err := c.Render(ctx, tc.FS, omit, id, config)
	if err != nil {
		// A Secret leak under SecretsOmit is rejected by the catalog; report
		// it under its property rather than as a generic failure.
		property := "render"
		if strings.Contains(err.Error(), "under SecretsOmit") {
			property = "secret-omit"
		}
		fail(property, err)
		return nil
	}
	for i := 0; i < 3; i++ {
		again, err := c.Render(ctx, tc.FS, omit, id, config)
		if err != nil {
			fail("determinism", fmt.Errorf("render %d failed after a success: %w", i+2, err))
			return nil
		}
		if !reflect.DeepEqual(first, again) {
			fail("determinism", fmt.Errorf("render %d differs from the first", i+2))
			break
		}
	}
	checkOutputLimit(c, id, tc, omit, config, first, fail)
	checkInputs(tc.FS, first, fail)
	checkPhases(first, fail)
	for _, file := range tc.SecretOnly {
		if in, ok := findInput(first.Inputs, file); ok && in.Kind == render.InputRead {
			fail("secret-omit", fmt.Errorf("SecretsOmit read %s, which only a Secret consumes", file))
		}
	}
	withSecrets, err := c.Render(ctx, tc.FS, include, id, config)
	if err != nil {
		fail("secret-identity", fmt.Errorf("SecretsInclude render failed: %w", err))
		return nil
	}
	if a, b := inputPaths(first.Inputs), inputPaths(withSecrets.Inputs); !reflect.DeepEqual(a, b) {
		fail("inputs-modes", fmt.Errorf("input paths differ between modes: omit %v, include %v", a, b))
	}
	if a, b := refs(first.Objects()), refs(withSecrets.Objects()); !reflect.DeepEqual(a, b) {
		fail("secret-identity", fmt.Errorf("object refs differ between modes: omit %v, include %v", a, b))
	}
	if tc.Want != nil {
		want := append([]string(nil), tc.Want...)
		sort.Strings(want)
		if got := refs(first.Desired()); !reflect.DeepEqual(got, want) {
			fail("want", fmt.Errorf("desired objects %v, want %v", got, want))
		}
	}
	return nil
}

func configAny(m proto.Message) (*anypb.Any, error) {
	if m == nil {
		return nil, errors.New("case has no Config")
	}
	return anypb.New(m)
}

func findInput(inputs []render.Input, path string) (render.Input, bool) {
	for _, in := range inputs {
		if in.Path == path {
			return in, true
		}
	}
	return render.Input{}, false
}

func inputPaths(inputs []render.Input) []string {
	out := make([]string, 0, len(inputs))
	for _, in := range inputs {
		out = append(out, in.Path)
	}
	return out
}

func checkInputs(fsys MapFS, res render.Result, fail func(string, error)) {
	for _, in := range res.Inputs {
		switch in.Kind {
		case render.InputRead, render.InputStat:
			content, ok := fsys[in.Path]
			switch {
			case ok:
				if want := render.GitBlobID([]byte(content)); in.Blob != want {
					fail("inputs", fmt.Errorf("%s input %s has blob %q, want %q", in.Kind, in.Path, in.Blob, want))
				}
			case in.Kind == render.InputStat && isDir(fsys, in.Path):
			default:
				fail("inputs", fmt.Errorf("%s input %s is not in the FS", in.Kind, in.Path))
			}
		case render.InputListed:
			if !isDir(fsys, in.Path) {
				fail("inputs", fmt.Errorf("listed input %s is not a directory in the FS", in.Path))
			}
		case render.InputAbsent:
			if _, ok := fsys[in.Path]; ok || isDir(fsys, in.Path) {
				fail("inputs", fmt.Errorf("absent input %s exists in the FS", in.Path))
			}
		default:
			fail("inputs", fmt.Errorf("input %s has unknown kind %d", in.Path, in.Kind))
		}
	}
}

func isDir(fsys MapFS, path string) bool {
	info, err := fsys.Stat(path)
	return err == nil && info.IsDir()
}

func checkPhases(res render.Result, fail func(string, error)) {
	names := map[string]bool{}
	desired := 0
	for _, p := range res.Phases {
		if p.Name == "" || names[p.Name] {
			fail("phases", fmt.Errorf("phase name %q is empty or repeated", p.Name))
		}
		names[p.Name] = true
		if !p.Ephemeral {
			desired += len(p.Objects)
		}
	}
	if got := len(res.Desired()); got != desired {
		fail("phases", fmt.Errorf("Desired has %d objects, non-ephemeral phases hold %d", got, desired))
	}
}

func refs(objs []*unstructured.Unstructured) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, fmt.Sprintf("%s %s/%s", o.GetKind(), o.GetNamespace(), o.GetName()))
	}
	sort.Strings(out)
	return out
}

// checkOutputLimit re-renders under a catalog whose output cap is one byte
// below the first render's encoded size and requires the typed error. It is
// skipped for renders with no objects.
func checkOutputLimit(c *render.Catalog, id string, tc Case, env render.Env, config *anypb.Any, first render.Result, fail func(string, error)) {
	var size int64
	for _, obj := range first.Objects() {
		b, err := json.Marshal(obj.Object)
		if err != nil {
			fail("output-limit", err)
			return
		}
		size += int64(len(b))
	}
	if size < 2 {
		return
	}
	lim := render.DefaultLimits
	if tc.Limits != nil {
		lim = *tc.Limits
	}
	lim.OutputBytes = size - 1
	_, err := c.WithLimits(lim).Render(context.Background(), tc.FS, env, id, config)
	var typed *render.OutputLimitError
	switch {
	case err == nil:
		fail("output-limit", fmt.Errorf("render of %d encoded bytes succeeded under an output limit of %d", size, size-1))
	case !errors.As(err, &typed) || !errors.Is(err, render.ErrLimit):
		fail("output-limit", fmt.Errorf("error %q is not *render.OutputLimitError matching ErrLimit", err))
	}
}
