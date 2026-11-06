package rendertest

import (
	"context"
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
	// must not read them.
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
//	inputs            Inputs name only files (or directories) that exist in FS
//	secret-identity   object refs are equal under SecretsOmit and SecretsInclude
//	secret-omit       SecretsOmit output carries no Secret data and reads no
//	                  SecretOnly file
//	errors            an error case fails with WantErr (and ErrLimit when set)
//	phases            Phases partition Objects and Desired excludes Ephemeral
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
	checkInputs(tc.FS, first, fail)
	checkPhases(first, fail)
	for _, file := range tc.SecretOnly {
		for _, in := range first.Inputs {
			if in == file {
				fail("secret-omit", fmt.Errorf("SecretsOmit read %s, which only a Secret consumes", file))
			}
		}
	}
	withSecrets, err := c.Render(ctx, tc.FS, include, id, config)
	if err != nil {
		fail("secret-identity", fmt.Errorf("SecretsInclude render failed: %w", err))
		return nil
	}
	if a, b := refs(first.Objects), refs(withSecrets.Objects); !reflect.DeepEqual(a, b) {
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

func checkInputs(fsys MapFS, res render.Result, fail func(string, error)) {
	for _, in := range res.Inputs {
		if dir, ok := strings.CutSuffix(in, "/"); ok {
			if _, err := fsys.Stat(strings.TrimPrefix(dir, "./")); dir != "." && err != nil {
				fail("inputs", fmt.Errorf("input directory %s is not in the FS", in))
			}
			continue
		}
		if _, ok := fsys[in]; !ok {
			fail("inputs", fmt.Errorf("input %s is not a file in the FS", in))
		}
	}
}

func checkPhases(res render.Result, fail func(string, error)) {
	phases := res.EffectivePhases()
	prev := 0
	desired := 0
	for _, p := range phases {
		if p.End < prev || p.End > len(res.Objects) {
			fail("phases", fmt.Errorf("phase %q end %d outside [%d, %d]", p.Name, p.End, prev, len(res.Objects)))
			return
		}
		if !p.Ephemeral {
			desired += p.End - prev
		}
		prev = p.End
	}
	if prev != len(res.Objects) {
		fail("phases", fmt.Errorf("phases cover %d of %d objects", prev, len(res.Objects)))
		return
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
