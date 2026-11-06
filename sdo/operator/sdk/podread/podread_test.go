package podread_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
)

func query() podread.Query {
	return podread.Query{ID: "policy", Target: podread.Target{Image: "synthetic-cache"}, Args: []string{"cache-cli", "get", "policy"}, Interval: time.Minute}
}

func TestQueryValidationBoundsTheDeclaration(t *testing.T) {
	for name, change := range map[string]func(*podread.Query){
		"empty id":        func(q *podread.Query) { q.ID = "" },
		"no role":         func(q *podread.Query) { q.Target = podread.Target{} },
		"bad image":       func(q *podread.Query) { q.Target.Image = "(" },
		"no executable":   func(q *podread.Query) { q.Args = nil },
		"empty command":   func(q *podread.Query) { q.Args = []string{""} },
		"too many args":   func(q *podread.Query) { q.Args = make([]string, podread.MaxArgs+1); q.Args[0] = "x" },
		"oversized arg":   func(q *podread.Query) { q.Args = []string{strings.Repeat("x", podread.MaxArgBytes+1)} },
		"too frequent":    func(q *podread.Query) { q.Interval = podread.MinInterval - time.Second },
		"interval absent": func(q *podread.Query) { q.Interval = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			q := query()
			change(&q)
			if err := q.Validate(); err == nil {
				t.Fatal("Validate accepted the query")
			}
		})
	}
	if err := query().Validate(); err != nil {
		t.Fatal(err)
	}
}

type reader struct {
	id      string
	watches []sdk.WatchKind
	queries []podread.Query
}

func (r reader) Spec() sdk.DetectorSpec { return sdk.DetectorSpec{ID: r.id, Watches: r.watches} }
func (reader) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
func (r reader) PodReads() []podread.Query { return r.queries }

var podWatch = []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}

func TestDetectorDeclarations(t *testing.T) {
	if err := podread.ValidateDetector(reader{id: "a", queries: []podread.Query{query()}}); err == nil || !strings.Contains(err.Error(), "v1 Pod watch") {
		t.Fatalf("no Pod watch: %v", err)
	}
	if err := podread.ValidateDetector(reader{id: "a"}); err != nil {
		t.Fatalf("a detector without reads needs no Pod watch: %v", err)
	}
	same := reader{id: "a", watches: podWatch, queries: []podread.Query{query()}}
	merged, err := podread.Queries([]sdk.Detector{same, reader{id: "b", watches: podWatch, queries: []podread.Query{query()}}})
	if err != nil || len(merged) != 1 {
		t.Fatalf("identical queries merge: %v, %v", merged, err)
	}
	other := query()
	other.Args = []string{"other"}
	if _, err := podread.Queries([]sdk.Detector{same, reader{id: "b", watches: podWatch, queries: []podread.Query{other}}}); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting definitions: %v", err)
	}
	many := make([]podread.Query, podread.MaxQueries+1)
	for i := range many {
		many[i] = query()
		many[i].ID = strings.Repeat("q", i+1)
	}
	if _, err := podread.Queries([]sdk.Detector{reader{id: "a", watches: podWatch, queries: many}}); err == nil {
		t.Fatal("excess queries accepted")
	}
}

func TestTargetSelectsByRole(t *testing.T) {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "anything", Labels: map[string]string{"tier": "cache"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "example.invalid/proxy:1"}, {Name: "store", Image: "example.invalid/synthetic-cache:2"}}},
	}
	for name, test := range map[string]struct {
		target    podread.Target
		container string
		selected  bool
	}{
		"image picks the container": {podread.Target{Image: "synthetic-cache"}, "store", true},
		"labels read the first":     {podread.Target{Labels: map[string]string{"tier": "cache"}}, "sidecar", true},
		"both must match":           {podread.Target{Labels: map[string]string{"tier": "web"}, Image: "synthetic-cache"}, "", false},
		"image absent":              {podread.Target{Image: "other-store"}, "", false},
	} {
		t.Run(name, func(t *testing.T) {
			container, selected := test.target.Select(pod)
			if container != test.container || selected != test.selected {
				t.Fatalf("Select = %q, %v", container, selected)
			}
		})
	}
	target := podread.Target{Image: "synthetic-cache"}
	finished := pod
	finished.Status.Phase = corev1.PodSucceeded
	terminating := pod
	terminating.DeletionTimestamp = &metav1.Time{}
	for name, p := range map[string]corev1.Pod{"finished": finished, "terminating": terminating} {
		if _, ok := target.Select(p); ok {
			t.Fatalf("%s Pod selected", name)
		}
	}
}

func TestRequireNeverReportsAFailedReadAsAResult(t *testing.T) {
	good := podread.Reading{Pod: "p", Stdout: "value"}
	for name, result := range map[string]podread.Result{
		"source error": {Error: "no Pod holds the role"},
		"no readings":  {},
		"reading":      {Readings: []podread.Reading{good, {Pod: "q", Error: "timed out"}}},
		"truncated":    {Readings: []podread.Reading{good, {Pod: "q", Truncated: true}}},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := sdktest.Snapshot{PodReads: map[string]podread.Result{"policy": result}}
			if _, err := podread.Require(snapshot, "policy"); err == nil {
				t.Fatal("Require accepted the result")
			}
			if _, ok := podread.ResultFrom(snapshot, "policy"); !ok {
				t.Fatal("ResultFrom hides a failed result from an author judging it")
			}
		})
	}
	if _, err := podread.Require(sdktest.Snapshot{}, "policy"); err == nil {
		t.Fatal("missing input is not an error")
	}
	if _, err := podread.Require(sdktest.Snapshot{PodReads: map[string]podread.Result{"policy": {Readings: []podread.Reading{good, {Pod: "q", ExitCode: 1}}}}}, "policy"); err != nil {
		t.Fatalf("a nonzero exit is a result: %v", err)
	}
}
