package operatorcli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

type simStateObservation struct {
	at     time.Time
	rv     string
	absent bool
}

// The independent oracle knows synthetic upstream observations, not the reader's
// matching rules. Arrival order is shuffled independently of virtual finish time.
func runEvidenceSchedule(t *testing.T, seed int64, schedule []byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	schedule = schedule[:min(len(schedule), 32)]
	choice := func(i, n int) int {
		if i < len(schedule) {
			return int(schedule[i]) % n
		}
		return rng.Intn(n)
	}
	dir := filepath.Join(t.TempDir(), "history")
	store, err := apihistory.Open(dir, "synthetic", apihistory.Limits{Queue: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	type operation struct {
		start, end  apihistory.Event
		body        []byte
		observation *simStateObservation
	}
	operations := make([]operation, 24)
	var firstMutation time.Time
	var observations []simStateObservation
	for i := range operations {
		action := i % 12
		if i >= 12 {
			action = choice(i, 12)
		}
		start := base.Add(time.Duration(i*3+1) * time.Second)
		finish := start.Add(time.Second)
		event := apihistory.Event{RequestID: fmt.Sprint(i), Phase: "end", Started: start, Finished: finish, Verb: "get", Resource: "pods", Namespace: "app", Name: "p", Status: 200}
		rv := fmt.Sprint(i + 1)
		object := func(name, namespace string) map[string]any {
			return map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": name, "namespace": namespace, "resourceVersion": rv}}
		}
		var body any = object("p", "app")
		var observation *simStateObservation
		switch action {
		case 0:
			observation = &simStateObservation{at: finish, rv: rv}
		case 1:
			event.Verb = "patch"
			event.DryRun = true
		case 2:
			event.Name = "other"
			body = object("other", "app")
		case 3:
			event.Group = "other.example"
			body = map[string]any{"apiVersion": "other.example/v1", "kind": "Pod", "metadata": map[string]any{"name": "p", "namespace": "app", "resourceVersion": rv}}
		case 4:
			event.Name = ""
			event.Verb = "list"
			event.Partial = true
			body = map[string]any{"apiVersion": "v1", "kind": "PodList", "items": []any{}}
		case 5:
			event.Name = ""
			event.Verb = "list"
			body = map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"continue": "synthetic"}, "items": []any{}}
		case 6:
			event.Subresource = "exec"
			event.DryRun = true
			body = nil
		case 7:
			event.Status = 404
			body = nil
			observation = &simStateObservation{at: finish, absent: true}
		case 8:
			event.Name = ""
			event.Verb = "list"
			body = map[string]any{"apiVersion": "v1", "kind": "PodList", "items": []any{}}
			observation = &simStateObservation{at: finish, absent: true}
		case 9:
			event.Name = ""
			event.Namespace = ""
			event.Verb = "list"
			event.Partial = true
			body = map[string]any{"apiVersion": "v1", "kind": "PodList", "items": []any{object("p", "other"), object("p", "app")}}
			observation = &simStateObservation{at: finish, rv: rv}
		case 10:
			event.Verb = "patch"
			observation = &simStateObservation{at: finish, rv: rv}
		case 11:
			event.Status = 500
		}
		// Actions 6 and 10 are the only non-dry-run mutations in the model.
		if (action == 6 || action == 10) && firstMutation.IsZero() {
			firstMutation = start
		}
		var raw []byte
		if body != nil {
			raw, err = json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
		}
		begin := event
		begin.Phase = "start"
		begin.Finished = time.Time{}
		begin.Status = 0
		operations[i] = operation{begin, event, raw, observation}
		if observation != nil {
			observations = append(observations, *observation)
		}
	}
	readCLI := func(args ...string) []byte {
		t.Helper()
		var out bytes.Buffer
		args = append(args, "--directory", dir)
		if err := RunEvidence(args, func(string) string { return "" }, &out); err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
	}
	check := func(cut time.Time, closed bool) {
		t.Helper()
		raw := readCLI("state", "Pod/app/p", "--at", cut.Format(time.RFC3339Nano))
		var state observedState
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		if !state.Unknown || !state.Stale || state.Origin != "responder_api_proxy" {
			t.Fatalf("invented continuity: %s", raw)
		}
		if state.ObservedAt.After(cut) {
			t.Fatalf("future observation: %s", raw)
		}
		var latest *simStateObservation
		for i := range observations {
			candidate := &observations[i]
			if !candidate.at.After(cut) && (latest == nil || candidate.at.After(latest.at)) {
				latest = candidate
			}
		}
		if !state.ObservedAt.IsZero() {
			history, err := apihistory.Read(dir)
			if err != nil {
				t.Fatal(err)
			}
			provenance := false
			for _, event := range history.Events {
				if event.Sequence == state.Event && event.Finished.Equal(state.ObservedAt) && event.Group == "" && event.Resource == "pods" && !event.DryRun && event.Subresource == "" {
					provenance = true
				}
			}
			if !provenance {
				t.Fatalf("invalid event provenance: %s", raw)
			}
			found := false
			for _, o := range observations {
				if o.at.Equal(state.ObservedAt) && o.absent == state.ObservedAbsent {
					var object struct {
						Metadata struct {
							ResourceVersion string `json:"resourceVersion"`
						} `json:"metadata"`
					}
					if !o.absent {
						if err := json.Unmarshal(state.LastObserved, &object); err != nil {
							t.Fatal(err)
						}
					}
					if o.absent || object.Metadata.ResourceVersion == o.rv {
						found = true
					}
				}
			}
			if !found {
				t.Fatalf("seed=%d schedule=%x fabricated provenance: %s", seed, schedule, raw)
			}
		}
		if closed {
			if latest == nil {
				if !state.ObservedAt.IsZero() {
					t.Fatal("observation before first upstream result")
				}
			} else if !state.ObservedAt.Equal(latest.at) || state.ObservedAbsent != latest.absent {
				t.Fatalf("seed=%d wrong latest at %s: %s want %+v", seed, cut, raw, latest)
			}
		}
	}
	for admitted, i := range rng.Perm(len(operations)) {
		op := operations[i]
		store.Record(op.start, nil)
		store.Record(op.end, op.body)
		check(base.Add(time.Duration(choice(i, 80))*time.Second), false)
		raw := readCLI("history", "--limit", "3", "--offset", fmt.Sprint(choice(i, 8)))
		var page struct {
			Events []apihistory.Event `json:"events"`
			Next   int                `json:"next_offset"`
		}
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		if len(page.Events) > 3 {
			t.Fatal("history page exceeded limit")
		}
		if len(page.Events) > 0 {
			sequence := page.Events[0].Sequence
			var shown struct {
				Event apihistory.Event `json:"event"`
			}
			if err := json.Unmarshal(readCLI("show", "--event", fmt.Sprint(sequence)), &shown); err != nil {
				t.Fatal(err)
			}
			if shown.Event.Sequence != sequence || shown.Event.RequestID != page.Events[0].RequestID {
				t.Fatal("show borrowed another envelope")
			}
		}
		// Keep the exact-state oracle independent of projection admission loss.
		// Reads above observe arbitrary in-flight cuts; this barrier releases
		// the next operation only when both current envelopes are visible.
		deadline := time.Now().Add(2 * time.Second)
		for {
			history, err := apihistory.Read(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(history.Events) == 2*(admitted+1) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("seed=%d schedule=%x recording stalled", seed, schedule)
			}
			time.Sleep(time.Millisecond)
		}
	}
	store.Close()
	h, err := apihistory.Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	// The admission barriers make every scheduled observation available here.
	if h.Manifest.Complete {
		for i := 0; i < 80; i++ {
			check(base.Add(time.Duration(i)*time.Second), true)
		}
		raw := readCLI("state", "Pod/app/p", "--at", "before-first-mutation")
		var state observedState
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		if !state.RequestedAt.Equal(firstMutation) {
			t.Fatalf("dryRun/exec boundary: %s want %s", raw, firstMutation)
		}
		if state.ObservedAt.After(firstMutation) {
			t.Fatal("borrowed post-mutation result")
		}
	} else {
		t.Fatalf("seed=%d schedule=%x controlled recording lost completeness: %+v", seed, schedule, h.Manifest)
	}
	// CLI reads must preserve every stored byte.
	snapshot := func() map[string]string {
		t.Helper()
		files := map[string]string{}
		err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				b, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				files[p] = string(b)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	before := snapshot()
	for _, selector := range []string{"Pod/app/p", "Pod.other.example/app/p", "Pod/app/never", "Pod/other/p"} {
		raw := readCLI("state", selector, "--at", base.Add(100*time.Second).Format(time.RFC3339Nano))
		var state observedState
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		if selector == "Pod.other.example/app/p" && !state.ObservedAt.IsZero() {
			t.Fatalf("borrowed API group: %s", raw)
		}
		if selector == "Pod/app/never" && len(state.LastObserved) > 0 {
			t.Fatalf("borrowed object: %s", raw)
		}
	}
	after := snapshot()
	if len(before) != len(after) {
		t.Fatal("reader created files")
	}
	for p, b := range before {
		if after[p] != b {
			t.Fatalf("reader mutated %s", p)
		}
	}
}

func TestSimEvidenceSchedule(t *testing.T) {
	seeds := []int64{1, 21, 81, 135, 20261022}
	if raw := os.Getenv("SDO_CHAOS_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = []int64{seed}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runEvidenceSchedule(t, seed, nil) })
	}
}
func FuzzEvidenceHistorySchedule(f *testing.F) {
	f.Add(int64(1), []byte{})
	f.Add(int64(81), []byte{0, 1, 6, 10})
	f.Add(int64(135), []byte{9, 5, 7, 8})
	f.Fuzz(func(t *testing.T, seed int64, schedule []byte) { runEvidenceSchedule(t, seed, schedule) })
}
