package apihistory

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func historySimSeeds(t *testing.T) []int64 {
	t.Helper()
	if value := os.Getenv("SDO_CHAOS_SEED"); value != "" {
		seed, err := strconv.ParseInt(value, 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		return []int64{seed}
	}
	return []int64{1, 21, 81, 135, 20261022}
}

// The clock controls envelope times. Disk/worker completion uses Close barriers,
// not sleeps; neither the production ticker nor the OS scheduler is virtualized.
func TestSimHistoryLifecycle(t *testing.T) {
	for _, seed := range historySimSeeds(t) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			dir := filepath.Join(t.TempDir(), "history")
			limits := Limits{Bytes: int64(1500 + rng.Intn(5000)), Events: uint64(4 + rng.Intn(16)), Queue: 2 + rng.Intn(8)}
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			var previousEpoch uint64
			for epoch := 0; epoch < 4; epoch++ {
				lease, err := BeginWriter(dir)
				if err != nil {
					t.Fatal(err)
				}
				store, err := Open(dir, "synthetic", limits)
				if err != nil {
					lease.Close()
					t.Fatal(err)
				}
				for i := 0; i < 12; i++ {
					now = now.Add(time.Duration(1+rng.Intn(100)) * time.Millisecond)
					id := fmt.Sprintf("%d-%d", epoch, i)
					event := Event{RequestID: id, Phase: "start", Verb: "get", Started: now}
					store.Record(event, nil)
					// A seed chooses a crash cut leaving some requests without end envelopes.
					if rng.Intn(5) != 0 {
						event.Phase = "end"
						event.Finished = now.Add(time.Millisecond)
						store.Record(event, nil)
					}
					if rng.Intn(7) == 0 {
						store.Gap()
					}
					h, err := Read(dir)
					if err != nil {
						t.Fatal(err)
					}
					if h.Manifest.Complete {
						t.Fatal("live writer certified complete history")
					}
					if got := store.queuedBytes.Load(); got < 0 || got > MaxQueuedBytes {
						t.Fatalf("queued bytes=%d", got)
					}
				}
				store.Close()
				lease.Close()
				h, err := Read(dir)
				if err != nil {
					t.Fatal(err)
				}
				if h.Manifest.Epoch <= previousEpoch {
					t.Fatal("restart failed to advance epoch")
				}
				previousEpoch = h.Manifest.Epoch
				if h.Manifest.Events > limits.Events || h.Manifest.Bytes > limits.Bytes {
					t.Fatalf("seed=%d cap: %+v", seed, h.Manifest)
				}
				var diskBytes int64
				for _, path := range []string{"events.jsonl", "blobs"} {
					err := filepath.WalkDir(filepath.Join(dir, path), func(p string, d os.DirEntry, err error) error {
						if err != nil {
							return err
						}
						if !d.IsDir() {
							info, err := d.Info()
							if err != nil {
								return err
							}
							diskBytes += info.Size()
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				if diskBytes != h.Manifest.Bytes || uint64(len(h.Events)) != h.Manifest.Events {
					t.Fatalf("accounting: disk=%d manifest=%+v", diskBytes, h.Manifest)
				}
				if store.queuedBytes.Load() != 0 || len(store.queue) != 0 {
					t.Fatal("queue reservation leaked on exit")
				}
				pending := map[string]bool{}
				for i, e := range h.Events {
					if e.Sequence != uint64(i+1) {
						t.Fatal("sequence reused")
					}
					if e.Phase == "start" {
						pending[e.RequestID] = true
					} else {
						delete(pending, e.RequestID)
					}
				}
				if (len(pending) > 0 || h.Manifest.Dropped > 0 || epoch > 0) && h.Manifest.Complete {
					t.Fatalf("false completeness: %+v", h.Manifest)
				}
			}
		})
	}
}

// Faults use actual failing filesystem operations, without permission assumptions
// (the suite also runs as root). No production fault-injection hooks are needed.
func TestSimHistoryStorageFaults(t *testing.T) {
	for _, fault := range []string{"write", "full", "blob", "manifest-write", "manifest-rename", "marker-write", "marker-rename", "generation-sync"} {
		t.Run(fault, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "history")
			s, err := Open(dir, "synthetic", Limits{})
			if err != nil {
				t.Fatal(err)
			}
			s.Close()
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			if fault == "marker-write" || fault == "marker-rename" || fault == "generation-sync" {
				if fault == "generation-sync" {
					must(os.Remove(GenerationPath(dir)))
					must(os.Symlink("/dev/null", GenerationPath(dir)))
					if err := BeginGeneration(dir); err == nil {
						t.Fatal("expected fsync failure")
					}
				} else {
					lease, err := BeginWriter(dir)
					if err != nil {
						t.Fatal(err)
					}
					if fault == "marker-write" {
						must(os.Mkdir(filepath.Join(dir, "disabled.tmp"), 0700))
					} else {
						must(os.Mkdir(filepath.Join(dir, "recording-disabled"), 0700))
					}
					if _, err := Open(dir, "synthetic", Limits{}); err == nil {
						t.Fatal("expected marker failure")
					}
					must(lease.Close())
				}
				h, err := Read(dir)
				if err != nil {
					t.Fatal(err)
				}
				if h.Manifest.Complete {
					t.Fatal("failed generation certified after exit")
				}
				return
			}
			if fault == "full" {
				must(os.Remove(filepath.Join(dir, "manifest.json")))
				must(os.Remove(filepath.Join(dir, "events.jsonl")))
				must(os.Symlink("/dev/full", filepath.Join(dir, "events.jsonl")))
			}
			s, err = Open(dir, "synthetic", Limits{})
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "write":
				must(s.file.Close())
			case "blob":
				must(os.Remove(filepath.Join(dir, "blobs")))
			case "manifest-write":
				must(os.Mkdir(filepath.Join(dir, "manifest.tmp"), 0700))
			case "manifest-rename":
				must(os.Remove(filepath.Join(dir, "manifest.json")))
				must(os.Mkdir(filepath.Join(dir, "manifest.json"), 0700))
			}
			s.Record(Event{RequestID: "fault", Phase: "start", Verb: "get"}, nil)
			s.Record(Event{RequestID: "fault", Phase: "end", Verb: "get"}, []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"}}`))
			s.Close()
			if s.dropped.Load() == 0 {
				t.Fatal("fault not recorded")
			}
			// /dev/full is never read: restore the empty event stream after writer exit.
			if fault == "full" {
				must(os.Remove(filepath.Join(dir, "events.jsonl")))
				must(os.WriteFile(filepath.Join(dir, "events.jsonl"), nil, 0600))
			}
			h, err := Read(dir)
			if err == nil && h.Manifest.Complete && !h.Manifest.Open {
				t.Fatalf("storage fault certified: %+v", h.Manifest)
			}
		})
	}
}

func TestSimHistoryQueueBudget(t *testing.T) {
	// Hold the consumer at a deterministic cut. Exercise the same admission method
	// used by Record, then release every reservation without disk timing races.
	s := &Store{queue: make(chan observation, 2)}
	for i := 0; i < 3; i++ {
		s.enqueue(Event{Phase: "start"}, nil)
	}
	if len(s.queue) != 2 || s.dropped.Load() != 1 {
		t.Fatal("queue capacity not enforced")
	}
	for len(s.queue) > 0 {
		item := <-s.queue
		s.queuedBytes.Add(-item.weight)
	}
	s.queuedBytes.Store(MaxQueuedBytes - 1)
	s.enqueue(Event{Phase: "end"}, nil)
	if len(s.queue) != 0 || s.queuedBytes.Load() != MaxQueuedBytes-1 || s.dropped.Load() != 2 {
		t.Fatal("byte reservation not rolled back")
	}
}

// Canary leaves deliberately exclude the current dev identity/key allowlist.
// The fixture retains every supported kind and its typed list representation.
func TestSimHistoryCredentialLeaves(t *testing.T) {
	for _, seed := range historySimSeeds(t) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			kinds := []struct{ kind, version string }{{"Pod", "v1"}, {"Service", "v1"}, {"ConfigMap", "v1"}, {"Secret", "v1"}, {"Endpoints", "v1"}, {"Event", "v1"}, {"Deployment", "apps/v1"}, {"StatefulSet", "apps/v1"}, {"ReplicaSet", "apps/v1"}, {"HorizontalPodAutoscaler", "autoscaling/v1"}, {"HorizontalPodAutoscaler", "autoscaling/v2"}, {"PodDisruptionBudget", "policy/v1"}, {"EndpointSlice", "discovery.k8s.io/v1"}, {"NetworkPolicy", "networking.k8s.io/v1"}}
			for _, schema := range kinds {
				for _, list := range []bool{false, true} {
					token := fmt.Sprintf("DP1_SYNTHETIC_%x", rng.Uint64())
					values := []string{token, "https://" + token + ":password@example.invalid/health?token=" + token, base64.StdEncoding.EncodeToString([]byte(token)), "雪🔑" + token}
					hostile := func() string { return values[rng.Intn(len(values))] }
					obj := map[string]any{"kind": schema.kind, "apiVersion": schema.version, "metadata": map[string]any{"name": "object", "namespace": "app", "resourceVersion": "1", "labels": map[string]any{"safe": hostile()}, "annotations": map[string]any{"safe": hostile()}, "managedFields": []any{map[string]any{"manager": hostile()}}}, "data": map[string]any{"safe": hostile()}, "stringData": map[string]any{"safe": hostile()}, "binaryData": map[string]any{"safe": hostile()}, "spec": map[string]any{"containers": []any{map[string]any{"image": hostile(), "command": []any{hostile()}, "env": []any{map[string]any{"name": "SAFE", "value": hostile()}}, "readinessProbe": map[string]any{"httpGet": map[string]any{"path": hostile()}}}}, "selector": map[string]any{"secret": hostile()}, "template": map[string]any{"spec": map[string]any{"token": hostile()}}, "unknown": hostile()}, "status": map[string]any{"message": hostile(), "reason": hostile(), "conditions": []any{map[string]any{"reason": hostile(), "message": hostile()}}}, "message": hostile(), "reason": hostile(), "unknown": hostile()}
					if list {
						obj = map[string]any{"kind": schema.kind + "List", "apiVersion": schema.version, "metadata": map[string]any{"continue": hostile()}, "items": []any{obj}}
					}
					raw, err := json.Marshal(obj)
					if err != nil {
						t.Fatal(err)
					}
					dir := t.TempDir()
					s, err := Open(dir, "synthetic", Limits{})
					if err != nil {
						t.Fatal(err)
					}
					s.Record(Event{Phase: "end", Verb: "get"}, raw)
					s.Close()
					h, err := Read(dir)
					if err != nil {
						t.Fatal(err)
					}
					if len(h.Events) != 1 || len(h.Body(h.Events[0])) == 0 {
						t.Fatalf("fixture not projected: %s list=%v", schema.kind, list)
					}
					err = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
						if err != nil {
							return err
						}
						if d.IsDir() {
							return nil
						}
						b, err := os.ReadFile(p)
						if err != nil {
							return err
						}
						for _, canary := range []string{token, values[2]} {
							if bytes.Contains(b, []byte(canary)) {
								t.Errorf("seed=%d %s list=%v canary in %s", seed, schema.kind, list, filepath.Base(p))
							}
						}
						return nil
					})
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

// Minimized schedule: start writer, lose both envelopes, fail manifest update,
// exit writer, read. Seed 81; no projection or goroutine timing is involved.
func TestSimHistoryLostEnvelopesWithFailedManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "history")
	lease, err := BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(dir, "synthetic", Limits{})
	if err != nil {
		lease.Close()
		t.Fatal(err)
	}
	if err := store.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "manifest.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	store.Record(Event{RequestID: "lost", Phase: "start", Verb: "post"}, nil)
	store.Record(Event{RequestID: "lost", Phase: "end", Verb: "post"}, nil)
	store.Close()
	lease.Close()
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete {
		t.Fatalf("seed=81 schedule=write-error,manifest-error,exit,read: lost envelopes certified complete: %+v", h.Manifest)
	}
}

// Schedule bytes select a combined fault mask, queue/event/byte budgets, then a
// prefix cut. The writer runs normally: all fault operations precede admission.
func runHistoryFaultSchedule(t *testing.T, seed int64, schedule []byte) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	schedule = schedule[:min(len(schedule), 5)]
	choose := func(i, n int) int {
		if i < len(schedule) {
			return int(schedule[i]) % n
		}
		return rng.Intn(n)
	}
	mask := choose(0, 8)
	dir := filepath.Join(t.TempDir(), "history")
	lease, err := BeginWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	limits := Limits{Queue: 1 + choose(1, 32), Events: uint64(1 + choose(2, 64)), Bytes: int64(1+choose(3, 256)) * 256}
	s, err := Open(dir, "synthetic", limits)
	if err != nil {
		lease.Close()
		t.Fatal(err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	if mask&1 != 0 {
		must(s.file.Close())
	}
	if mask&2 != 0 {
		must(os.Mkdir(filepath.Join(dir, "manifest.tmp"), 0700))
	}
	if mask&4 != 0 {
		must(os.Remove(filepath.Join(dir, "blobs")))
	}
	cut := 1 + choose(4, 16)
	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < cut; i++ {
		id := fmt.Sprint(i)
		event := Event{RequestID: id, Phase: "start", Started: clock.Add(time.Duration(i) * time.Second), Verb: "post"}
		s.Record(event, nil)
		if i != cut-1 {
			event.Phase = "end"
			event.Finished = event.Started.Add(time.Millisecond)
			s.Record(event, []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p"}}`))
		}
	}
	s.Close()
	must(lease.Close())
	h, err := Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Every prefix has one unfinished request; even simultaneous append/manifest
	// failures must not resurrect the initial complete manifest after writer exit.
	if h.Manifest.Complete {
		t.Fatalf("seed=%d schedule=%x mask=%d cut=%d false completeness: %+v", seed, schedule, mask, cut, h.Manifest)
	}
	if h.Manifest.Bytes > limits.Bytes || h.Manifest.Events > limits.Events {
		t.Fatalf("seed=%d caps exceeded", seed)
	}
	if s.queuedBytes.Load() != 0 {
		t.Fatal("queue reservation leaked")
	}
	if mask&2 != 0 {
		must(os.Remove(filepath.Join(dir, "manifest.tmp")))
	}
	must(os.MkdirAll(filepath.Join(dir, "blobs"), 0700))
	s, err = Open(dir, "synthetic", limits)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	h, err = Read(dir)
	if err != nil {
		t.Fatal(err)
	}
	if h.Manifest.Complete || h.Manifest.Epoch != 2 {
		t.Fatalf("restart certified failed prefix: %+v", h.Manifest)
	}
	if h.Manifest.Bytes > limits.Bytes || h.Manifest.Events > limits.Events {
		t.Fatal("restart exceeded cap")
	}
}
func TestSimHistoryFaultSchedule(t *testing.T) {
	for _, seed := range historySimSeeds(t) {
		for mask := byte(0); mask < 8; mask++ {
			t.Run(fmt.Sprintf("%d/%d", seed, mask), func(t *testing.T) { runHistoryFaultSchedule(t, seed, []byte{mask, 31, 63, 255, 7}) })
		}
	}
}
func FuzzHistoryStorageSchedule(f *testing.F) {
	f.Add(int64(81), []byte{3, 31, 63, 255, 0})
	f.Add(int64(1), []byte{7, 0, 0, 0, 15})
	f.Add(int64(135), []byte{0, 31, 63, 255, 15})
	f.Fuzz(func(t *testing.T, seed int64, schedule []byte) { runHistoryFaultSchedule(t, seed, schedule) })
}
