package runtime

import (
	"testing"
	"time"
)

func TestBatcherDeduplicatesAndOrdersSimultaneousFindings(t *testing.T) {
	batcher := NewBatcher()
	batcher.Add(stateFinding("b"))
	batcher.Add(stateFinding("a"))
	batcher.Add(stateFinding("a"))

	batch := batcher.Drain()
	if len(batch) != 2 || batch[0].Fingerprint != "a" || batch[1].Fingerprint != "b" {
		t.Fatalf("unexpected batch %#v", batch)
	}
	if second := batcher.Drain(); len(second) != 0 {
		t.Fatalf("drain retained findings: %#v", second)
	}
}

func TestBatcherDebouncesSeparateNearSimultaneousEvents(t *testing.T) {
	start := time.Unix(0, 0)
	batcher := NewDebouncedBatcher(100 * time.Millisecond)
	batcher.AddAt(stateFinding("a"), start)
	batcher.AddAt(stateFinding("b"), start.Add(50*time.Millisecond))
	if batcher.Ready(start.Add(99 * time.Millisecond)) {
		t.Fatal("batch became ready before debounce deadline")
	}
	if !batcher.Ready(start.Add(100 * time.Millisecond)) {
		t.Fatal("batch did not become ready at debounce deadline")
	}
	batch := batcher.Drain()
	if len(batch) != 2 || batch[0].Fingerprint != "a" || batch[1].Fingerprint != "b" {
		t.Fatalf("unexpected debounced batch %#v", batch)
	}
}

func TestBatcherNamespacesIdenticalFingerprintsByDetector(t *testing.T) {
	batcher := NewBatcher()
	first := stateFinding("shared")
	first.DetectorID = "first"
	second := stateFinding("shared")
	second.DetectorID = "second"
	batcher.Add(first)
	batcher.Add(second)

	batch := batcher.Drain()
	if len(batch) != 2 || batch[0].DetectorID != "first" || batch[1].DetectorID != "second" {
		t.Fatalf("identical cross-detector fingerprints collided: %#v", batch)
	}
}

func TestBatcherUsesPerDetectorDebounceAndDrainsOnlyReadyFindings(t *testing.T) {
	start := time.Unix(0, 0)
	batcher := NewDebouncedBatcher(time.Second)
	fast := stateFinding("fast")
	fast.DetectorID = "fast-detector"
	slow := stateFinding("slow")
	slow.DetectorID = "slow-detector"
	batcher.AddAtWithDebounce(fast, start, 10*time.Millisecond)
	batcher.AddAtWithDebounce(slow, start, 100*time.Millisecond)

	if !batcher.Ready(start.Add(10 * time.Millisecond)) {
		t.Fatal("fast detector finding was not ready at its own deadline")
	}
	batch := batcher.DrainReady(start.Add(10 * time.Millisecond))
	if len(batch) != 1 || batch[0].DetectorID != "fast-detector" {
		t.Fatalf("drain included non-ready findings: %#v", batch)
	}
	if deadline, ok := batcher.Deadline(); !ok || !deadline.Equal(start.Add(100*time.Millisecond)) {
		t.Fatalf("slow detector deadline was not retained: %v %v", deadline, ok)
	}
}
