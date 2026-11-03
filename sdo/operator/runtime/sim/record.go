package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
)

// This file is the recorder: one ordered event log of what the controller does.
// The two behavioral checks in properties.go are plain functions over that log,
// so a harness records during a simulated run and checks it afterwards.
//
// The recorder hooks at seams production already has, and production never
// imports it:
//
//   - Kube.Patch and Cache.Snapshot record cluster writes and observations;
//   - *Recorder implements runtime.LifecycleSink structurally (it cannot import
//     runtime), so it receives incident opens, agent dispatches and terminal
//     outcomes;
//   - Recorder.Detector wraps any sdk.Detector to record each evaluation;
//   - the remaining entries (memory activation, crashes, run bounds) are
//     recorded by explicit calls from the harness.

// Entry is one record of the log. Entries are plain values; a test may build a
// log by hand to show that a check fails.
type Entry interface{ Time() time.Time }

// Log is the ordered event log. Order is the order of recording; timestamps
// alone cannot order events that share a virtual instant.
type Log []Entry

// FindingRef names a finding by its state key and the detector that owns it.
type FindingRef struct {
	// Key is runtime.FindingStateKey(detector, fingerprint).
	Key      string
	Detector string
}

// Opened is an incident opening with the findings of its opening batch.
type Opened struct {
	At       time.Time
	Incident string
	Findings []FindingRef
}

// Outcome is an incident's terminal outcome: Closed or Failed.
type Outcome struct {
	At       time.Time
	Incident string
	Result   OutcomeResult
}

// OutcomeResult is the terminal state of an incident.
type OutcomeResult string

const (
	Closed OutcomeResult = "closed"
	Failed OutcomeResult = "failed"
)

// HandlerKind says who handled an incident.
type HandlerKind string

const (
	ByProgram HandlerKind = "program"
	ByAgent   HandlerKind = "agent"
)

// Handler records that a program (ID and revision) or an agent started
// handling an incident.
type Handler struct {
	At       time.Time
	Incident string
	Kind     HandlerKind
	ID       string
	Revision string
}

// DetectorDecl is a detector of a revision with its configured evaluation interval.
type DetectorDecl struct {
	ID       string
	Interval time.Duration
}

// Activated records that a memory revision became active: from this entry the
// controller runs exactly these detectors, each at its own interval.
type Activated struct {
	At        time.Time
	Revision  string
	Detectors []DetectorDecl
}

// Observation is one read of the cluster the controller made. ThroughSeq is the
// highest controller-write sequence number the observed view reflects; a stale
// informer cache reports an older value than the latest write.
type Observation struct {
	At         time.Time
	ID         int
	ThroughSeq uint64
}

// Evaluated is one detector evaluation attempt over an observation (ID 0 when
// unknown). Failed is set when the evaluation returned an error or panicked; a
// failed attempt still counts as the detector having been scheduled.
type Evaluated struct {
	At          time.Time
	Detector    string
	Observation int
	Failed      bool
}

// Attribution says which incident, binding and attempt a cluster write serves.
// A binding is a program or step together with the objects it is bound to;
// Action is the index of the logical action within its plan.
type Attribution struct {
	Incident string
	Binding  string
	Attempt  int
	Action   int
}

// Write is one controller write to the cluster. Seq is assigned only to a write
// the cluster committed, including one whose reply was lost to the caller.
type Write struct {
	At          time.Time
	Seq         uint64
	Attribution Attribution
	Object      sdk.ObjectRef
	// Patch is a digest of the patch type and bytes.
	Patch     string
	Committed bool
}

// ControllerDown is a controller restart or host crash beginning; ControllerUp
// is the controller serving again. Detection is not expected between them.
type ControllerDown struct {
	At   time.Time
	Host bool
}
type ControllerUp struct{ At time.Time }

// FaultsQuiesced is the instant after which the run injects no more faults;
// RunEnded is the end of the run.
type FaultsQuiesced struct{ At time.Time }
type RunEnded struct{ At time.Time }

func (e Opened) Time() time.Time         { return e.At }
func (e Outcome) Time() time.Time        { return e.At }
func (e Handler) Time() time.Time        { return e.At }
func (e Activated) Time() time.Time      { return e.At }
func (e Observation) Time() time.Time    { return e.At }
func (e Evaluated) Time() time.Time      { return e.At }
func (e Write) Time() time.Time          { return e.At }
func (e ControllerDown) Time() time.Time { return e.At }
func (e ControllerUp) Time() time.Time   { return e.At }
func (e FaultsQuiesced) Time() time.Time { return e.At }
func (e RunEnded) Time() time.Time       { return e.At }

type attributionKey struct{}

// WithAttribution tags the writes made under ctx. The harness sets it where it
// invokes the controller's write path; Kube.Patch reads it.
func WithAttribution(ctx context.Context, a Attribution) context.Context {
	return context.WithValue(ctx, attributionKey{}, a)
}

// AttributionFrom returns the attribution carried by ctx, if any.
func AttributionFrom(ctx context.Context) Attribution {
	a, _ := ctx.Value(attributionKey{}).(Attribution)
	return a
}

// Recorder appends entries in order. It is safe for concurrent use, because
// the controller calls its sinks from its own goroutines.
type Recorder struct {
	now  func() time.Time
	mu   sync.Mutex
	log  Log
	seen map[string]bool
	// failedIncidents holds incidents whose dispatch failed, until acknowledged.
	failedIncidents map[string]bool
	// closureIsTerminal makes incident_closed the terminal outcome, for a run
	// with no broker to acknowledge closures (incident_acknowledged never comes).
	closureIsTerminal bool
	observations      int
	lastObservation   int
	writes            uint64
}

// NewRecorder returns a recorder stamping entries with now (World.Now).
func NewRecorder(now func() time.Time) *Recorder {
	return &Recorder{now: now, seen: map[string]bool{}, failedIncidents: map[string]bool{}}
}

// ClosureIsTerminal records an incident's outcome at incident_closed instead of
// at incident_acknowledged. A run without a closure broker never acknowledges.
func (r *Recorder) ClosureIsTerminal() *Recorder {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closureIsTerminal = true
	return r
}

// Add appends an entry.
func (r *Recorder) Add(e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log = append(r.log, e)
}

// Log returns a copy of the log so far.
func (r *Recorder) Log() Log {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append(Log(nil), r.log...)
}

// Now is the recorder's clock.
func (r *Recorder) Now() time.Time { return r.now() }

// observe records a cluster read and returns its ID.
func (r *Recorder) observe(throughSeq uint64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations++
	r.lastObservation = r.observations
	r.log = append(r.log, Observation{At: r.now(), ID: r.observations, ThroughSeq: throughSeq})
	return r.observations
}

// LastObservation is the most recent cluster read. Detectors that follow a
// snapshot in the serial run evaluate it.
func (r *Recorder) LastObservation() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastObservation
}

// write records a patch. Only a committed write consumes a sequence number.
func (r *Recorder) write(a Attribution, ref sdk.ObjectRef, patchType string, patch []byte, committed bool) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var seq uint64
	if committed {
		r.writes++
		seq = r.writes
	}
	sum := sha256.Sum256(append([]byte(patchType+"\x00"), patch...))
	r.log = append(r.log, Write{At: r.now(), Seq: seq, Attribution: a, Object: ref, Patch: hex.EncodeToString(sum[:8]), Committed: committed})
	return seq
}

// Detector wraps d so every evaluation is recorded with the finding keys it
// reported active. The wrapper changes nothing else.
func (r *Recorder) Detector(d sdk.Detector) sdk.Detector {
	return recordingDetector{Detector: d, rec: r}
}

type recordingDetector struct {
	sdk.Detector
	rec *Recorder
}

func (d recordingDetector) Detect(ctx context.Context, view sdk.DetectionContext) (findings []sdk.Finding, err error) {
	id := d.Spec().ID
	completed := false
	defer func() {
		// A panic is recorded as a failed attempt and then continues unwinding.
		if !completed {
			d.rec.Add(Evaluated{At: d.rec.Now(), Detector: id, Observation: d.rec.LastObservation(), Failed: true})
		}
	}()
	findings, err = d.Detector.Detect(ctx, view)
	completed = true
	d.rec.Add(Evaluated{At: d.rec.Now(), Detector: id, Observation: d.rec.LastObservation(), Failed: err != nil})
	return findings, err
}

// RecordLifecycle implements runtime.LifecycleSink. Like the durable sink it is
// idempotent in event_id, so a transition replayed after a crash is one entry.
func (r *Recorder) RecordLifecycle(event *contracts.LifecycleEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id := event.GetEventId(); id != "" {
		if r.seen[id] {
			return nil
		}
		r.seen[id] = true
	}
	at, id := event.GetRecordedAt().AsTime(), event.GetIncidentId()
	switch event.GetEvent() {
	case "incident_opened":
		var findings []FindingRef
		for _, key := range event.GetFindingKeys() {
			findings = append(findings, FindingRef{Key: key, Detector: DetectorOfKey(key)})
		}
		r.log = append(r.log, Opened{At: at, Incident: id, Findings: findings})
	case "responder_dispatched":
		r.log = append(r.log, Handler{At: at, Incident: id, Kind: ByAgent, ID: "responder"})
	case "incident_closed":
		if r.closureIsTerminal {
			result := Closed
			if event.GetDispatchError() {
				result = Failed
			}
			r.log = append(r.log, Outcome{At: at, Incident: id, Result: result})
		} else if event.GetDispatchError() {
			r.failedIncidents[id] = true
		}
	case "incident_acknowledged":
		result := Closed
		if r.failedIncidents[id] {
			result = Failed
		}
		delete(r.failedIncidents, id)
		r.log = append(r.log, Outcome{At: at, Incident: id, Result: result})
	}
	return nil
}

// DetectorOfKey returns the detector id of a finding state key.
func DetectorOfKey(key string) string {
	detector, _, _ := strings.Cut(key, "\x00")
	return detector
}
