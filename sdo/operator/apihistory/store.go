// Package apihistory stores bounded observations outside durable operational memory.
// It has no Kubernetes transport or repository layout dependencies.
package apihistory

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sdo.dev/operator/apihistory/internal/apiio"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const MaxBodyBytes = 4 << 20
const MaxLogBytes = 64 << 10

// MaxProjectionBytes bounds parsing work; larger responses retain envelopes only.
const MaxProjectionBytes = 256 << 10
const MaxQueuedBytes = 8 << 20

// MaxAdmittedWrites bounds the write-class requests (writes, exec, attach, port-forward) one history admits. Each
// reserves its start and end envelopes from a budget separate from the blob budget, so blobs and reads can never
// crowd a write's record out. Past the bound the proxy refuses writes: it fails closed.
const MaxAdmittedWrites = 1024
const reservedEvents = 2 * MaxAdmittedWrites
const reservedEnvelopeBytes = int64(reservedEvents) * 4096

// ErrRecordingUnavailable is returned by Admit when a write's record cannot be guaranteed.
var ErrRecordingUnavailable = errors.New("recording unavailable")

var errCap = errors.New("history cap")

type Limits struct {
	Bytes  int64
	Events uint64
	Queue  int
}

func (l Limits) defaults() Limits {
	if l.Bytes <= 0 {
		l.Bytes = 64 << 20
	}
	if l.Events == 0 {
		l.Events = 10000
	}
	if l.Queue <= 0 {
		l.Queue = 1024
	}
	return l
}

type Event struct {
	Sequence        uint64    `json:"sequence"`
	Epoch           uint64    `json:"epoch"`
	RequestID       string    `json:"request_id,omitempty"`
	Phase           string    `json:"phase"`
	Started         time.Time `json:"started"`
	Finished        time.Time `json:"finished,omitempty"`
	Verb            string    `json:"verb"`
	Group           string    `json:"group,omitempty"`
	Version         string    `json:"version,omitempty"`
	Resource        string    `json:"resource,omitempty"`
	Namespace       string    `json:"namespace,omitempty"`
	Name            string    `json:"name,omitempty"`
	Subresource     string    `json:"subresource,omitempty"`
	Status          int       `json:"status,omitempty"`
	ResourceVersion string    `json:"resource_version,omitempty"`
	BodyHash        string    `json:"body_hash,omitempty"`
	BodyFormat      string    `json:"body_format,omitempty"`
	Omission        string    `json:"omission,omitempty"`
	DryRun          bool      `json:"dry_run,omitempty"`
	Partial         bool      `json:"partial,omitempty"`
	// FieldManager is the manager the proxy forced on a write.
	FieldManager string `json:"field_manager,omitempty"`
	// ChangedPaths are the field paths a write changed, keys only. PathsOmitted marks a write whose body could
	// not be read, so the paths are unknown rather than empty.
	ChangedPaths []string `json:"changed_paths,omitempty"`
	PathsOmitted bool     `json:"changed_paths_omitted,omitempty"`
	// Container, Command and Ports describe an exec, attach or port-forward session; Command is redacted.
	Container string   `json:"container,omitempty"`
	Command   []string `json:"command,omitempty"`
	Ports     []string `json:"ports,omitempty"`
	// SessionEnd is when an upgraded exec, attach or port-forward stream closed.
	SessionEnd time.Time `json:"session_end,omitempty"`
	// Reserved marks the envelope of an admitted write; it is stored from the reserved envelope budget.
	Reserved bool `json:"-"`
}

type Manifest struct {
	Generation string `json:"generation"`
	Version    int    `json:"version"`
	Incident   string `json:"incident"`
	Epoch      uint64 `json:"epoch"`
	Complete   bool   `json:"complete"`
	Open       bool   `json:"open"`
	Dropped    uint64 `json:"dropped"`
	// WriteGaps counts lost write records only: a lost start or end envelope of a write or exec session, and
	// each restart of the recorder. Lost bodies, oversized responses and interrupted streams are not write gaps.
	// A gap is a write at WriteGapAt: nothing unrecorded can follow it, so readers restart settlement there.
	WriteGaps  uint64    `json:"write_gaps,omitempty"`
	WriteGapAt time.Time `json:"write_gap_at,omitempty"`
	Events     uint64    `json:"events"`
	Bytes      int64     `json:"bytes"`
	Updated    time.Time `json:"updated"`
	// SnapshotThrough bounds a drained prefix, including asynchronous projections.
	SnapshotThrough time.Time `json:"snapshot_through,omitempty"`
}
type observation struct {
	weight int64
	event  Event
	body   []byte
}
type Store struct {
	queuedBytes atomic.Int64
	projecting  chan struct{}
	projections sync.WaitGroup
	identityKey []byte
	directory   string
	limits      Limits
	manifest    Manifest
	queue       chan observation
	dropped     atomic.Uint64
	gapMu       sync.Mutex
	closeOnce   sync.Once
	gate        sync.RWMutex
	closed      bool
	failed      atomic.Bool
	admitted    atomic.Int64
	done        chan struct{}
	file        *os.File
}

func Open(directory, incident string, limits Limits) (*Store, error) {
	if directory == "" || incident == "" {
		return nil, errors.New("history directory and incident required")
	}
	expected, err := generation(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := BeginGeneration(directory); err != nil {
			return nil, err
		}
		expected, err = generation(directory)
	}
	if err != nil {
		return nil, err
	}
	// Invalidate the previous generation before any fallible initialization.
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(directory, "disabled.tmp"), []byte("redacted: recording unavailable\n"), 0600); err != nil {
		return nil, err
	}
	if err := os.Rename(filepath.Join(directory, "disabled.tmp"), filepath.Join(directory, "recording-disabled")); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(directory, "blobs"), 0700); err != nil {
		return nil, err
	}
	identityKey, err := historyIdentityKey(directory)
	if err != nil {
		return nil, err
	}
	m := Manifest{Version: 1, Incident: incident, Complete: true}
	data, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err == nil {
		if err = json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		if m.Version != 1 || m.Incident != incident {
			return nil, errors.New("history identity mismatch")
		}
		m.Complete = false
		if err := recoverUsage(directory, &m); err != nil {
			return nil, err
		}
		// A recorder that restarts may have lost a write's record while it was gone.
		m.WriteGaps++
		m.WriteGapAt = apiio.Now().UTC()
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	m.Generation = expected
	m.Epoch++
	m.Open = true
	m.SnapshotThrough = time.Time{}
	f, err := os.OpenFile(filepath.Join(directory, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	s := &Store{identityKey: identityKey, directory: directory, limits: limits.defaults(), manifest: m, file: f, done: make(chan struct{})}
	s.dropped.Store(m.Dropped)
	// Reserved envelopes never wait on, or compete with, the ordinary queue limit.
	s.queue = make(chan observation, s.limits.Queue+reservedEvents)
	s.projecting = make(chan struct{}, 8)
	if err := s.writeManifest(); err != nil {
		f.Close()
		return nil, err
	}
	if err := os.Remove(filepath.Join(directory, "recording-disabled")); err != nil {
		f.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

// Record admits bounded projection work without synchronous parsing or disk I/O.
// Raw bytes exist only in at most eight transient workers, never in the recording
// queue. Projection precedes hashing, queueing and persistence. Callers retain
// ownership of raw; an admitted worker receives its own bounded copy.
func (s *Store) Record(event Event, raw []byte) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.closed {
		return
	}
	event = s.projectEvent(event)
	event.BodyHash = ""
	if event.BodyFormat == "log_tail" || event.Subresource == "log" {
		event.BodyFormat = ""
		event.Omission = "redacted_log_metadata_only"
		s.enqueue(event, nil)
		return
	}
	if len(raw) > MaxProjectionBytes {
		event.Omission = "redacted_projection_budget"
		event.Partial = true
		s.Gap()
		s.enqueue(event, nil)
		return
	}
	if len(raw) == 0 {
		s.enqueue(event, nil)
		return
	}
	if !event.Reserved && len(s.queue) >= s.limits.Queue {
		s.lose(event)
		return
	}
	select {
	case s.projecting <- struct{}{}:
		s.projections.Add(1)
		captured := append([]byte(nil), raw...)
		go func() {
			defer s.projections.Done()
			defer func() { <-s.projecting }()
			body, rv, err := projectResponseWithVersion(captured, s.opaqueIdentifier("metadata.resourceVersion"))
			event.ResourceVersion = rv
			if err != nil {
				event.Omission = "redacted_unsupported_projection"
				body = nil
				event.BodyFormat = ""
			} else {
				event.BodyFormat = "projected_json"
				event.Omission = "redacted_metadata_only"
			}
			s.enqueue(event, body)
		}()
	default:
		event.Omission = "redacted_projection_busy"
		s.Gap()
		s.enqueue(event, nil)
	}
}

func (s *Store) enqueue(event Event, body []byte) {
	envelope, _ := json.Marshal(event)
	weight := int64(len(body) + len(envelope) + 256)
	if event.Reserved {
		// The channel holds reservedEvents beyond the ordinary limit, and Admit bounds reserved events to that
		// count, so this send cannot block or drop; a failure here would lose a write's record.
		s.queuedBytes.Add(weight)
		select {
		case s.queue <- observation{event: event, body: body, weight: weight}:
		default:
			s.queuedBytes.Add(-weight)
			s.failed.Store(true)
			s.lose(event)
		}
		return
	}
	if s.queuedBytes.Add(weight) > MaxQueuedBytes || len(s.queue) >= s.limits.Queue {
		s.queuedBytes.Add(-weight)
		s.lose(event)
		return
	}
	select {
	case s.queue <- observation{event: event, body: body, weight: weight}:
	default:
		s.queuedBytes.Add(-weight)
		s.lose(event)
	}
}

// Admit reserves the start and end envelopes of one write-class request before the proxy forwards it. It fails
// when the store is closed or has failed, or the reserved envelope budget is spent. A request that is not
// admitted must not be forwarded: a write is never allowed without a record.
func (s *Store) Admit() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.closed || s.failed.Load() {
		return ErrRecordingUnavailable
	}
	if s.admitted.Add(1) > MaxAdmittedWrites {
		s.admitted.Add(-1)
		return fmt.Errorf("%w: reserved write envelope budget spent", ErrRecordingUnavailable)
	}
	return nil
}
func (s *Store) Gap() { s.dropped.Add(1) }

// WriteGap records that a write's record was lost, at the current time.
func (s *Store) WriteGap() {
	s.gapMu.Lock()
	defer s.gapMu.Unlock()
	s.manifest.WriteGaps++
	s.manifest.WriteGapAt = apiio.Now().UTC()
}

// lose counts an envelope that could not be stored: always a gap, and a write gap when it belonged to a write
// or an exec or attach session.
func (s *Store) lose(event Event) {
	s.Gap()
	if WriteClass(event) {
		s.WriteGap()
	}
}

// WriteClass reports whether an event belongs to a request that changes the cluster: a POST, PUT, PATCH or
// DELETE on any resource or subresource, or an exec or attach session. Reads, dry runs, port-forward tunnels
// and other connect subresources, and the review creates that persist nothing are not writes.
func WriteClass(event Event) bool {
	if event.DryRun {
		return false
	}
	switch event.Subresource {
	case "exec", "attach":
		return true
	case "portforward", "proxy":
		return false
	}
	switch event.Verb {
	case "post", "put", "patch", "delete":
	default:
		return false
	}
	return !strings.HasSuffix(event.Resource, "subjectaccessreviews") && event.Resource != "selfsubjectrulesreviews" && event.Resource != "selfsubjectreviews" && event.Resource != "tokenreviews"
}
func (s *Store) Close() {
	s.closeOnce.Do(func() {
		s.gate.Lock()
		s.closed = true
		s.gate.Unlock()
		s.projections.Wait()
		close(s.queue)
		<-s.done
	})
}
func (s *Store) writeManifest() error {
	s.gapMu.Lock()
	defer s.gapMu.Unlock()
	s.manifest.Dropped = s.dropped.Load()
	s.manifest.Complete = s.manifest.Complete && s.manifest.Dropped == 0
	s.manifest.Updated = apiio.Now().UTC()
	b, err := json.Marshal(s.manifest)
	if err != nil {
		return err
	}
	p := filepath.Join(s.directory, "manifest.tmp")
	if err = os.WriteFile(p, b, 0600); err != nil {
		return err
	}
	return os.Rename(p, filepath.Join(s.directory, "manifest.json"))
}
func (s *Store) run() {
	defer close(s.done)
	defer s.file.Close()
	tick := apiio.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case item, ok := <-s.queue:
			if !ok {
				s.manifest.Open = false
				_ = s.writeManifest()
				return
			}
			s.queuedBytes.Add(-item.weight)
			if err := s.append(item); err != nil {
				s.lose(item.event)
				s.noteAppendFailure(item, err)
			}
			if err := s.writeManifest(); err != nil {
				s.dropped.Add(1)
				s.failed.Store(true)
			}
		case <-tick.C:
			s.checkpoint()
		}
	}
}

// checkpoint publishes a prefix only after every admitted projection and queued
// observation has drained. New Record calls wait at the admission gate; the
// timestamp precedes draining, so it never certifies work that is still queued.
func (s *Store) checkpoint() {
	s.gate.Lock()
	through := apiio.Now().UTC()
	s.projections.Wait()
	// Capture admitted envelopes under the gate, then release callers before
	// doing disk I/O. New observations remain queued behind this snapshot.
	var batch []observation
drain:
	for {
		select {
		case item, ok := <-s.queue:
			if !ok {
				break drain
			}
			s.queuedBytes.Add(-item.weight)
			batch = append(batch, item)
		default:
			break drain
		}
	}
	s.gate.Unlock()
	for _, item := range batch {
		if err := s.append(item); err != nil {
			s.lose(item.event)
			s.noteAppendFailure(item, err)
		}
	}
	s.gapMu.Lock()
	s.manifest.SnapshotThrough = through
	s.gapMu.Unlock()
	if err := s.writeManifest(); err != nil {
		s.Gap()
		s.failed.Store(true)
	}
}

// noteAppendFailure fails the store closed to new writes when a record is lost for a reason other than an
// ordinary event's budget: a disk error, or any lost reserved envelope.
func (s *Store) noteAppendFailure(item observation, err error) {
	if item.event.Reserved || !errors.Is(err, errCap) {
		s.failed.Store(true)
	}
}

func (s *Store) append(item observation) error {
	eventCap, byteCap := s.limits.Events, s.limits.Bytes
	if item.event.Reserved {
		eventCap, byteCap = eventCap+reservedEvents, byteCap+reservedEnvelopeBytes
	}
	if s.manifest.Events >= eventCap {
		return fmt.Errorf("%w: events", errCap)
	}
	e := item.event
	e.Sequence = s.manifest.Events + 1
	e.Epoch = s.manifest.Epoch
	var blobPath string
	var extra int64
	if len(item.body) > 0 {
		hash := sha256.Sum256(item.body)
		e.BodyHash = hex.EncodeToString(hash[:])
		blobPath = filepath.Join(s.directory, "blobs", e.BodyHash)
		if _, err := os.Stat(blobPath); errors.Is(err, os.ErrNotExist) {
			extra = int64(len(item.body))
		} else if err != nil {
			return err
		}
		// A reserved envelope may use the envelope budget, never the blob budget: a body that does not fit
		// the ordinary byte cap is dropped and the envelope kept.
		if item.event.Reserved && extra > 0 && s.manifest.Bytes+extra+4096 > s.limits.Bytes {
			e.BodyHash, e.Omission, blobPath, extra = "", "redacted_blob_budget", "", 0
		}
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if s.manifest.Bytes+int64(len(b))+extra > byteCap {
		return fmt.Errorf("%w: bytes", errCap)
	}
	if extra > 0 {
		if err := os.WriteFile(blobPath, item.body, 0600); err != nil {
			return err
		}
		s.manifest.Bytes += extra
	}
	n, err := s.file.Write(b)
	s.manifest.Bytes += int64(n)
	if err != nil {
		return err
	}
	s.manifest.Events++
	return nil
}

type History struct {
	Manifest Manifest `json:"manifest"`
	Events   []Event  `json:"events"`
	// Live is true while a recording proxy holds the history's writer lock.
	Live      bool `json:"-"`
	directory string
}

func Read(directory string) (History, error) { return readPrefix(directory, nil) }

// ReadThrough reads one immutable manifest-bounded prefix covering until. A
// live writer is safe only with a producer checkpoint. It never fails because a response body or stream was
// lost: completeness for write readers is Manifest.WriteGaps, which includes the write records a stale
// generation, disabled recording, a short event file or an unparsable line lost. The read waits for a
// checkpoint covering until.
func ReadThrough(ctx context.Context, directory string, until time.Time) (History, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		h, err := readPrefix(directory, &until)
		if err != nil || !h.Manifest.Open || !h.Live || !h.Manifest.SnapshotThrough.Before(until) {
			return h, err
		}
		timer := apiio.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return h, fmt.Errorf("history checkpoint unavailable through closure: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func readPrefix(directory string, through *time.Time) (History, error) {
	h := History{directory: directory}
	// Directory locks remain available on read-only mounts and filesystems. A
	// live forwarding generation invalidates a previous closed manifest even if
	// the recorder cannot write a disabled marker.
	writerOpen := writerPresent(directory)
	h.Live = writerOpen
	b, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		return h, fmt.Errorf("Unknown: history manifest: %w", err)
	}
	if err = json.Unmarshal(b, &h.Manifest); err != nil {
		return h, err
	}
	if h.Manifest.Version != 1 {
		return h, errors.New("unknown history version")
	}
	// An unclosed manifest may predate failed appends and failed manifest
	// updates. Writer liveness disappears on exit, so Open must independently
	// prevent that stale manifest from certifying a fully recorded generation.
	if h.Manifest.Open && (through == nil || h.Manifest.SnapshotThrough.Before(*through)) {
		h.Manifest.Complete = false
	}
	f, err := os.Open(filepath.Join(directory, "events.jsonl"))
	if err != nil {
		return h, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 64<<10)
	parseFailed := false
	for (through == nil || uint64(len(h.Events)) < h.Manifest.Events) && scan.Scan() {
		var e Event
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
			h.Manifest.Complete = false
			parseFailed = true
			break
		}
		h.Events = append(h.Events, e)
	}
	unfinished := make(map[string]bool)
	for _, e := range h.Events {
		if e.RequestID != "" {
			if e.Phase == "start" {
				unfinished[e.RequestID] = true
			} else if e.Phase == "end" {
				delete(unfinished, e.RequestID)
			}
		}
	}
	if len(unfinished) > 0 && through == nil {
		h.Manifest.Complete = false
	}
	if err := scan.Err(); err != nil {
		return h, err
	}
	// Lost records found at read time are write gaps at the last time the manifest was written: the recorder
	// refuses a write it cannot record, so nothing unrecorded follows.
	lostRecords := parseFailed
	if uint64(len(h.Events)) != h.Manifest.Events {
		h.Manifest.Complete = false
		lostRecords = lostRecords || (through != nil && uint64(len(h.Events)) < h.Manifest.Events)
	}
	if _, err := os.Stat(filepath.Join(directory, "recording-disabled")); (writerOpen && (through == nil || (!h.Manifest.Open && h.Manifest.SnapshotThrough.Before(*through)))) || !errors.Is(err, os.ErrNotExist) {
		h.Manifest.Complete = false
		h.Manifest.Dropped++
		lostRecords = lostRecords || !errors.Is(err, os.ErrNotExist)
	}
	expected, generationErr := generation(directory)
	if generationErr != nil || expected != h.Manifest.Generation {
		h.Manifest.Complete = false
		h.Manifest.Dropped++
		lostRecords = true
	}
	// A manifest still open with no live writer is a recorder that died with records queued.
	lostRecords = lostRecords || (through != nil && h.Manifest.Open && !writerOpen)
	if lostRecords && through != nil {
		h.Manifest.WriteGaps++
		if h.Manifest.WriteGapAt.Before(h.Manifest.Updated) {
			h.Manifest.WriteGapAt = h.Manifest.Updated
		}
	}
	return h, nil
}
func (h History) Body(e Event) []byte {
	if len(e.BodyHash) != 64 {
		return nil
	}
	if _, err := hex.DecodeString(e.BodyHash); err != nil {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(h.directory, "blobs", e.BodyHash))
	if err != nil {
		return nil
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != e.BodyHash {
		return nil
	}
	return b
}

// A crash may land an append before its manifest update. Recover usage from
// append-only bytes so a new epoch cannot reset the incident's caps or sequence.
func recoverUsage(directory string, m *Manifest) error {
	file, err := os.Open(filepath.Join(directory, "events.jsonl"))
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	m.Bytes = info.Size()
	m.Events = 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 64<<10)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return fmt.Errorf("unfinished history envelope: %w", err)
		}
		m.Events++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(filepath.Join(directory, "blobs"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("nonregular history blob")
		}
		m.Bytes += info.Size()
	}
	return nil
}
