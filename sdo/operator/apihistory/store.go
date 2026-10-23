// Package apihistory stores bounded observations outside durable operational memory.
// It has no Kubernetes transport or repository layout dependencies.
package apihistory

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const MaxBodyBytes = 4 << 20
const MaxLogBytes = 64 << 10

// MaxProjectionBytes bounds parsing work; larger responses retain envelopes only.
const MaxProjectionBytes = 256 << 10
const MaxQueuedBytes = 8 << 20

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
}

type Manifest struct {
	Generation string    `json:"generation"`
	Version    int       `json:"version"`
	Incident   string    `json:"incident"`
	Epoch      uint64    `json:"epoch"`
	Complete   bool      `json:"complete"`
	Open       bool      `json:"open"`
	Dropped    uint64    `json:"dropped"`
	Events     uint64    `json:"events"`
	Bytes      int64     `json:"bytes"`
	Updated    time.Time `json:"updated"`
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
	closeOnce   sync.Once
	gate        sync.RWMutex
	closed      bool
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
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	m.Generation = expected
	m.Epoch++
	m.Open = true
	f, err := os.OpenFile(filepath.Join(directory, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	s := &Store{identityKey: identityKey, directory: directory, limits: limits.defaults(), manifest: m, file: f, done: make(chan struct{})}
	s.dropped.Store(m.Dropped)
	s.queue = make(chan observation, s.limits.Queue)
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

// Record admits bounded projection work without waiting for parsing or disk.
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
	if len(s.queue) == cap(s.queue) {
		s.Gap()
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
	if s.queuedBytes.Add(weight) > MaxQueuedBytes {
		s.queuedBytes.Add(-weight)
		s.Gap()
		return
	}
	select {
	case s.queue <- observation{event: event, body: body, weight: weight}:
	default:
		s.queuedBytes.Add(-weight)
		s.Gap()
	}
}
func (s *Store) Gap() { s.dropped.Add(1) }
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
	s.manifest.Dropped = s.dropped.Load()
	s.manifest.Complete = s.manifest.Complete && s.manifest.Dropped == 0
	s.manifest.Updated = time.Now().UTC()
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
	tick := time.NewTicker(100 * time.Millisecond)
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
				s.dropped.Add(1)
			}
			if err := s.writeManifest(); err != nil {
				s.dropped.Add(1)
			}
		case <-tick.C:
			if err := s.writeManifest(); err != nil {
				s.dropped.Add(1)
			}
		}
	}
}
func (s *Store) append(item observation) error {
	if s.manifest.Events >= s.limits.Events {
		return errors.New("event cap")
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
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if s.manifest.Bytes+int64(len(b))+extra > s.limits.Bytes {
		return errors.New("byte cap")
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
	Manifest  Manifest `json:"manifest"`
	Events    []Event  `json:"events"`
	directory string
}

func Read(directory string) (History, error) {
	h := History{directory: directory}
	// Directory locks remain available on read-only mounts and filesystems. A
	// live forwarding generation invalidates a previous closed manifest even if
	// the recorder cannot write a disabled marker.
	writerOpen := writerPresent(directory)
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
	f, err := os.Open(filepath.Join(directory, "events.jsonl"))
	if err != nil {
		return h, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 64<<10)
	for scan.Scan() {
		var e Event
		if err := json.Unmarshal(scan.Bytes(), &e); err != nil {
			h.Manifest.Complete = false
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
	if len(unfinished) > 0 {
		h.Manifest.Complete = false
	}
	if err := scan.Err(); err != nil {
		return h, err
	}
	if uint64(len(h.Events)) != h.Manifest.Events {
		h.Manifest.Complete = false
	}
	if _, err := os.Stat(filepath.Join(directory, "recording-disabled")); writerOpen || !errors.Is(err, os.ErrNotExist) {
		h.Manifest.Complete = false
		h.Manifest.Dropped++
	}
	expected, generationErr := generation(directory)
	if generationErr != nil || expected != h.Manifest.Generation {
		h.Manifest.Complete = false
		h.Manifest.Dropped++
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
