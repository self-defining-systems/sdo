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

	"sdo.dev/operator/sdk/sdktest"
)

const MaxBodyBytes = 4 << 20
const MaxLogBytes = 64 << 10

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
		l.Queue = 2
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
	Version  int       `json:"version"`
	Incident string    `json:"incident"`
	Epoch    uint64    `json:"epoch"`
	Complete bool      `json:"complete"`
	Open     bool      `json:"open"`
	Dropped  uint64    `json:"dropped"`
	Events   uint64    `json:"events"`
	Bytes    int64     `json:"bytes"`
	Updated  time.Time `json:"updated"`
}
type observation struct {
	event Event
	body  []byte
}
type Store struct {
	directory string
	limits    Limits
	manifest  Manifest
	queue     chan observation
	dropped   atomic.Uint64
	closeOnce sync.Once
	gate      sync.RWMutex
	closed    bool
	done      chan struct{}
	file      *os.File
}

func Open(directory, incident string, limits Limits) (*Store, error) {
	if directory == "" || incident == "" {
		return nil, errors.New("history directory and incident required")
	}
	if err := os.MkdirAll(filepath.Join(directory, "blobs"), 0700); err != nil {
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
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	m.Epoch++
	m.Open = true
	f, err := os.OpenFile(filepath.Join(directory, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	s := &Store{directory: directory, limits: limits.defaults(), manifest: m, file: f, done: make(chan struct{})}
	s.dropped.Store(m.Dropped)
	s.queue = make(chan observation, s.limits.Queue)
	if err := s.writeManifest(); err != nil {
		f.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

// Record never waits for disk. Projection happens before hashing or queuing;
// neither request bodies nor HTTP headers are part of this API.
func (s *Store) Record(event Event, raw []byte) {
	var body []byte
	if len(raw) > MaxBodyBytes {
		event.Omission = "body_cap"
		s.dropped.Add(1)
	} else if len(raw) > 0 {
		if event.BodyFormat == "log_tail" {
			if len(raw) > MaxLogBytes {
				raw = raw[len(raw)-MaxLogBytes:]
			}
			body = append([]byte(nil), raw...)
		} else {
			var meta struct {
				Metadata struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"metadata"`
			}
			_ = json.Unmarshal(raw, &meta)
			event.ResourceVersion = meta.Metadata.ResourceVersion
			var err error
			body, err = sdktest.ProjectCapturedObject(raw)
			if err != nil {
				event.Omission = "unsupported_projection"
				body = nil
			} else {
				event.BodyFormat = "projected_json"
			}
		}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.queue <- observation{event, body}:
	default:
		s.dropped.Add(1)
	}
}
func (s *Store) Gap() { s.dropped.Add(1) }
func (s *Store) Close() {
	s.closeOnce.Do(func() { s.gate.Lock(); s.closed = true; close(s.queue); s.gate.Unlock(); <-s.done })
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
	if err := scan.Err(); err != nil {
		return h, err
	}
	if uint64(len(h.Events)) != h.Manifest.Events {
		h.Manifest.Complete = false
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
