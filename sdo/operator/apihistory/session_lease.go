package apihistory

import (
	"encoding/json"
	"errors"
	"sync"
	"syscall"
	"time"

	"sdo.dev/operator/apihistory/internal/apiio"
	"sdo.dev/operator/safefs"
)

// ErrSessionFenced refuses a session's client bytes once the incident is sealed.
var ErrSessionFenced = errors.New("incident_closed: SDO closed this session at the end of the incident")

// errSessionClosed ends a held write whose stream closed meanwhile.
var errSessionClosed = errors.New("session closed")

// sessionFencePoll is how often a held or open session looks at the fence.
const sessionFencePoll = 500 * time.Millisecond

// sessionActivityStep bounds how often activity rewrites the journal: the
// controller sees a session's last bytes at most this late.
const sessionActivityStep = time.Second

// SessionLease covers one exec or attach stream. The upgrade handshake is a
// write like any other and holds the shared lock; once the stream is
// established the lock is released and the journal keeps the session open
// with the time bytes last flowed in either direction. The controller starts
// the stability window at that time, so a session in use keeps Recovered from
// holding and an idle one does not. Each client message takes the shared lock
// again. A verification fence is provisional (a withdrawn recovery reopens
// writes), so client bytes are held while it stands and forwarded once it is
// withdrawn; only the terminal seal refuses them and ends the stream.
type SessionLease struct {
	lease     *MutationLease
	mu        sync.Mutex
	handshake bool
	finished  bool
	persisted time.Time
	active    time.Time
	ended     chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
	// poll waits one fence poll; tests replace it.
	poll func() <-chan time.Time
}

// BeginSession starts a session lease; it fails like a write when fenced.
func (g *MutationGate) BeginSession(subject LeaseSubject) (*SessionLease, error) {
	lease, err := g.begin(mutationRecord{Pending: true, Session: true, ScriptJob: g.ScriptJob, Subject: subject, Started: g.now()})
	if err != nil {
		return nil, err
	}
	return &SessionLease{lease: lease, handshake: true, ended: make(chan struct{}), stop: make(chan struct{}), poll: wallPoll}, nil
}

func wallPoll() <-chan time.Time { return apiio.NewTimer(sessionFencePoll).C }

// Established ends the handshake: the stream is open and no longer holds the
// lock. It is the session's first activity.
func (s *SessionLease) Established() {
	s.mu.Lock()
	if !s.handshake {
		s.mu.Unlock()
		return
	}
	s.handshake = false
	s.persistLocked(s.lease.gate.now())
	_ = syscall.Flock(int(s.lease.file.Fd()), syscall.LOCK_UN)
	s.mu.Unlock()
	go s.watch()
}

func (s *SessionLease) watch() {
	for {
		select {
		case <-s.stop:
			return
		case <-s.poll():
			if s.lease.gate.sealed() {
				close(s.ended)
				return
			}
		}
	}
}

// Ended is closed once the incident is sealed: close the stream.
func (s *SessionLease) Ended() <-chan struct{} { return s.ended }

// Touch records bytes flowing on the stream (in either direction).
func (s *SessionLease) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handshake {
		return
	}
	now := s.lease.gate.now()
	s.active = now
	if now.Sub(s.persisted) >= sessionActivityStep {
		s.persistLocked(now)
	}
}

// persistLocked rewrites the journal record with the last activity.
func (s *SessionLease) persistLocked(at time.Time) {
	if s.finished {
		return
	}
	s.persisted, s.active = at, at
	record := s.lease.record
	record.Active = at.UTC()
	b, _ := json.Marshal(record)
	gate := s.lease.gate
	// A lost activity update only ends the stability window early; the next one retries.
	_ = safefs.WriteFile(gate.Directory, s.lease.name, b, 0644)
}

// Forward sends one client message under the shared lock. While a
// verification fence stands it waits (without the lock) for the fence to be
// withdrawn, for the seal, or for done; it refuses from the seal on.
func (s *SessionLease) Forward(done <-chan struct{}, write func() (int, error)) (int, error) {
	s.mu.Lock()
	handshake := s.handshake
	s.mu.Unlock()
	if handshake {
		if err := s.lease.gate.Fenced(); err != nil {
			return 0, err
		}
		return write()
	}
	fd := int(s.lease.file.Fd())
	for {
		// A held message waits without the lock, so it never delays the
		// controller's observation, withdrawal or seal.
		if s.lease.gate.Fenced() != nil && !s.lease.gate.sealed() {
			select {
			case <-done:
				return 0, errSessionClosed
			case <-s.stop:
				return 0, errSessionClosed
			case <-s.poll():
			}
			continue
		}
		if err := syscall.Flock(fd, syscall.LOCK_SH); err != nil {
			return 0, err
		}
		if s.lease.gate.sealed() {
			_ = syscall.Flock(fd, syscall.LOCK_UN)
			return 0, ErrSessionFenced
		}
		if s.lease.gate.Fenced() == nil {
			n, err := write()
			if n > 0 {
				s.Touch()
			}
			_ = syscall.Flock(fd, syscall.LOCK_UN)
			return n, err
		}
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		select {
		case <-done:
			return 0, errSessionClosed
		case <-s.stop:
			return 0, errSessionClosed
		case <-s.poll():
		}
	}
}

// Finish records the stream's end.
func (s *SessionLease) Finish(uncertain bool) error {
	s.stopOnce.Do(func() { close(s.stop) })
	s.mu.Lock()
	s.finished = true
	s.mu.Unlock()
	return s.lease.Finish(uncertain)
}
