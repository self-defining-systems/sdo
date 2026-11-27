package apihistory

import (
	"errors"
	"sync"
	"syscall"
	"time"

	"sdo.dev/operator/apihistory/internal/apiio"
)

// ErrSessionFenced refuses a session's client bytes once Recovered fenced the incident.
var ErrSessionFenced = errors.New("incident_verified: SDO closed this session at recovery")

// sessionFencePoll is how often an open session looks for the fence.
const sessionFencePoll = 500 * time.Millisecond

// SessionLease covers one exec or attach stream. The upgrade handshake is a
// write like any other and holds the shared lock; once the stream is
// established the lock is released and the journal keeps the session as open,
// so a stream left open never blocks observation. Each client message takes
// the shared lock again and is refused from the fence on, and Fenced tells the
// owner to close the stream.
type SessionLease struct {
	lease     *MutationLease
	mu        sync.Mutex
	handshake bool
	fenced    chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
}

// BeginSession starts a session lease; it fails like a write when fenced.
func (g *MutationGate) BeginSession(subject LeaseSubject) (*SessionLease, error) {
	lease, err := g.begin(mutationRecord{Pending: true, Session: true, ScriptJob: g.ScriptJob, Subject: subject, Started: apiio.Now().UTC()})
	if err != nil {
		return nil, err
	}
	return &SessionLease{lease: lease, handshake: true, fenced: make(chan struct{}), stop: make(chan struct{})}, nil
}

// Established ends the handshake: the stream is open and no longer holds the lock.
func (s *SessionLease) Established() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.handshake {
		return
	}
	s.handshake = false
	_ = syscall.Flock(int(s.lease.file.Fd()), syscall.LOCK_UN)
	go s.watch()
}

func (s *SessionLease) watch() {
	ticker := apiio.NewTicker(sessionFencePoll)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			if s.lease.gate.fenced() != nil {
				close(s.fenced)
				return
			}
		}
	}
}

// Fenced is closed once the incident is verified or sealed: close the stream.
func (s *SessionLease) Fenced() <-chan struct{} { return s.fenced }

// Forward sends one client message under the shared lock, refused once fenced.
func (s *SessionLease) Forward(write func() (int, error)) (int, error) {
	s.mu.Lock()
	handshake := s.handshake
	s.mu.Unlock()
	if !handshake {
		if err := syscall.Flock(int(s.lease.file.Fd()), syscall.LOCK_SH); err != nil {
			return 0, err
		}
		defer syscall.Flock(int(s.lease.file.Fd()), syscall.LOCK_UN)
	}
	if s.lease.gate.fenced() != nil {
		return 0, ErrSessionFenced
	}
	return write()
}

// Finish records the stream's end.
func (s *SessionLease) Finish(uncertain bool) error {
	s.stopOnce.Do(func() { close(s.stop) })
	return s.lease.Finish(uncertain)
}
