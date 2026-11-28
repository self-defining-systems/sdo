package apihistory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"sdo.dev/operator/apihistory/internal/apiio"
	"sdo.dev/operator/sessionstorage"
)

// IncidentVerified supplements a command's normal result. Mutations also use
// incident_verified as their typed rejection reason after closure.
type IncidentVerified struct {
	VerifiedAt time.Time `json:"verified_at"`
	Message    string    `json:"message"`
}

func VerificationWarning(at time.Time) *IncidentVerified {
	return &IncidentVerified{VerifiedAt: at.UTC(), Message: fmt.Sprintf("write refused while recovered; retry if `sdo detect` shows recovered: no. Recovery judged at %s; return unless a test of this incident fails", at.UTC().Format(time.RFC3339Nano))}
}

// MutationGate coordinates the controller and the API sidecar through their
// existing private session mount. Shared leases cover the entire write; an
// exclusive observation atomically checks the journal and publishes the fence.
type MutationGate struct {
	Directory        string
	ScriptJob        string
	ScriptJobStopped func(string) bool
	// Clock stamps leases and session activity; nil reads the wall clock.
	Clock func() time.Time
}

func (g *MutationGate) now() time.Time {
	if g.Clock != nil {
		return g.Clock().UTC()
	}
	return apiio.Now().UTC()
}

type MutationLease struct {
	gate   *MutationGate
	file   *mutationLock
	name   string
	record mutationRecord
}
type MutationObservation struct {
	gate     *MutationGate
	file     *mutationLock
	Revision string
	Pending  bool
	// OpenSessions are exec and attach streams past their upgrade. They do not
	// block the fence: the stability window starts at their last activity.
	OpenSessions []JournalEntry
}

// LeaseSubject names what one lease covers: "patch deployments/web".
type LeaseSubject struct {
	Verb   string `json:"verb,omitempty"`
	Object string `json:"object,omitempty"`
}

func (s LeaseSubject) String() string { return strings.TrimSpace(s.Verb + " " + s.Object) }

type mutationRecord struct {
	ScriptJob string       `json:"script_job,omitempty"`
	Pending   bool         `json:"pending"`
	Uncertain bool         `json:"uncertain"`
	Session   bool         `json:"session,omitempty"`
	Subject   LeaseSubject `json:"subject,omitzero"`
	Started   time.Time    `json:"started,omitzero"`
	// Active is when bytes last flowed on an established session.
	Active time.Time `json:"active,omitzero"`
}

// JournalEntry is one unfinished or uncertain lease of the journal.
type JournalEntry struct {
	Subject   LeaseSubject
	Started   time.Time
	Active    time.Time
	Session   bool
	Pending   bool
	Uncertain bool
}

func PrepareMutationGate(directory string) (*MutationGate, error) {
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(directory, "mutation.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	return &MutationGate{Directory: directory}, nil
}
func (g *MutationGate) Verified() (*IncidentVerified, error) {
	b, err := os.ReadFile(filepath.Join(g.Directory, "incident-verified.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var result IncidentVerified
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}
	if result.VerifiedAt.IsZero() {
		return nil, errors.New("invalid incident verification marker")
	}
	return &result, nil
}

// A file lock alone cannot pin an inode across canonical session replacement.
// Keep the controller's storage pin until the observation or lease closes.
type mutationLock struct {
	*os.File
	releaseStorage func()
	releaseOnce    sync.Once
}

func (l *mutationLock) Close() error {
	defer l.releaseOnce.Do(l.releaseStorage)
	return l.File.Close()
}
func (g *MutationGate) lock(mode int) (*mutationLock, error) {
	releaseStorage := sessionstorage.Use()
	f, err := os.OpenFile(filepath.Join(g.Directory, "mutation.lock"), os.O_RDONLY, 0)
	if err != nil {
		releaseStorage()
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), mode); err != nil {
		f.Close()
		releaseStorage()
		return nil, err
	}
	return &mutationLock{File: f, releaseStorage: releaseStorage}, nil
}
func (g *MutationGate) Begin() (*MutationLease, error) { return g.BeginWrite(LeaseSubject{}) }

// BeginWrite starts a write lease that names its subject in the journal.
func (g *MutationGate) BeginWrite(subject LeaseSubject) (*MutationLease, error) {
	return g.begin(mutationRecord{Pending: true, ScriptJob: g.ScriptJob, Subject: subject, Started: g.now()})
}

// Fenced reports a sealed or verified incident: no new write or session may
// start. A caller that must not race the fence holds a shared lock.
func (g *MutationGate) Fenced() error {
	if _, err := os.Stat(filepath.Join(g.Directory, "incident-closed")); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		return errors.New("incident_closed")
	}
	warning, err := g.Verified()
	if err != nil {
		return err
	}
	if warning != nil {
		return errors.New("incident_verified")
	}
	return nil
}

func (g *MutationGate) begin(record mutationRecord) (*MutationLease, error) {
	f, err := g.lock(syscall.LOCK_SH)
	if err != nil {
		return nil, err
	}
	if err := g.Fenced(); err != nil {
		f.Close()
		return nil, err
	}
	name := "mutation-" + apiio.Text() + ".json"
	b, _ := json.Marshal(record)
	// The private session directory protects the journal. Its files must be
	// readable across UIDs, including on storage that squashes root.
	if err := os.WriteFile(filepath.Join(g.Directory, name), b, 0644); err != nil {
		f.Close()
		return nil, err
	}
	return &MutationLease{gate: g, file: f, name: name, record: record}, nil
}
func (w *MutationLease) Finish(uncertain bool) error {
	defer w.file.Close()
	b, _ := json.Marshal(mutationRecord{Uncertain: uncertain, ScriptJob: w.gate.ScriptJob, Session: w.record.Session, Subject: w.record.Subject, Started: w.record.Started})
	next := filepath.Join(w.gate.Directory, "."+w.name)
	if err := os.WriteFile(next, b, 0644); err != nil {
		return err
	}
	defer os.Remove(next)
	return os.Rename(next, filepath.Join(w.gate.Directory, w.name))
}
func (g *MutationGate) TryObserve() (*MutationObservation, error) {
	f, err := g.lock(syscall.LOCK_EX | syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := &MutationObservation{gate: g, file: f}
	entries, err := os.ReadDir(g.Directory)
	if err != nil {
		f.Close()
		return nil, err
	}
	hash := sha256.New()
	stopped := map[string]bool{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "mutation-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(g.Directory, entry.Name()))
		if err != nil {
			f.Close()
			return nil, err
		}
		var record mutationRecord
		if err := json.Unmarshal(b, &record); err != nil {
			f.Close()
			return nil, err
		}
		if record.Session && record.Pending && !record.Uncertain {
			// Past its upgrade (the lease's lock is released): an open stream,
			// not a write in flight. Its activity is no new write, so the
			// revision leaves it out.
			result.OpenSessions = append(result.OpenSessions, record.entry())
			record.Active = time.Time{}
			b, _ = json.Marshal(record)
		} else if record.Pending || record.Uncertain {
			quiescent, checked := stopped[record.ScriptJob]
			if !checked && record.ScriptJob != "" && g.ScriptJobStopped != nil {
				quiescent = g.ScriptJobStopped(record.ScriptJob)
				stopped[record.ScriptJob] = quiescent
			}
			result.Pending = result.Pending || !quiescent
		}
		hash.Write([]byte(entry.Name()))
		hash.Write(b)
	}
	result.Revision = fmt.Sprintf("%x", hash.Sum(nil))
	return result, nil
}
func (r mutationRecord) entry() JournalEntry {
	return JournalEntry{Subject: r.Subject, Started: r.Started, Active: r.Active, Session: r.Session, Pending: r.Pending, Uncertain: r.Uncertain}
}

// InFlight lists the journal's unfinished and uncertain leases without taking
// the lock, so a blocked observation can name what blocks it.
func (g *MutationGate) InFlight() []JournalEntry {
	entries, _ := os.ReadDir(g.Directory)
	var result []JournalEntry
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "mutation-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(g.Directory, entry.Name()))
		var record mutationRecord
		if err != nil || json.Unmarshal(b, &record) != nil || !(record.Pending || record.Uncertain) {
			continue
		}
		result = append(result, record.entry())
	}
	slices.SortFunc(result, func(a, b JournalEntry) int { return a.Started.Compare(b.Started) })
	return result
}

// LastSessionActivity is the newest activity of the open sessions and that
// session; ok is false without one.
func (o *MutationObservation) LastSessionActivity() (at time.Time, session JournalEntry, ok bool) {
	for _, entry := range o.OpenSessions {
		active := entry.Active
		if active.IsZero() {
			active = entry.Started
		}
		if !ok || active.After(at) {
			at, session, ok = active, entry, true
		}
	}
	return at, session, ok
}

// sealed reports the terminal seal.
func (g *MutationGate) sealed() bool {
	_, err := os.Stat(filepath.Join(g.Directory, "incident-closed"))
	return !errors.Is(err, os.ErrNotExist)
}

func (o *MutationObservation) Fence(at time.Time) error {
	if o.Pending {
		return errors.New("pending or uncertain mutation blocks closure")
	}
	b, err := json.Marshal(VerificationWarning(at))
	if err != nil {
		return err
	}
	if err := o.gate.ClearWithdrawn(); err != nil {
		return err
	}
	temporary := filepath.Join(o.gate.Directory, ".incident-verified-"+apiio.Text())
	if err := os.WriteFile(temporary, b, 0600); err != nil {
		return err
	}
	defer os.Remove(temporary)
	return os.Rename(temporary, filepath.Join(o.gate.Directory, "incident-verified.json"))
}

const withdrawnFile = "recovery-withdrawn"

// Withdraw records why the recovery no longer holds and the alert that says
// so, until that alert clears or the next fence or seal. It needs no lock: the
// responder is told even while a busy gate keeps the fence.
func (g *MutationGate) Withdraw(reason, alert string) error {
	b, err := json.Marshal(withdrawal{Reason: reason, Alert: alert})
	if err != nil {
		return err
	}
	temporary := filepath.Join(g.Directory, "."+withdrawnFile+"-"+apiio.Text())
	if err := os.WriteFile(temporary, b, 0600); err != nil {
		return err
	}
	defer os.Remove(temporary)
	return os.Rename(temporary, filepath.Join(g.Directory, withdrawnFile))
}

type withdrawal struct {
	Reason string `json:"reason"`
	Alert  string `json:"alert"`
}

// Withdrawn is why the last recovery was withdrawn and by which alert, or "".
func (g *MutationGate) Withdrawn() (reason, alert string) {
	b, _ := os.ReadFile(filepath.Join(g.Directory, withdrawnFile))
	var recorded withdrawal
	_ = json.Unmarshal(b, &recorded)
	return recorded.Reason, recorded.Alert
}

// ClearWithdrawn ends the notice.
func (g *MutationGate) ClearWithdrawn() error {
	err := os.Remove(filepath.Join(g.Directory, withdrawnFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Seal prevents further writes after a terminal closure without recording health proof.
func (o *MutationObservation) Seal() error {
	if err := o.gate.ClearWithdrawn(); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(o.gate.Directory, "incident-closed"), nil, 0600)
}

func (o *MutationObservation) Close() error { return o.file.Close() }

// Reopen withdraws a verification fence after the controller returned an
// unpublished closure to observation: at once for a running responder, and
// before a new dispatch once a finished one is proved quiescent.
// Pending, in-flight or uncertain writes can never be unfenced.
func (g *MutationGate) Reopen() error {
	observation, err := g.TryObserve()
	if err != nil {
		return err
	}
	if observation == nil {
		return errors.New("mutation still in flight while reopening recovery")
	}
	defer observation.Close()
	if observation.Pending {
		return errors.New("pending or uncertain mutation blocks reopening recovery")
	}
	err = os.Remove(filepath.Join(g.Directory, "incident-verified.json"))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}
