package apihistory

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"sdo.dev/operator/sessionstorage"
)

// IncidentVerified supplements a command's normal result. Mutations also use
// incident_verified as their typed rejection reason after closure.
type IncidentVerified struct {
	VerifiedAt time.Time `json:"verified_at"`
	Message    string    `json:"message"`
}

func VerificationWarning(at time.Time) *IncidentVerified {
	return &IncidentVerified{VerifiedAt: at.UTC(), Message: fmt.Sprintf("SDO verified this incident at %s; you may finish your report", at.UTC().Format(time.RFC3339Nano))}
}

// MutationGate coordinates the controller and the API sidecar through their
// existing private session mount. Shared leases cover the entire write; an
// exclusive observation atomically checks the journal and publishes the fence.
type MutationGate struct{ Directory string }
type MutationLease struct {
	gate *MutationGate
	file *mutationLock
	name string
}
type MutationObservation struct {
	gate     *MutationGate
	file     *mutationLock
	Revision string
	Pending  bool
}
type mutationRecord struct {
	Pending   bool `json:"pending"`
	Uncertain bool `json:"uncertain"`
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
}

func (l *mutationLock) Close() error {
	defer l.releaseStorage()
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
func (g *MutationGate) Begin() (*MutationLease, error) {
	f, err := g.lock(syscall.LOCK_SH)
	if err != nil {
		return nil, err
	}
	warning, err := g.Verified()
	if err != nil || warning != nil {
		f.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("incident_verified")
	}
	name := "mutation-" + rand.Text() + ".json"
	b, _ := json.Marshal(mutationRecord{Pending: true})
	if err := os.WriteFile(filepath.Join(g.Directory, name), b, 0600); err != nil {
		f.Close()
		return nil, err
	}
	return &MutationLease{gate: g, file: f, name: name}, nil
}
func (w *MutationLease) Finish(uncertain bool) error {
	defer w.file.Close()
	b, _ := json.Marshal(mutationRecord{Uncertain: uncertain})
	return os.WriteFile(filepath.Join(w.gate.Directory, w.name), b, 0600)
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
		result.Pending = result.Pending || record.Pending || record.Uncertain
		hash.Write([]byte(entry.Name()))
		hash.Write(b)
	}
	result.Revision = fmt.Sprintf("%x", hash.Sum(nil))
	return result, nil
}
func (o *MutationObservation) Fence(at time.Time) error {
	if o.Pending {
		return errors.New("pending or uncertain mutation blocks closure")
	}
	b, err := json.Marshal(VerificationWarning(at))
	if err != nil {
		return err
	}
	temporary := filepath.Join(o.gate.Directory, ".incident-verified-"+rand.Text())
	if err := os.WriteFile(temporary, b, 0600); err != nil {
		return err
	}
	defer os.Remove(temporary)
	return os.Rename(temporary, filepath.Join(o.gate.Directory, "incident-verified.json"))
}
func (o *MutationObservation) Close() error { return o.file.Close() }
