// Package sandbox manages the disposable Kubernetes environments an onboarding
// session tries a commit in. Each sandbox is a cluster on the host (kind) with
// SDO's Prometheus and a tracing store; one numeric cap bounds how many exist,
// and all of them are deleted when the session ends.
//
// The manager owns the bookkeeping (indexes, the cap, owners, the per-sandbox
// kubeconfig under its directory); a Provider owns the cluster effects, so the
// rules are tested with a fake provider and the kind provider is checked on a
// real cluster.
package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ClusterPrefix starts the name of every cluster the manager creates; a sweep
// at session start deletes every provider cluster with this prefix.
const ClusterPrefix = "sdo-sandbox-"

// ErrCap is returned by Add when the cap of sandboxes already exists.
var ErrCap = errors.New("sandbox cap reached")

// Spec is what a provider creates: one cluster with its kubeconfig and the
// host ports that reach its Prometheus and tracing UI.
type Spec struct {
	Name           string
	Index          int
	Kubeconfig     string
	PrometheusPort int
	TracesPort     int
	// Namespace and ControlNamespace mirror the target's: where the application
	// deploys and where SDO's own objects (Prometheus) live.
	Namespace        string
	ControlNamespace string
}

// Provider performs the cluster effects.
type Provider interface {
	// Create makes a ready cluster and writes its kubeconfig to spec.Kubeconfig.
	Create(ctx context.Context, spec Spec) error
	// Delete removes the cluster; deleting a missing cluster is not an error.
	Delete(ctx context.Context, name string) error
	// Names lists the existing clusters whose name starts with ClusterPrefix.
	Names(ctx context.Context) ([]string, error)
}

// Info describes one sandbox.
type Info struct {
	Index      int       `json:"index"`
	Name       string    `json:"name"`
	Owner      string    `json:"owner,omitempty"`
	Kubeconfig string    `json:"kubeconfig"`
	Prometheus string    `json:"prometheus"`
	Traces     string    `json:"traces"`
	Created    time.Time `json:"created"`
	Commit     string    `json:"commit,omitempty"`
	// Ready is read from the provider on List: the recorded cluster exists.
	Ready bool `json:"-"`
}

// ID is the name the commands use: sandbox-N.
func (i Info) ID() string { return "sandbox-" + strconv.Itoa(i.Index) }

// ParseID returns N of "sandbox-N".
func ParseID(id string) (int, error) {
	n, err := strconv.Atoi(strings.TrimPrefix(id, "sandbox-"))
	if err != nil || n < 1 || !strings.HasPrefix(id, "sandbox-") {
		return 0, fmt.Errorf("%q is not a sandbox name (want sandbox-N)", id)
	}
	return n, nil
}

// Manager is the sandbox bookkeeping under Dir. Several CLI processes share
// one Dir; every mutation holds the directory lock.
type Manager struct {
	// Dir holds one directory per sandbox (<Dir>/<N>/{kubeconfig,sandbox.json}).
	Dir      string
	Cap      int
	Provider Provider
	Now      func() time.Time
	// BasePort is the first host port; sandbox N uses BasePort+2N (Prometheus)
	// and BasePort+2N+1 (tracing).
	BasePort         int
	Namespace        string
	ControlNamespace string
}

func (m Manager) validate() error {
	switch {
	case m.Dir == "" || !filepath.IsAbs(m.Dir):
		return errors.New("sandbox manager needs an absolute directory")
	case m.Cap < 1:
		return fmt.Errorf("sandbox cap %d must be at least 1", m.Cap)
	case m.Provider == nil || m.Now == nil:
		return errors.New("sandbox manager needs a provider and a clock")
	case m.BasePort < 1024 || m.BasePort > 60000:
		return fmt.Errorf("sandbox base port %d is out of range", m.BasePort)
	}
	return nil
}

// Usage is the "sandboxes: used/cap" line.
func Usage(used, limit int) string { return fmt.Sprintf("sandboxes: %d/%d", used, limit) }

func (m Manager) lock() (func(), error) {
	if err := os.MkdirAll(m.Dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(m.Dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return func() { syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}

func (m Manager) read() ([]Info, error) {
	entries, err := os.ReadDir(m.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Info
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(m.Dir, e.Name(), "sandbox.json"))
		if errors.Is(err, os.ErrNotExist) {
			continue // a creation that never recorded itself; the sweep deletes its cluster
		}
		if err != nil {
			return nil, err
		}
		var info Info
		if err := json.Unmarshal(data, &info); err != nil {
			return nil, fmt.Errorf("read sandbox %s: %w", e.Name(), err)
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out, nil
}

func (m Manager) write(info Info) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(m.Dir, strconv.Itoa(info.Index), "sandbox.json"), data, 0o600)
}

func (m Manager) name(index int) string { return ClusterPrefix + strconv.Itoa(index) }

// Add creates the lowest free sandbox, or fails with ErrCap.
func (m Manager) Add(ctx context.Context, owner string) (Info, error) {
	if err := m.validate(); err != nil {
		return Info{}, err
	}
	unlock, err := m.lock()
	if err != nil {
		return Info{}, err
	}
	defer unlock()
	existing, err := m.read()
	if err != nil {
		return Info{}, err
	}
	if len(existing) >= m.Cap {
		return Info{}, fmt.Errorf("%w: %s; remove one first", ErrCap, Usage(len(existing), m.Cap))
	}
	taken := map[int]bool{}
	for _, e := range existing {
		taken[e.Index] = true
	}
	index := 1
	for taken[index] {
		index++
	}
	directory := filepath.Join(m.Dir, strconv.Itoa(index))
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return Info{}, err
	}
	spec := Spec{
		Name: m.name(index), Index: index, Kubeconfig: filepath.Join(directory, "kubeconfig"),
		PrometheusPort: m.BasePort + 2*index, TracesPort: m.BasePort + 2*index + 1,
		Namespace: m.Namespace, ControlNamespace: m.ControlNamespace,
	}
	if err := m.Provider.Create(ctx, spec); err != nil {
		// A half-made cluster is deleted, never reused.
		_ = m.Provider.Delete(context.WithoutCancel(ctx), spec.Name)
		_ = os.RemoveAll(directory)
		return Info{}, fmt.Errorf("create %s: %w", spec.Name, err)
	}
	info := Info{
		Index: index, Name: spec.Name, Owner: owner, Kubeconfig: spec.Kubeconfig, Created: m.Now().UTC(), Ready: true,
		Prometheus: "http://127.0.0.1:" + strconv.Itoa(spec.PrometheusPort), Traces: "http://127.0.0.1:" + strconv.Itoa(spec.TracesPort),
	}
	if err := m.write(info); err != nil {
		_ = m.Provider.Delete(context.WithoutCancel(ctx), spec.Name)
		_ = os.RemoveAll(directory)
		return Info{}, err
	}
	return info, nil
}

// List returns the recorded sandboxes with Ready read from the provider.
func (m Manager) List(ctx context.Context) ([]Info, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	unlock, err := m.lock()
	if err != nil {
		return nil, err
	}
	defer unlock()
	infos, err := m.read()
	if err != nil {
		return nil, err
	}
	names, err := m.Provider.Names(ctx)
	if err != nil {
		return nil, err
	}
	live := map[string]bool{}
	for _, n := range names {
		live[n] = true
	}
	for i := range infos {
		infos[i].Ready = live[infos[i].Name]
	}
	return infos, nil
}

// Get returns one sandbox.
func (m Manager) Get(ctx context.Context, index int) (Info, error) {
	infos, err := m.List(ctx)
	if err != nil {
		return Info{}, err
	}
	for _, i := range infos {
		if i.Index == index {
			return i, nil
		}
	}
	return Info{}, fmt.Errorf("sandbox-%d does not exist", index)
}

// SetCommit records the commit last deployed to a sandbox.
func (m Manager) SetCommit(index int, commit string) error {
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	infos, err := m.read()
	if err != nil {
		return err
	}
	for _, i := range infos {
		if i.Index == index {
			i.Commit = commit
			return m.write(i)
		}
	}
	return fmt.Errorf("sandbox-%d does not exist", index)
}

// Remove deletes one sandbox's cluster and its directory.
func (m Manager) Remove(ctx context.Context, index int) error {
	if err := m.validate(); err != nil {
		return err
	}
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	infos, err := m.read()
	if err != nil {
		return err
	}
	for _, i := range infos {
		if i.Index != index {
			continue
		}
		if err := m.Provider.Delete(ctx, i.Name); err != nil {
			return fmt.Errorf("delete %s: %w", i.Name, err)
		}
		return os.RemoveAll(filepath.Join(m.Dir, strconv.Itoa(index)))
	}
	return fmt.Errorf("sandbox-%d does not exist", index)
}

// DeleteAll is the session end and the host-start sweep: every recorded
// sandbox and every provider cluster with the prefix, recorded or not (a
// crashed host leaves clusters whose records never landed).
func (m Manager) DeleteAll(ctx context.Context) error {
	if err := m.validate(); err != nil {
		return err
	}
	unlock, err := m.lock()
	if err != nil {
		return err
	}
	defer unlock()
	names, err := m.Provider.Names(ctx)
	if err != nil {
		return err
	}
	var failures []error
	for _, n := range names {
		if err := m.Provider.Delete(ctx, n); err != nil {
			failures = append(failures, fmt.Errorf("delete %s: %w", n, err))
		}
	}
	if len(failures) > 0 {
		return errors.Join(failures...)
	}
	entries, _ := os.ReadDir(m.Dir)
	for _, e := range entries {
		if e.IsDir() {
			if err := os.RemoveAll(filepath.Join(m.Dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
