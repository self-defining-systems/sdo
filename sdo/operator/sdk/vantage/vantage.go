// Package vantage gives deterministic detectors typed, read-only probes made
// from inside an application Pod: a fresh TCP or HTTP dial to a named peer
// from the Pod's own network namespace, its traffic-control queueing
// disciplines, its processes' limits and states, its container cgroup's CPU,
// memory and IO limits and pressure, its /dev/shm usage and its socket table.
//
// The kinds are a closed set and each runs a fixed command. The runtime runs
// them in a helper container it attaches to the Pod (an ephemeral container
// of a pinned minimal tools image that shares the target container's process
// namespace and the Pod's network namespace), so a probe works whatever the
// application image holds. A detector declares probes (Consumer); the runtime
// runs them on their interval within a collection budget and freezes typed
// readings into the snapshot. No exec occurs in detectors.
package vantage

import (
	"fmt"
	"net"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
)

// Kind is one probe kind. Each has a fixed command and fixed facts.
type Kind string

const (
	// TCPConnect dials To DialsPerProbe times (DialTimeout each) from the
	// Pod's network namespace; DNS resolution counts in the latency.
	TCPConnect Kind = "tcp-connect"
	// HTTPGet sends GET To.Path DialsPerProbe times on fresh connections.
	HTTPGet Kind = "http-get"
	// Qdisc lists the queueing disciplines of the Pod's interfaces (netem,
	// tbf and other shaping show here).
	Qdisc Kind = "qdisc"
	// ProcLimits reads each process's open-files and processes limits and
	// open descriptor count.
	ProcLimits Kind = "proc-limits"
	// ProcState counts the target container's processes by scheduler state
	// (running, sleeping, stopped, zombie, uninterruptible).
	ProcState Kind = "proc-state"
	// CgroupCPU reads the container cgroup's cpu.max, cpu.stat throttling and
	// cpu.pressure.
	CgroupCPU Kind = "cgroup-cpu"
	// CgroupMemory reads memory.max, memory.current, memory.events and
	// memory.pressure.
	CgroupMemory Kind = "cgroup-memory"
	// CgroupIO reads io.max and io.pressure.
	CgroupIO Kind = "cgroup-io"
	// ShmUsage reads the size and use of the Pod's /dev/shm.
	ShmUsage Kind = "shm-usage"
	// SocketSummary counts the Pod's TCP sockets by state, overall and per
	// remote peer.
	SocketSummary Kind = "socket-summary"
)

// Kinds lists every probe kind.
var Kinds = []Kind{TCPConnect, HTTPGet, Qdisc, ProcLimits, ProcState, CgroupCPU, CgroupMemory, CgroupIO, ShmUsage, SocketSummary}

// Dials reports whether the kind dials a peer.
func (k Kind) Dials() bool { return k == TCPConnect || k == HTTPGet }

// Limits bound declarations, fan-out, retained output and the collection
// cycle, like podread: a probe can neither load the application nor stall
// the controller's evaluation loop.
const (
	MaxProbes        = 16
	MaxTargets       = podread.MaxTargets
	MaxIDBytes       = 128
	LimitBytes       = 16 * 1024
	MaxConcurrent    = 4
	MinInterval      = 30 * time.Second
	ExecTimeout      = 6 * time.Second
	CollectionBudget = 8 * time.Second
	// DialsPerProbe dials are made per probe run, each within DialTimeout.
	DialsPerProbe = 4
	DialTimeout   = time.Second
	// MaxDialHistory dials are kept per Pod across runs, newest last, so a
	// loss judgement sees a window of traffic.LinkLossWindow dials.
	MaxDialHistory = 20
	// MaxProcesses bounds the processes one process probe reads.
	MaxProcesses = 64
	// MaxPeers bounds the peers a socket summary reports.
	MaxPeers = 32
)

// Peer is a dial target: a Service name or other DNS name, or an IP address,
// and a port. Path is the request path of an HTTPGet.
type Peer struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Path string `json:"path,omitempty"`
}

func (p Peer) String() string { return net.JoinHostPort(p.Host, strconv.Itoa(p.Port)) + p.Path }

var pathPattern = regexp.MustCompile(`^/[A-Za-z0-9/._~%!$&'()*+,;=:@?-]*$`)

// Probe declares one vantage read and the role of the Pods it runs in.
type Probe struct {
	ID       string         `json:"id"`
	Kind     Kind           `json:"kind"`
	From     podread.Target `json:"from"`
	To       *Peer          `json:"to,omitempty"`
	Interval time.Duration  `json:"interval"`
}

// Validate checks the probe's identifier, kind, role, peer and interval.
func (p Probe) Validate() error {
	if p.ID == "" || len(p.ID) > MaxIDBytes || strings.TrimSpace(p.ID) != p.ID {
		return fmt.Errorf("invalid vantage probe id %q", p.ID)
	}
	if !slices.Contains(Kinds, p.Kind) {
		return fmt.Errorf("vantage probe %q: unknown kind %q", p.ID, p.Kind)
	}
	if err := p.From.Validate(); err != nil {
		return fmt.Errorf("vantage probe %q: %w", p.ID, err)
	}
	if p.Interval < MinInterval {
		return fmt.Errorf("vantage probe %q interval %s is below the minimum %s", p.ID, p.Interval, MinInterval)
	}
	if !p.Kind.Dials() {
		if p.To != nil {
			return fmt.Errorf("vantage probe %q: kind %s takes no peer", p.ID, p.Kind)
		}
		return nil
	}
	if p.To == nil {
		return fmt.Errorf("vantage probe %q: kind %s requires a peer", p.ID, p.Kind)
	}
	host := p.To.Host
	if net.ParseIP(host) == nil && len(validation.IsDNS1123Subdomain(host)) != 0 {
		return fmt.Errorf("vantage probe %q: peer host %q is not a DNS name or IP address", p.ID, host)
	}
	if p.To.Port < 1 || p.To.Port > 65535 {
		return fmt.Errorf("vantage probe %q: peer port %d is out of range", p.ID, p.To.Port)
	}
	switch {
	case p.Kind == HTTPGet && p.To.Path == "":
		return fmt.Errorf("vantage probe %q: an http-get needs a request path", p.ID)
	case p.Kind == HTTPGet && (!pathPattern.MatchString(p.To.Path) || len(p.To.Path) > 256):
		return fmt.Errorf("vantage probe %q: request path %q is not a plain absolute path", p.ID, p.To.Path)
	case p.Kind == TCPConnect && p.To.Path != "":
		return fmt.Errorf("vantage probe %q: a tcp-connect takes no path", p.ID)
	}
	return nil
}

// Consumer declares a detector's vantage probes. It must watch v1 Pod.
type Consumer interface{ VantageProbes() []Probe }

// ValidateProbes rejects malformed, duplicate or excessive declarations.
func ValidateProbes(probes []Probe) error {
	if len(probes) > MaxProbes {
		return fmt.Errorf("vantage probes exceed %d", MaxProbes)
	}
	seen := map[string]bool{}
	for _, probe := range probes {
		if err := probe.Validate(); err != nil {
			return err
		}
		if seen[probe.ID] {
			return fmt.Errorf("duplicate vantage probe %q", probe.ID)
		}
		seen[probe.ID] = true
	}
	return nil
}

// ValidateDetector validates a consumer at registration. Targets come from the
// snapshot's Pods, which only a Pod watch keeps observed.
func ValidateDetector(d sdk.Detector) error {
	c, ok := d.(Consumer)
	if !ok || len(c.VantageProbes()) == 0 {
		return nil
	}
	if err := ValidateProbes(c.VantageProbes()); err != nil {
		return err
	}
	for _, watch := range d.Spec().Watches {
		if watch.APIVersion == "v1" && watch.Kind == "Pod" {
			return nil
		}
	}
	return fmt.Errorf("vantage probes require a v1 Pod watch")
}

// Probes merges identical probe IDs across detectors and rejects conflicts.
func Probes(detectors []sdk.Detector) ([]Probe, error) {
	byID := map[string]Probe{}
	for _, d := range detectors {
		if err := ValidateDetector(d); err != nil {
			return nil, err
		}
		if c, ok := d.(Consumer); ok {
			for _, probe := range c.VantageProbes() {
				if old, ok := byID[probe.ID]; ok && !reflect.DeepEqual(old, probe) {
					return nil, fmt.Errorf("conflicting vantage probe %q", probe.ID)
				}
				byID[probe.ID] = probe
			}
		}
	}
	probes := make([]Probe, 0, len(byID))
	for _, probe := range byID {
		probes = append(probes, probe)
	}
	sort.Slice(probes, func(i, j int) bool { return probes[i].ID < probes[j].ID })
	return probes, ValidateProbes(probes)
}

// LongestInterval is how stale a declared probe can be at evaluation.
func LongestInterval(d sdk.Detector) time.Duration {
	var longest time.Duration
	if c, ok := d.(Consumer); ok {
		for _, probe := range c.VantageProbes() {
			longest = max(longest, probe.Interval)
		}
	}
	return longest
}

// Dial is one fresh connection of a TCPConnect or HTTPGet probe.
type Dial struct {
	At       time.Time     `json:"at"`
	Duration time.Duration `json:"duration_ns"`
	OK       bool          `json:"ok"`
	// Class is the failure class (traffic.LinkFailureClass values).
	Class  string `json:"class,omitempty"`
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Reading is one Pod's answer to a probe. Error means the probe gave no
// answer (the helper could not start, the command timed out, the target's
// files were unreadable); it is never a health verdict. For a dialing kind,
// OK reports that every dial of this run succeeded, Latency is their median,
// and Dials keeps up to MaxDialHistory recent dials of this Pod, newest last.
// Facts are the kind's parsed values (see the Fact constants).
type Reading struct {
	Namespace  string            `json:"namespace"`
	Pod        string            `json:"pod"`
	UID        string            `json:"uid,omitempty"`
	Container  string            `json:"container"`
	ObservedAt time.Time         `json:"observed_at"`
	Kind       Kind              `json:"kind"`
	OK         bool              `json:"ok"`
	Latency    time.Duration     `json:"latency_ns,omitempty"`
	Dials      []Dial            `json:"dials,omitempty"`
	Facts      map[string]string `json:"facts,omitempty"`
	Error      string            `json:"error,omitempty"`
}

// Ref names the probed Pod, for a finding's primary resource.
func (r Reading) Ref() sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: r.Namespace, Name: r.Pod}
}

// Fact returns a fact and whether the probe reported it.
func (r Reading) Fact(key string) (string, bool) {
	value, ok := r.Facts[key]
	return value, ok
}

// Int returns an integer fact.
func (r Reading) Int(key string) (int64, bool) {
	value, ok := r.Facts[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseInt(value, 10, 64)
	return n, err == nil
}

// Float returns a decimal fact.
func (r Reading) Float(key string) (float64, bool) {
	value, ok := r.Facts[key]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseFloat(value, 64)
	return n, err == nil
}

// Result is a probe's frozen readings. Baseline holds the complete readings
// of the same probe at the controller's last promoted healthy baseline; it is
// empty until the application has been healthy once.
type Result struct {
	Probe    Probe     `json:"probe"`
	Readings []Reading `json:"readings,omitempty"`
	Baseline []Reading `json:"baseline,omitempty"`
	Error    string    `json:"error,omitempty"`
}

// Source supplies frozen probe results.
type Source interface {
	VantageResult(string) (Result, bool)
}

// ResultSet lists every carried result, for snapshot capture.
type ResultSet interface {
	VantageResults() map[string]Result
}

// ResultFrom reads a frozen result without I/O, failed readings included.
func ResultFrom(ctx sdk.DetectionContext, id string) (Result, bool) {
	if s, ok := ctx.(Source); ok {
		return s.VantageResult(id)
	}
	return Result{}, false
}

// Require returns a result whose every reading answered, or a detector
// error. A missing, failed or empty probe cannot establish a clear.
func Require(ctx sdk.DetectionContext, id string) (Result, error) {
	r, ok := ResultFrom(ctx, id)
	if !ok {
		return r, fmt.Errorf("vantage probe %q unavailable", id)
	}
	if r.Error != "" {
		return r, fmt.Errorf("vantage probe %q: %s", id, r.Error)
	}
	if len(r.Readings) == 0 {
		return r, fmt.Errorf("vantage probe %q has no readings", id)
	}
	for _, reading := range r.Readings {
		if reading.Error != "" {
			return r, fmt.Errorf("vantage probe %q Pod %s: %s", id, reading.Pod, reading.Error)
		}
	}
	return r, nil
}

// BaselineFor returns the baseline reading of the same Pod, or else the first
// baseline reading, since a replaced Pod of the same role has the same
// healthy reference.
func (r Result) BaselineFor(reading Reading) (Reading, bool) {
	for _, base := range r.Baseline {
		if base.UID != "" && base.UID == reading.UID || base.Pod == reading.Pod {
			return base, true
		}
	}
	if len(r.Baseline) > 0 {
		return r.Baseline[0], true
	}
	return Reading{}, false
}

// WithHistory returns current with the dials of previous (an earlier reading
// of the same Pod) before its own, keeping the newest MaxDialHistory.
func WithHistory(previous, current Reading) Reading {
	if previous.UID != current.UID || previous.Pod != current.Pod || !current.Kind.Dials() {
		return current
	}
	dials := append(slices.Clone(previous.Dials), current.Dials...)
	if len(dials) > MaxDialHistory {
		dials = dials[len(dials)-MaxDialHistory:]
	}
	current.Dials = dials
	return current
}
