package upstream

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// DefaultPollInterval applies to a source that does not set one.
const DefaultPollInterval = 30 * time.Second

// HoldsConfigMapName is the control-namespace ConfigMap that persists holds so
// a broken revision is not retried every poll.
const HoldsConfigMapName = "sdo-upstream-holds"

// Source is one followed upstream as read from the accepted sources.yaml.
// PollInterval comes from the decoded watcher config (gitpoll: poll_interval);
// zero means DefaultPollInterval.
type Source struct {
	Path         string
	Kind         string
	Config       *anypb.Any
	PollInterval time.Duration
}

// Fingerprint identifies the source's followed upstream (kind and config,
// not the poll interval or mount path). A hold recorded for another
// fingerprint says nothing about the current upstream and is discarded.
func (s Source) Fingerprint() string {
	h := sha256.New()
	h.Write([]byte(s.Kind))
	h.Write([]byte{0})
	if s.Config != nil {
		// Deterministic marshaling makes equal configs hash equally within
		// one binary version, which is all a persisted hold needs.
		b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(s.Config)
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// Hold records that advancing a source to Revision was refused. Holds are not
// retried; a different upstream revision supersedes them.
type Hold struct {
	Path        string
	Fingerprint string
	Revision    string
	Reason      string
	At          time.Time
}

// Request asks the core to advance one source from Prev (the newest accepted
// upstream revision) to Revision.
type Request struct {
	Source   Source
	Revision Revision
	Prev     string
}

// DecisionKind classifies what the host does after one poll observation.
type DecisionKind int

const (
	// Idle: the accepted revision is the latest. Nothing to do.
	Idle DecisionKind = iota
	// Held: the latest revision is held; nothing to do until upstream moves.
	Held
	// Advance: run Decision.Request through the core advance.
	Advance
	// Busy: an advance for this source is in flight; the newest revision is
	// re-observed when it finishes.
	Busy
	// Unavailable: the upstream was unreachable; retry at the next poll.
	Unavailable
	// Failed: the observation could not be used (Decision.Err); retry at the
	// next poll.
	Failed
)

func (k DecisionKind) String() string {
	switch k {
	case Idle:
		return "idle"
	case Held:
		return "held"
	case Advance:
		return "advance"
	case Busy:
		return "busy"
	case Unavailable:
		return "unavailable"
	case Failed:
		return "failed"
	}
	return fmt.Sprintf("DecisionKind(%d)", int(k))
}

// Decision is the follower's answer to one Observe.
type Decision struct {
	Kind    DecisionKind
	Request Request // set for Advance
	Err     error   // set for Unavailable and Failed
	// HoldsChanged reports that Holds() differs from what was last persisted.
	HoldsChanged bool
}

// OutcomeKind is how an advance ended.
type OutcomeKind int

const (
	// Accepted: the broker accepted the advance.
	Accepted OutcomeKind = iota
	// Refused: the advance was refused with a typed reason; hold the revision.
	Refused
	// Retry: the advance hit a transient failure (ErrUnavailable, busy
	// controller); try the same revision at the next poll without a hold.
	Retry
)

// Outcome is the result of an advance reported back to the follower.
type Outcome struct {
	Kind   OutcomeKind
	Reason string // required for Refused
}

// FollowerConfig configures a Follower.
type FollowerConfig struct {
	// Jitter is the fraction of the poll interval added at random to each
	// poll, in [0, 1). Zero disables jitter.
	Jitter float64
	// Rand returns a value in [0, 1); required when Jitter > 0. Injected so
	// the state machine stays deterministic.
	Rand func() float64
}

func (c FollowerConfig) validate() error {
	if c.Jitter < 0 || c.Jitter >= 1 {
		return fmt.Errorf("follower jitter %v must be in [0, 1)", c.Jitter)
	}
	if c.Jitter > 0 && c.Rand == nil {
		return errors.New("follower jitter needs a Rand source")
	}
	return nil
}

// Follower is the pure state machine behind source following: it tracks the
// followed sources, when each is next polled, which revision is in flight and
// which are held. It performs no I/O and reads no clock; the host reads the
// accepted sources and history, calls the watcher and the advance, and reports
// each result back.
//
// Bursts coalesce by construction: Observe always targets the latest
// revision, at most one advance per source is in flight, and a finished
// advance is followed by an immediate re-poll, so intermediate upstream
// commits are never deployed.
type Follower struct {
	cfg     FollowerConfig
	sources map[string]*tracked
	holds   map[string]Hold
}

type tracked struct {
	src      Source
	fp       string
	next     time.Time
	inflight *Request
}

// NewFollower returns a follower with no sources.
func NewFollower(cfg FollowerConfig) (*Follower, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Follower{cfg: cfg, sources: map[string]*tracked{}, holds: map[string]Hold{}}, nil
}

// LoadHolds installs holds decoded from the ConfigMap. Call it before the
// first Sync; Sync drops holds for sources that are gone or changed.
func (f *Follower) LoadHolds(holds []Hold) {
	for _, h := range holds {
		f.holds[h.Path] = h
	}
}

// Sync reconciles the followed set with the accepted sources.yaml. New and
// changed sources are polled immediately; removed sources and their holds are
// dropped; a source whose upstream (fingerprint) is unchanged keeps its
// schedule. It reports whether the holds changed.
func (f *Follower) Sync(now time.Time, sources []Source) (holdsChanged bool, err error) {
	seen := map[string]bool{}
	for _, s := range sources {
		if s.Path == "" || s.Kind == "" {
			return false, fmt.Errorf("source %+v needs a path and a kind", s.Path)
		}
		if s.PollInterval < 0 {
			return false, fmt.Errorf("source %q: negative poll interval %s", s.Path, s.PollInterval)
		}
		if seen[s.Path] {
			return false, fmt.Errorf("source path %q listed twice", s.Path)
		}
		seen[s.Path] = true
	}
	for path := range f.sources {
		if !seen[path] {
			delete(f.sources, path)
		}
	}
	for _, s := range sources {
		fp := s.Fingerprint()
		t, ok := f.sources[s.Path]
		if !ok || t.fp != fp {
			f.sources[s.Path] = &tracked{src: s, fp: fp, next: now}
			continue
		}
		t.src = s // the poll interval may have changed
	}
	for path, h := range f.holds {
		t, ok := f.sources[path]
		if !ok || t.fp != h.Fingerprint {
			delete(f.holds, path)
			holdsChanged = true
		}
	}
	return holdsChanged, nil
}

// Due lists the sources to poll at now, sorted by path. Sources with an
// advance in flight are never due.
func (f *Follower) Due(now time.Time) []Source {
	var due []Source
	for _, t := range f.sources {
		if t.inflight == nil && !now.Before(t.next) {
			due = append(due, t.src)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].Path < due[j].Path })
	return due
}

// NextPoll is the earliest time a source becomes due, or false when no source
// is waiting (none followed, or all in flight).
func (f *Follower) NextPoll() (time.Time, bool) {
	var first time.Time
	found := false
	for _, t := range f.sources {
		if t.inflight != nil {
			continue
		}
		if !found || t.next.Before(first) {
			first, found = t.next, true
		}
	}
	return first, found
}

// Observe records one poll of the source at path. accepted is the newest
// SDO-Upstream trailer on the accepted history (empty if none); latest and err
// are the watcher's answer. It reschedules the next poll and decides.
func (f *Follower) Observe(now time.Time, path, accepted string, latest Revision, err error) Decision {
	t, ok := f.sources[path]
	if !ok {
		return Decision{Kind: Failed, Err: fmt.Errorf("source %q is not followed", path)}
	}
	if t.inflight != nil {
		return Decision{Kind: Busy}
	}
	f.schedule(now, t)
	if err != nil {
		if errors.Is(err, ErrUnavailable) {
			return Decision{Kind: Unavailable, Err: err}
		}
		return Decision{Kind: Failed, Err: err}
	}
	if latest.ID == "" {
		return Decision{Kind: Failed, Err: fmt.Errorf("source %q: watcher returned an empty revision ID", path)}
	}
	if accepted == "" {
		return Decision{Kind: Failed, Err: fmt.Errorf("source %q: no accepted upstream revision (SDO-Upstream trailer)", path)}
	}
	h, held := f.holds[path]
	if latest.ID == accepted {
		changed := held
		delete(f.holds, path) // upstream returned to what is deployed
		return Decision{Kind: Idle, HoldsChanged: changed}
	}
	if held && h.Revision == latest.ID {
		return Decision{Kind: Held}
	}
	req := Request{Source: t.src, Revision: latest, Prev: accepted}
	t.inflight = &req
	return Decision{Kind: Advance, Request: req}
}

// Finish records the end of the advance Observe returned. It reports whether
// the holds changed. Accepted and Retry re-poll or retry as described on
// OutcomeKind; Refused holds the revision.
func (f *Follower) Finish(now time.Time, path string, out Outcome) (holdsChanged bool, err error) {
	t, ok := f.sources[path]
	if !ok {
		// The source was removed mid-advance; there is nothing to track.
		return false, nil
	}
	if t.inflight == nil {
		return false, fmt.Errorf("source %q has no advance in flight", path)
	}
	req := *t.inflight
	switch out.Kind {
	case Accepted:
		t.inflight = nil
		t.next = now // upstream may have moved while we advanced
		if _, held := f.holds[path]; held {
			delete(f.holds, path)
			holdsChanged = true
		}
	case Refused:
		if out.Reason == "" {
			return false, fmt.Errorf("source %q: a refused advance needs a reason", path)
		}
		t.inflight = nil
		f.schedule(now, t)
		f.holds[path] = Hold{Path: path, Fingerprint: t.fp, Revision: req.Revision.ID, Reason: out.Reason, At: now.UTC()}
		holdsChanged = true
	case Retry:
		t.inflight = nil
		f.schedule(now, t)
	default:
		return false, fmt.Errorf("source %q: unknown outcome kind %d", path, int(out.Kind))
	}
	return holdsChanged, nil
}

// Holds lists the current holds sorted by path, for persistence.
func (f *Follower) Holds() []Hold {
	holds := make([]Hold, 0, len(f.holds))
	for _, h := range f.holds {
		holds = append(holds, h)
	}
	sort.Slice(holds, func(i, j int) bool { return holds[i].Path < holds[j].Path })
	return holds
}

func (f *Follower) schedule(now time.Time, t *tracked) {
	interval := t.src.PollInterval
	if interval == 0 {
		interval = DefaultPollInterval
	}
	if f.cfg.Jitter > 0 {
		interval += time.Duration(float64(interval) * f.cfg.Jitter * f.cfg.Rand())
	}
	t.next = now.Add(interval)
}

// holdRecord is the persisted form of one Hold.
type holdRecord struct {
	Path        string `json:"path"`
	Fingerprint string `json:"fingerprint"`
	Revision    string `json:"revision"`
	Reason      string `json:"reason"`
	At          string `json:"at"`
}

var holdKeyPattern = regexp.MustCompile(`^hold-[0-9a-f]{16}$`)

func holdKey(path string) string {
	sum := sha256.Sum256([]byte(path))
	return "hold-" + hex.EncodeToString(sum[:8])
}

// EncodeHolds renders holds as the Data of the sdo-upstream-holds ConfigMap:
// one JSON record per source, keyed by a digest of the source path so any
// path is a valid key.
func EncodeHolds(holds []Hold) (map[string]string, error) {
	data := make(map[string]string, len(holds))
	for _, h := range holds {
		if h.Path == "" || h.Revision == "" || h.Reason == "" || h.At.IsZero() {
			return nil, fmt.Errorf("hold %+v needs path, revision, reason and time", h)
		}
		key := holdKey(h.Path)
		if _, dup := data[key]; dup {
			return nil, fmt.Errorf("two holds for source %q", h.Path)
		}
		b, err := json.Marshal(holdRecord{
			Path: h.Path, Fingerprint: h.Fingerprint, Revision: h.Revision, Reason: h.Reason,
			At: h.At.UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return nil, err
		}
		data[key] = string(b)
	}
	return data, nil
}

// DecodeHolds parses the ConfigMap Data written by EncodeHolds. It rejects
// unknown fields, malformed records and keys that do not match their path, so
// a hand-edited or foreign ConfigMap fails loudly instead of silently
// suppressing advances.
func DecodeHolds(data map[string]string) ([]Hold, error) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	holds := make([]Hold, 0, len(keys))
	for _, key := range keys {
		if !holdKeyPattern.MatchString(key) {
			return nil, fmt.Errorf("%s: unexpected key %q", HoldsConfigMapName, key)
		}
		dec := json.NewDecoder(strings.NewReader(data[key]))
		dec.DisallowUnknownFields()
		var r holdRecord
		if err := dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("%s: key %s: %w", HoldsConfigMapName, key, err)
		}
		if dec.More() || dec.InputOffset() != int64(len(bytes.TrimSpace([]byte(data[key])))) {
			return nil, fmt.Errorf("%s: key %s: trailing data", HoldsConfigMapName, key)
		}
		if r.Path == "" || r.Revision == "" || r.Reason == "" {
			return nil, fmt.Errorf("%s: key %s: path, revision and reason are required", HoldsConfigMapName, key)
		}
		if holdKey(r.Path) != key {
			return nil, fmt.Errorf("%s: key %s does not belong to path %q", HoldsConfigMapName, key, r.Path)
		}
		at, err := time.Parse(time.RFC3339Nano, r.At)
		if err != nil {
			return nil, fmt.Errorf("%s: key %s: at: %w", HoldsConfigMapName, key, err)
		}
		holds = append(holds, Hold{Path: r.Path, Fingerprint: r.Fingerprint, Revision: r.Revision, Reason: r.Reason, At: at})
	}
	return holds, nil
}
