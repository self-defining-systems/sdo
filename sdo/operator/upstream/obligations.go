package upstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// ObligationsConfigMapName is the control-namespace ConfigMap that persists
// open obligations.
const ObligationsConfigMapName = "sdo-upstream-obligations"

// Obligation retry bounds: the first retry waits ObligationBackoffBase, each
// failure doubles the wait up to ObligationBackoffMax. An obligation is never
// abandoned; only a successful refresh settles it.
const (
	ObligationBackoffBase = 30 * time.Second
	ObligationBackoffMax  = 10 * time.Minute
)

// ObligationKind names work an accepted advance still owes.
type ObligationKind string

// ObligationDesignerRefresh: the advance changed the desired-state shape and
// was accepted, but the detection designer has not produced accepted detection
// for it. The accepted detectors describe the previous shape.
const ObligationDesignerRefresh ObligationKind = "designer_refresh"

// Obligation is work an advance was accepted without finishing. Unlike a
// Hold it is bound to an ACCEPTED revision, so the follower's observation that
// upstream equals the accepted revision never clears it, and neither does a
// change of the followed source: only Settle does.
type Obligation struct {
	Path string
	Kind ObligationKind
	// Accepted is the accepted commit the obligation is bound to; Upstream the
	// advanced upstream revision.
	Accepted string
	Upstream string
	// Cause and Owner are the operability vocabulary (cause=coverage,
	// owner=designer); Reason is the last failure.
	Cause    string
	Owner    string
	Reason   string
	Attempts int
	Since    time.Time
	Next     time.Time
}

func (o Obligation) key() string { return o.Path + "\x00" + string(o.Kind) }

func backoff(attempts int) time.Duration {
	wait := ObligationBackoffBase
	for i := 1; i < attempts && wait < ObligationBackoffMax; i++ {
		wait *= 2
	}
	return min(wait, ObligationBackoffMax)
}

// Owe records an obligation after its first failed attempt. An obligation
// already open for the source and kind is kept (its attempts and age are not
// reset) but rebinds to the newer accepted commit. It reports whether the
// obligations changed.
func (f *Follower) Owe(now time.Time, o Obligation) (bool, error) {
	if o.Path == "" || o.Kind == "" || o.Accepted == "" || o.Reason == "" {
		return false, fmt.Errorf("obligation %+v needs a kind, an accepted commit and a reason", o)
	}
	if prior, open := f.obligations[o.key()]; open {
		prior.Accepted, prior.Upstream, prior.Reason = o.Accepted, o.Upstream, o.Reason
		prior.Attempts++
		prior.Next = now.Add(backoff(prior.Attempts))
		f.obligations[o.key()] = prior
		return true, nil
	}
	o.Attempts, o.Since = 1, now.UTC()
	o.Next = now.Add(backoff(1))
	f.obligations[o.key()] = o
	return true, nil
}

// DueObligations lists the open obligations whose retry time has come.
func (f *Follower) DueObligations(now time.Time) []Obligation {
	var due []Obligation
	for _, o := range f.obligations {
		if !now.Before(o.Next) {
			due = append(due, o)
		}
	}
	sortObligations(due)
	return due
}

// NextObligation is the earliest retry time, or false when none is open.
func (f *Follower) NextObligation() (time.Time, bool) {
	var first time.Time
	found := false
	for _, o := range f.obligations {
		if !found || o.Next.Before(first) {
			first, found = o.Next, true
		}
	}
	return first, found
}

// Settle ends an obligation after the work succeeded. It reports whether one
// was open.
func (f *Follower) Settle(o Obligation) bool {
	_, open := f.obligations[o.key()]
	delete(f.obligations, o.key())
	return open
}

// Retried records another failed attempt with exponential bounded backoff.
func (f *Follower) Retried(now time.Time, o Obligation, reason string) {
	prior, open := f.obligations[o.key()]
	if !open {
		return
	}
	prior.Attempts++
	prior.Reason = reason
	prior.Next = now.Add(backoff(prior.Attempts))
	f.obligations[o.key()] = prior
}

// LoadObligations installs persisted obligations. They are due at now: a
// restart retries them at once (the work is idempotent).
func (f *Follower) LoadObligations(now time.Time, obligations []Obligation) {
	for _, o := range obligations {
		o.Next = now
		f.obligations[o.key()] = o
	}
}

// Obligations lists the open obligations sorted by source and kind.
func (f *Follower) Obligations() []Obligation {
	list := make([]Obligation, 0, len(f.obligations))
	for _, o := range f.obligations {
		list = append(list, o)
	}
	sortObligations(list)
	return list
}

func sortObligations(list []Obligation) {
	sort.Slice(list, func(i, j int) bool { return list[i].key() < list[j].key() })
}

type obligationRecord struct {
	Path     string `json:"path"`
	Kind     string `json:"kind"`
	Accepted string `json:"accepted"`
	Upstream string `json:"upstream,omitempty"`
	Cause    string `json:"cause,omitempty"`
	Owner    string `json:"owner,omitempty"`
	Reason   string `json:"reason"`
	Attempts int    `json:"attempts"`
	Since    string `json:"since"`
}

var obligationKeyPattern = regexp.MustCompile(`^obligation-[0-9a-f]{16}$`)

func obligationKey(o Obligation) string {
	sum := sha256.Sum256([]byte(o.key()))
	return "obligation-" + hex.EncodeToString(sum[:8])
}

// EncodeObligations renders obligations as the Data of the
// sdo-upstream-obligations ConfigMap.
func EncodeObligations(obligations []Obligation) (map[string]string, error) {
	data := make(map[string]string, len(obligations))
	for _, o := range obligations {
		if o.Path == "" || o.Kind == "" || o.Accepted == "" || o.Reason == "" || o.Since.IsZero() {
			return nil, fmt.Errorf("obligation %+v needs path, kind, accepted commit, reason and time", o)
		}
		b, err := json.Marshal(obligationRecord{
			Path: o.Path, Kind: string(o.Kind), Accepted: o.Accepted, Upstream: o.Upstream,
			Cause: o.Cause, Owner: o.Owner, Reason: o.Reason, Attempts: o.Attempts, Since: o.Since.UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return nil, err
		}
		data[obligationKey(o)] = string(b)
	}
	return data, nil
}

// DecodeObligations parses EncodeObligations output, rejecting unknown fields
// and keys that do not match their record.
func DecodeObligations(data map[string]string) ([]Obligation, error) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var list []Obligation
	for _, key := range keys {
		if !obligationKeyPattern.MatchString(key) {
			return nil, fmt.Errorf("%s: unexpected key %q", ObligationsConfigMapName, key)
		}
		dec := json.NewDecoder(strings.NewReader(data[key]))
		dec.DisallowUnknownFields()
		var r obligationRecord
		if err := dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("%s: key %s: %w", ObligationsConfigMapName, key, err)
		}
		if r.Path == "" || r.Kind == "" || r.Accepted == "" || r.Reason == "" {
			return nil, fmt.Errorf("%s: key %s: path, kind, accepted and reason are required", ObligationsConfigMapName, key)
		}
		since, err := time.Parse(time.RFC3339Nano, r.Since)
		if err != nil {
			return nil, fmt.Errorf("%s: key %s: since: %w", ObligationsConfigMapName, key, err)
		}
		o := Obligation{Path: r.Path, Kind: ObligationKind(r.Kind), Accepted: r.Accepted, Upstream: r.Upstream,
			Cause: r.Cause, Owner: r.Owner, Reason: r.Reason, Attempts: r.Attempts, Since: since}
		if obligationKey(o) != key {
			return nil, fmt.Errorf("%s: key %s does not belong to %q/%s", ObligationsConfigMapName, key, r.Path, r.Kind)
		}
		list = append(list, o)
	}
	return list, nil
}
