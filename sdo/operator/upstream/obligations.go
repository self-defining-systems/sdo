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
// failure doubles the wait up to ObligationBackoffMax, and each retry is a full
// judge run, so retries are bounded: after MaxObligationAttempts failures, or
// at once for a failure no retry can fix, the obligation becomes a typed hold
// (Held) owned by a person and is not retried again until a newer accepted
// commit owes it afresh or the controller restarts.
const (
	ObligationBackoffBase = 30 * time.Second
	ObligationBackoffMax  = 10 * time.Minute
	// MaxObligationAttempts is the failed attempts (the advance's own refresh
	// counts as the first) after which an obligation is held.
	MaxObligationAttempts = 5
	// ObligationAttemptsExhausted is the Held cause of a bounded budget spent.
	ObligationAttemptsExhausted = "attempts_exhausted"
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
	// Held is the typed cause (an operability cause, or
	// ObligationAttemptsExhausted) that stopped retrying; empty while open.
	// HeldOwner is who must act.
	Held      string
	HeldOwner string
}

// ObligationHold is a failure that no retry can fix: Cause and Owner are the
// operability vocabulary of the terminal stop that reported it.
type ObligationHold struct {
	Cause string
	Owner string
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
		if prior.Accepted != o.Accepted {
			// A newer accepted commit owes the work afresh: a fresh budget.
			prior.Attempts, prior.Held, prior.HeldOwner = 0, "", ""
		}
		prior.Accepted, prior.Upstream, prior.Reason = o.Accepted, o.Upstream, o.Reason
		prior.Attempts++
		prior.Next = now.Add(backoff(prior.Attempts))
		f.bound(&prior, terminalHold(o))
		f.obligations[o.key()] = prior
		return true, nil
	}
	o.Attempts, o.Since = 1, now.UTC()
	o.Next = now.Add(backoff(1))
	f.bound(&o, terminalHold(o))
	f.obligations[o.key()] = o
	return true, nil
}

// bound holds o when the failure is terminal or the attempt budget is spent.
func (f *Follower) bound(o *Obligation, hold *ObligationHold) {
	switch {
	case hold != nil:
		o.Held, o.HeldOwner = hold.Cause, hold.Owner
	case o.Attempts >= MaxObligationAttempts && o.Held == "":
		o.Held, o.HeldOwner = ObligationAttemptsExhausted, "designer"
	}
}

// terminalHold is the hold of a first failure that no retry can fix: the
// advance's own refresh ended on one of the operability causes that a repeat
// of the same agent task cannot change.
func terminalHold(o Obligation) *ObligationHold {
	switch o.Cause {
	case "agent_not_converging", "base_invalid", "policy":
		return &ObligationHold{Cause: o.Cause, Owner: o.Owner}
	}
	return nil
}

// DueObligations lists the open (not held) obligations whose retry time has
// come.
func (f *Follower) DueObligations(now time.Time) []Obligation {
	var due []Obligation
	for _, o := range f.obligations {
		if o.Held == "" && !now.Before(o.Next) {
			due = append(due, o)
		}
	}
	sortObligations(due)
	return due
}

// NextObligation is the earliest retry time, or false when none is open. A
// held obligation is never retried and does not wake the follower.
func (f *Follower) NextObligation() (time.Time, bool) {
	var first time.Time
	found := false
	for _, o := range f.obligations {
		if o.Held != "" {
			continue
		}
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

// Retried records another failed attempt with exponential bounded backoff. A
// non-nil hold (a failure no retry can fix) or a spent attempt budget turns the
// obligation into a typed hold instead.
func (f *Follower) Retried(now time.Time, o Obligation, reason string, hold *ObligationHold) {
	prior, open := f.obligations[o.key()]
	if !open {
		return
	}
	prior.Attempts++
	prior.Reason = reason
	prior.Next = now.Add(backoff(prior.Attempts))
	f.bound(&prior, hold)
	f.obligations[o.key()] = prior
}

// LoadObligations installs persisted obligations. They are due at now: a
// restart retries them at once (the work is idempotent, and a no-op once the
// start-up refresh made detection current). A restart is also a person's
// chance to have fixed the cause, so a held obligation is reopened with a
// fresh budget.
func (f *Follower) LoadObligations(now time.Time, obligations []Obligation) {
	for _, o := range obligations {
		o.Next = now
		if o.Held != "" {
			o.Held, o.HeldOwner, o.Attempts = "", "", 0
		}
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
	Path      string `json:"path"`
	Kind      string `json:"kind"`
	Accepted  string `json:"accepted"`
	Upstream  string `json:"upstream,omitempty"`
	Cause     string `json:"cause,omitempty"`
	Owner     string `json:"owner,omitempty"`
	Reason    string `json:"reason"`
	Attempts  int    `json:"attempts"`
	Since     string `json:"since"`
	Held      string `json:"held,omitempty"`
	HeldOwner string `json:"held_owner,omitempty"`
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
			Held: o.Held, HeldOwner: o.HeldOwner,
		})
		if err != nil {
			return nil, err
		}
		data[obligationKey(o)] = string(b)
	}
	return data, nil
}

// Quarantined is a record of the obligations ConfigMap that cannot be decoded
// (foreign key, malformed JSON, unknown field, key not matching its record). It
// is a typed hold of that record alone (cause record_invalid, owner operator):
// following continues, the record is kept untouched for a person to repair or
// delete, and it never blocks the valid ones.
type Quarantined struct {
	Key    string
	Reason string
}

// QuarantinedCause is the typed cause of a quarantined record.
const QuarantinedCause = "record_invalid"

// DecodeObligations parses EncodeObligations output. One bad record never
// hides the others: every undecodable key is returned in Quarantined with the
// reason, sorted by key.
func DecodeObligations(data map[string]string) ([]Obligation, []Quarantined) {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var list []Obligation
	var bad []Quarantined
	for _, key := range keys {
		o, err := decodeObligation(key, data[key])
		if err != nil {
			bad = append(bad, Quarantined{Key: key, Reason: err.Error()})
			continue
		}
		list = append(list, o)
	}
	return list, bad
}

func decodeObligation(key, record string) (Obligation, error) {
	if !obligationKeyPattern.MatchString(key) {
		return Obligation{}, fmt.Errorf("%s: unexpected key %q", ObligationsConfigMapName, key)
	}
	dec := json.NewDecoder(strings.NewReader(record))
	dec.DisallowUnknownFields()
	var r obligationRecord
	if err := dec.Decode(&r); err != nil {
		return Obligation{}, fmt.Errorf("%s: key %s: %w", ObligationsConfigMapName, key, err)
	}
	if r.Path == "" || r.Kind == "" || r.Accepted == "" || r.Reason == "" {
		return Obligation{}, fmt.Errorf("%s: key %s: path, kind, accepted and reason are required", ObligationsConfigMapName, key)
	}
	since, err := time.Parse(time.RFC3339Nano, r.Since)
	if err != nil {
		return Obligation{}, fmt.Errorf("%s: key %s: since: %w", ObligationsConfigMapName, key, err)
	}
	o := Obligation{Path: r.Path, Kind: ObligationKind(r.Kind), Accepted: r.Accepted, Upstream: r.Upstream,
		Cause: r.Cause, Owner: r.Owner, Reason: r.Reason, Attempts: r.Attempts, Since: since, Held: r.Held, HeldOwner: r.HeldOwner}
	if obligationKey(o) != key {
		return Obligation{}, fmt.Errorf("%s: key %s does not belong to %q/%s", ObligationsConfigMapName, key, r.Path, r.Kind)
	}
	return o, nil
}

// ObligationsSummary is the JSON list the sdo-operability ConfigMap shows: one
// record per open obligation with its cause, owner, attempts and age, in a
// stable order. No obligation is "[]".
func ObligationsSummary(obligations []Obligation) (string, error) {
	sorted := append([]Obligation(nil), obligations...)
	sortObligations(sorted)
	records := make([]obligationRecord, 0, len(sorted))
	for _, o := range sorted {
		records = append(records, obligationRecord{
			Path: o.Path, Kind: string(o.Kind), Accepted: o.Accepted, Upstream: o.Upstream,
			Cause: o.Cause, Owner: o.Owner, Reason: o.Reason, Attempts: o.Attempts, Since: o.Since.UTC().Format(time.RFC3339Nano),
		})
	}
	b, err := json.Marshal(records)
	return string(b), err
}
