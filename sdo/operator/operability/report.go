// Package operability defines the operability report: the typed, subject-bound
// aggregate of results that SDO's existing mechanisms already compute (patch
// application, render, deployment receipt, memory validation, evaluate-once).
// It performs no check itself. Adapters in the owning packages translate typed
// results into Checks; this package decides, routes and encodes.
//
// The stored report has no verdict. Every reader recomputes Decide over the
// checks it finds, so a stored or agent-supplied claim can never accept
// anything. The type is a leaf (standard library only) and names no benchmark,
// fault, verdict or application.
package operability

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// SchemaVersion is the only report schema. There is no compatibility reader: an
// unknown schema fails decode and the lane is reinstalled.
const SchemaVersion = 1

// MaxDetailBytes caps Check.Detail.
const MaxDetailBytes = 500

// ItemID names one operability item, O1 through O6.
type ItemID string

const (
	// O1: the deployment patches apply, render deterministically, deploy and are ready.
	O1 ItemID = "O1"
	// O2: every live object is attributable to accepted source and recorded inputs.
	O2 ItemID = "O2"
	// O3: detection coverage and bound sources answer.
	O3 ItemID = "O3"
	// O4: health detectors clear and journeys pass under synthetic traffic.
	O4 ItemID = "O4"
	// O5: operational memory is consistent.
	O5 ItemID = "O5"
	// O6: scope, ownership and budget limits hold.
	O6 ItemID = "O6"
)

// AllItems lists the items in report order.
func AllItems() []ItemID { return []ItemID{O1, O2, O3, O4, O5, O6} }

// Status is the outcome of one check. Only Fail and Pending stop acceptance.
type Status string

const (
	StatusPass    Status = "pass"
	StatusFail    Status = "fail"
	StatusNote    Status = "note"
	StatusPending Status = "pending"
	StatusSkipped Status = "skipped"
)

// Phase says where a check ran.
type Phase string

const (
	PhaseOffline Phase = "offline"
	PhaseLive    Phase = "live"
)

// Stage says which items a report must satisfy.
type Stage string

const (
	// StageDeployment gates publication of verified source: O1, O2, O6.
	StageDeployment Stage = "deployment"
	// StageComplete gates acceptance of the health commit and install: all items.
	StageComplete Stage = "complete"
	// StageAdvance gates acceptance of an upstream advance: the deployment
	// items and the independent detector verdict, O1, O2, O4, O6. The advance
	// changes no operational memory and an unchanged shape needs no new
	// detectors, so O3 and O5 are not its items; a changed shape is routed to
	// the detection designer after acceptance.
	StageAdvance Stage = "advance"
)

// StageItems returns the items a report of the stage must satisfy.
func StageItems(s Stage) []ItemID {
	switch s {
	case StageDeployment:
		return []ItemID{O1, O2, O6}
	case StageComplete:
		return AllItems()
	case StageAdvance:
		return []ItemID{O1, O2, O4, O6}
	}
	return nil
}

// Subject binds a report to the exact state it describes. A reader compares it
// with a recomputation from the repository, never with a stored claim.
type Subject struct {
	AcceptedCommit  string `json:"accepted_commit"`
	BaseTree        string `json:"base_tree"`
	EffectiveTree   string `json:"effective_tree"`
	PatchesDigest   string `json:"patches_digest"`
	RenderDigest    string `json:"render_digest"`
	EnvDigest       string `json:"env_digest"`
	DiagnosticsTree string `json:"diagnostics_tree,omitempty"`
}

// BindsTo reports the first field of the subject that differs from want. A
// subject without a commit or trees binds to nothing.
func (s Subject) BindsTo(want Subject) error {
	for _, f := range []struct{ name, have, want string }{
		{"accepted_commit", s.AcceptedCommit, want.AcceptedCommit},
		{"base_tree", s.BaseTree, want.BaseTree},
		{"effective_tree", s.EffectiveTree, want.EffectiveTree},
		{"patches_digest", s.PatchesDigest, want.PatchesDigest},
		{"render_digest", s.RenderDigest, want.RenderDigest},
		{"env_digest", s.EnvDigest, want.EnvDigest},
		{"diagnostics_tree", s.DiagnosticsTree, want.DiagnosticsTree},
	} {
		if f.have != f.want {
			return fmt.Errorf("operability subject %s %q does not match %q", f.name, f.have, f.want)
		}
	}
	return s.validate(false)
}

// Unavailable is the placeholder for a subject field a caller could not
// produce, so a failed offline report still validates and encodes. It never
// equals a recomputed value, so such a report binds to nothing.
const Unavailable = "unavailable"

// WithUnavailable returns the subject with every empty field set to Unavailable.
func (s Subject) WithUnavailable() Subject {
	for _, f := range []*string{&s.AcceptedCommit, &s.BaseTree, &s.EffectiveTree, &s.PatchesDigest, &s.RenderDigest, &s.EnvDigest, &s.DiagnosticsTree} {
		if *f == "" {
			*f = Unavailable
		}
	}
	return s
}

func (s Subject) validate(requireDiagnostics bool) error {
	for _, f := range []struct{ name, v string }{
		{"accepted_commit", s.AcceptedCommit},
		{"base_tree", s.BaseTree},
		{"effective_tree", s.EffectiveTree},
		{"patches_digest", s.PatchesDigest},
		{"render_digest", s.RenderDigest},
		{"env_digest", s.EnvDigest},
	} {
		if f.v == "" {
			return fmt.Errorf("operability subject %s is empty", f.name)
		}
	}
	if requireDiagnostics && s.DiagnosticsTree == "" {
		return fmt.Errorf("operability subject diagnostics_tree is empty")
	}
	return nil
}

// Check is one result. Adapters set Cause for Fail and Pending; routing is by
// Cause alone and no adapter names an owner.
type Check struct {
	ID     string   `json:"id"`
	Phase  Phase    `json:"phase"`
	Status Status   `json:"status"`
	Cause  Cause    `json:"cause,omitempty"`
	Detail string   `json:"detail,omitempty"`
	Paths  []string `json:"paths,omitempty"`
}

// Item groups the checks of one operability item.
type Item struct {
	ID     ItemID  `json:"id"`
	Checks []Check `json:"checks"`
}

// Report is the aggregate. Items are exactly O1 through O6 in order; O3 to O5
// may be empty only at stage deployment.
type Report struct {
	Schema  int     `json:"schema"`
	Stage   Stage   `json:"stage"`
	Subject Subject `json:"subject"`
	Items   []Item  `json:"items"`
}

// New returns a report with the six empty items in order.
func New(stage Stage, subject Subject) Report {
	items := make([]Item, 0, 6)
	for _, id := range AllItems() {
		items = append(items, Item{ID: id, Checks: []Check{}})
	}
	return Report{Schema: SchemaVersion, Stage: stage, Subject: subject, Items: items}
}

// With returns a copy with the checks appended to the item they belong to
// (the item is the prefix of the check ID before the first dot).
func (r Report) With(checks ...Check) (Report, error) {
	out := r.clone()
	for _, c := range checks {
		item, _, _ := strings.Cut(c.ID, ".")
		i := out.index(ItemID(item))
		if i < 0 {
			return Report{}, fmt.Errorf("operability check %q does not name an item", c.ID)
		}
		out.Items[i].Checks = append(out.Items[i].Checks, c)
	}
	return out, nil
}

func (r Report) index(id ItemID) int {
	for i, it := range r.Items {
		if it.ID == id {
			return i
		}
	}
	return -1
}

func (r Report) clone() Report {
	out := r
	out.Items = make([]Item, len(r.Items))
	for i, it := range r.Items {
		out.Items[i] = Item{ID: it.ID, Checks: append([]Check{}, it.Checks...)}
	}
	return out
}

// Merge combines an offline and a live report of one subject and stage. A check
// ID present in both is an error: a live result never overwrites an offline one.
func Merge(offline, live Report) (Report, error) {
	if offline.Stage != live.Stage {
		return Report{}, fmt.Errorf("operability merge: stage %q and %q differ", offline.Stage, live.Stage)
	}
	if offline.Subject != live.Subject {
		return Report{}, fmt.Errorf("operability merge: subjects differ")
	}
	out := offline.clone()
	seen := map[string]bool{}
	for _, it := range out.Items {
		for _, c := range it.Checks {
			seen[c.ID] = true
		}
	}
	for _, it := range live.Items {
		i := out.index(it.ID)
		if i < 0 {
			return Report{}, fmt.Errorf("operability merge: unknown item %q", it.ID)
		}
		for _, c := range it.Checks {
			if seen[c.ID] {
				return Report{}, fmt.Errorf("operability merge: check %q is in both reports", c.ID)
			}
			out.Items[i].Checks = append(out.Items[i].Checks, c)
		}
	}
	return out, nil
}

// Clip returns detail as one line of at most MaxDetailBytes bytes, cut on a
// rune boundary. Adapters build detail from typed errors and pass it here.
func Clip(detail string) string {
	detail = strings.Join(strings.Fields(detail), " ")
	if len(detail) <= MaxDetailBytes {
		return detail
	}
	cut := MaxDetailBytes
	for cut > 0 && !utf8.RuneStart(detail[cut]) {
		cut--
	}
	return detail[:cut]
}

// Validate checks structure: schema, item order, check shape, causes.
func (r Report) Validate() error {
	if r.Schema != SchemaVersion {
		return fmt.Errorf("operability report schema %d is not supported (want %d)", r.Schema, SchemaVersion)
	}
	if StageItems(r.Stage) == nil {
		return fmt.Errorf("operability report stage %q is unknown", r.Stage)
	}
	if err := r.Subject.validate(r.Stage == StageComplete); err != nil {
		return err
	}
	all := AllItems()
	if len(r.Items) != len(all) {
		return fmt.Errorf("operability report has %d items, want %d", len(r.Items), len(all))
	}
	ids := map[string]bool{}
	for i, it := range r.Items {
		if it.ID != all[i] {
			return fmt.Errorf("operability report item %d is %q, want %q", i, it.ID, all[i])
		}
		for _, c := range it.Checks {
			if err := c.validate(it.ID); err != nil {
				return err
			}
			if ids[c.ID] {
				return fmt.Errorf("operability check %q is duplicated", c.ID)
			}
			ids[c.ID] = true
		}
	}
	return nil
}

func (c Check) validate(item ItemID) error {
	if !strings.HasPrefix(c.ID, string(item)+".") || len(c.ID) == len(item)+1 {
		return fmt.Errorf("operability check %q must be named %s.<name>", c.ID, item)
	}
	switch c.Phase {
	case PhaseOffline, PhaseLive:
	default:
		return fmt.Errorf("operability check %q has unknown phase %q", c.ID, c.Phase)
	}
	switch c.Status {
	case StatusPass, StatusNote, StatusSkipped:
		if c.Cause != "" {
			return fmt.Errorf("operability check %q is %s and must not carry a cause", c.ID, c.Status)
		}
	case StatusFail, StatusPending:
		if !c.Cause.known() {
			return fmt.Errorf("operability check %q is %s with unknown cause %q", c.ID, c.Status, c.Cause)
		}
	default:
		return fmt.Errorf("operability check %q has unknown status %q", c.ID, c.Status)
	}
	if len(c.Detail) > MaxDetailBytes || !utf8.ValidString(c.Detail) || strings.ContainsAny(c.Detail, "\r\n") {
		return fmt.Errorf("operability check %q detail must be one valid line of at most %d bytes", c.ID, MaxDetailBytes)
	}
	return nil
}
