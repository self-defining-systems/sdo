package rehearsal

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Report is one rehearsal: every release of a chart replayed through SDO's
// upstream-follow path, with every blocker of every release (a rehearsal
// continues past a failing release, as a live follower would poll the next
// one).
type Report struct {
	Name     string    `json:"name"`
	Releases []Release `json:"releases"`
	// Seconds is the rehearsal's host wall time.
	Seconds float64 `json:"seconds"`
}

// Release is what SDO did with one release. Index 0 is the baseline
// (onboarding); later ones are upstream advances.
type Release struct {
	Index   int    `json:"index"`
	Name    string `json:"release"`
	Adopted bool   `json:"adopted"`
	// Outcome is the advance outcome (accepted, held, deferred) or
	// "onboarded"/"onboarding_failed" for the baseline; "error" when the
	// path returned an error instead of a typed result.
	Outcome string   `json:"outcome"`
	Hold    string   `json:"hold,omitempty"`
	Cause   string   `json:"cause,omitempty"`
	Owner   string   `json:"owner,omitempty"`
	Items   []string `json:"items,omitempty"`
	Detail  string   `json:"detail,omitempty"`
	Error   string   `json:"error,omitempty"`
	// Dispatches are the agent tasks SDO started for this release.
	Dispatches []Dispatch `json:"dispatches,omitempty"`
	// Incidents are what would open an incident after the release: a hold the
	// responder owns, and every drift.unexplained finding.
	Incidents []string `json:"incidents,omitempty"`
	// Recreated are the objects the deployer deleted and recreated.
	Recreated []string `json:"recreated,omitempty"`
	// Leftover are live objects of earlier releases that the accepted source
	// no longer declares (nothing pruned them).
	Leftover []string `json:"leftover,omitempty"`
	// Drift are the accepted-source inventory findings on the live namespace
	// after the release (the controller's drift.unexplained).
	Drift []Finding `json:"drift,omitempty"`
	// Generations lists every workload whose metadata.generation moved.
	Generations []GenerationChange `json:"generations,omitempty"`
	// Operability is the accepted report's check (absent when nothing was
	// accepted).
	Operability *OperabilityCheck `json:"operability,omitempty"`
	// KubeletErrors are API errors the fake kubelet hit (a status the real API
	// server refused is a harness gap, never ignored).
	KubeletErrors []string `json:"kubelet_errors,omitempty"`
	// InjectedSeconds is how far the injected clock moved during the release.
	InjectedSeconds float64 `json:"injected_seconds"`
	Seconds         float64 `json:"seconds"`
}

// Dispatch is one agent task SDO started.
type Dispatch struct {
	Role   string `json:"role"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// Finding is one drift finding.
type Finding struct {
	Detector string `json:"detector"`
	Rule     string `json:"rule,omitempty"`
	Resource string `json:"resource"`
	Summary  string `json:"summary,omitempty"`
}

// GenerationChange is a workload's generation before and after a release,
// and whether the release changed what the chart renders for it.
type GenerationChange struct {
	Workload       string `json:"workload"`
	Before         int64  `json:"before"`
	After          int64  `json:"after"`
	RenderedChange bool   `json:"rendered_change"`
	// Changed are the live spec paths that differ (at most a few), so churn
	// names what moved.
	Changed []string `json:"changed,omitempty"`
}

// SpecDiff lists the paths whose values differ between two decoded JSON
// documents, sorted, at most limit of them.
func SpecDiff(before, after any, limit int) []string {
	var out []string
	var walk func(path string, a, b any)
	walk = func(path string, a, b any) {
		am, aok := a.(map[string]any)
		bm, bok := b.(map[string]any)
		if aok && bok {
			keys := map[string]bool{}
			for k := range am {
				keys[k] = true
			}
			for k := range bm {
				keys[k] = true
			}
			for k := range keys {
				walk(path+"."+k, am[k], bm[k])
			}
			return
		}
		al, aok := a.([]any)
		bl, bok := b.([]any)
		if aok && bok && len(al) == len(bl) {
			for i := range al {
				walk(fmt.Sprintf("%s[%d]", path, i), al[i], bl[i])
			}
			return
		}
		ad, _ := json.Marshal(a)
		bd, _ := json.Marshal(b)
		if string(ad) != string(bd) {
			out = append(out, fmt.Sprintf("%s: %s -> %s", path, clip(string(ad), 120), clip(string(bd), 120)))
		}
	}
	walk("spec", before, after)
	sort.Strings(out)
	if len(out) > limit {
		out = append(out[:limit], fmt.Sprintf("... %d more", len(out)-limit))
	}
	return out
}

// OperabilityCheck is the accepted operability report's validity.
type OperabilityCheck struct {
	Valid      bool     `json:"valid"`
	Error      string   `json:"error,omitempty"`
	Checks     int      `json:"checks"`
	Duplicates []string `json:"duplicates,omitempty"`
}

// Blockers are the release's problems in one line each: what a live run would
// have stopped on, and what it would have shown as noise or churn.
func (r Release) Blockers() []string {
	var out []string
	if r.Error != "" {
		out = append(out, "error: "+r.Error)
	}
	if !r.Adopted && r.Error == "" {
		reason := r.Outcome
		if r.Hold != "" {
			reason = fmt.Sprintf("held %s (cause=%s owner=%s items=%s)", r.Hold, r.Cause, r.Owner, strings.Join(r.Items, ","))
		}
		out = append(out, "not adopted: "+reason+": "+clip(r.Detail, 400))
	}
	for _, f := range r.Drift {
		out = append(out, fmt.Sprintf("drift %s %s: %s", f.Detector, f.Resource, clip(f.Summary, 200)))
	}
	for _, g := range r.Generations {
		if !g.RenderedChange {
			out = append(out, fmt.Sprintf("generation churn: %s %d -> %d with no rendered change; live spec changed: %s", g.Workload, g.Before, g.After, strings.Join(g.Changed, "; ")))
		}
	}
	if r.Operability != nil && !r.Operability.Valid {
		out = append(out, "operability report invalid: "+r.Operability.Error)
	}
	for _, d := range r.Operability.duplicates() {
		out = append(out, "duplicate operability check "+d)
	}
	for _, e := range r.KubeletErrors {
		out = append(out, "harness: kubelet: "+clip(e, 300))
	}
	return out
}

func (o *OperabilityCheck) duplicates() []string {
	if o == nil {
		return nil
	}
	return o.Duplicates
}

func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// JSON is the report's file form.
func (r Report) JSON() ([]byte, error) {
	type withBlockers struct {
		Release
		Blockers []string `json:"blockers"`
	}
	out := struct {
		Name     string         `json:"name"`
		Seconds  float64        `json:"seconds"`
		Blocked  int            `json:"releases_with_blockers"`
		Releases []withBlockers `json:"releases"`
	}{Name: r.Name, Seconds: r.Seconds}
	for _, release := range r.Releases {
		blockers := release.Blockers()
		if blockers == nil {
			blockers = []string{}
		}
		if len(blockers) > 0 {
			out.Blocked++
		}
		out.Releases = append(out.Releases, withBlockers{release, blockers})
	}
	return json.MarshalIndent(out, "", "  ")
}

// Summary is the readable form: one block per release.
func (r Report) Summary() string {
	var b strings.Builder
	fmt.Fprintf(&b, "replay rehearsal %s: %d releases in %.1fs\n", r.Name, len(r.Releases), r.Seconds)
	for _, release := range r.Releases {
		state := "ADOPTED"
		if !release.Adopted {
			state = "NOT ADOPTED"
		}
		fmt.Fprintf(&b, "\n[%d] %s: %s (%s), %d agent dispatch(es), %.0fs injected, %.1fs wall\n", release.Index, release.Name, state, release.Outcome, len(release.Dispatches), release.InjectedSeconds, release.Seconds)
		for _, d := range release.Dispatches {
			fmt.Fprintf(&b, "    dispatch %s %s %s\n", d.Role, d.ID, clip(d.Reason, 160))
		}
		if len(release.Recreated) > 0 {
			fmt.Fprintf(&b, "    recreated: %s\n", strings.Join(release.Recreated, ", "))
		}
		if len(release.Leftover) > 0 {
			fmt.Fprintf(&b, "    leftover from earlier releases: %s\n", strings.Join(release.Leftover, ", "))
		}
		var changed []string
		for _, g := range release.Generations {
			changed = append(changed, fmt.Sprintf("%s %d->%d", g.Workload, g.Before, g.After))
		}
		sort.Strings(changed)
		if len(changed) > 0 {
			fmt.Fprintf(&b, "    generation changed: %s\n", strings.Join(changed, ", "))
		}
		if release.Operability != nil {
			fmt.Fprintf(&b, "    operability: valid=%v checks=%d\n", release.Operability.Valid, release.Operability.Checks)
		}
		for _, i := range release.Incidents {
			fmt.Fprintf(&b, "    incident: %s\n", clip(i, 300))
		}
		blockers := release.Blockers()
		if len(blockers) == 0 {
			b.WriteString("    no blockers\n")
		}
		for _, blocker := range blockers {
			fmt.Fprintf(&b, "    BLOCKER %s\n", blocker)
		}
	}
	return b.String()
}
