package bench

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// StampMaxAge is how long a passing preflight counts for `plan add`: a host's images, binaries and checkouts move.
const StampMaxAge = 24 * time.Hour

// PreflightStamp is what one `lanes preflight` run found for a pin set on a host: the fuzzops revision, the hooks
// name and revision a spec uses. OK is true when no check of that host failed; Failed lists the failures otherwise.
type PreflightStamp struct {
	Host     string    `json:"host"`
	Fuzzops  string    `json:"fuzzops"`
	Hooks    string    `json:"hooks,omitempty"`
	Revision string    `json:"revision,omitempty"` // the hooks (subject) revision
	At       time.Time `json:"at"`
	OK       bool      `json:"ok"`
	Failed   []string  `json:"failed,omitempty"`
}

func (s PreflightStamp) key() string {
	return strings.Join([]string{s.Host, s.Fuzzops, s.Hooks, s.Revision}, "|")
}

// Stamps is <root>/preflight.json: the latest stamp per (host, pins).
type Stamps struct {
	Stamps []PreflightStamp `json:"stamps"`
}

func stampsPath(r Root) string { return r.path("preflight.json") }

// LoadStamps reads the stamps; none recorded is an empty set.
func LoadStamps(root Root) (Stamps, error) {
	var st Stamps
	err := readJSON(stampsPath(root), &st)
	if errors.Is(err, os.ErrNotExist) {
		return Stamps{}, nil
	}
	return st, err
}

// SaveStamps merges stamps into the file: a stamp replaces the one of the same host and pins.
func SaveStamps(root Root, stamps []PreflightStamp) error {
	cur, err := LoadStamps(root)
	if err != nil {
		return err
	}
	by := map[string]PreflightStamp{}
	for _, s := range cur.Stamps {
		by[s.key()] = s
	}
	for _, s := range stamps {
		by[s.key()] = s
	}
	keys := make([]string, 0, len(by))
	for k := range by {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := Stamps{}
	for _, k := range keys {
		out.Stamps = append(out.Stamps, by[k])
	}
	return writeJSON(stampsPath(root), out)
}

// stampsFromResults turns a preflight report into stamps: for every spec and every host that can run it, the host
// passes when none of its checks failed. A failure on no known host (the manager-side checks) fails every stamp.
func stampsFromResults(inv Inventory, specs []specFile, results []PreflightResult, now time.Time) []PreflightStamp {
	failed := map[string][]string{}
	var global []string
	for _, r := range results {
		if r.OK {
			continue
		}
		line := r.Check + ": " + r.Detail
		if _, ok := inv.Hosts[r.Host]; ok {
			failed[r.Host] = append(failed[r.Host], line)
		} else {
			global = append(global, line)
		}
	}
	seen := map[string]bool{}
	var out []PreflightStamp
	for _, sf := range specs {
		for _, h := range inv.UsableHosts(sf.Spec) {
			st := PreflightStamp{Host: h, Fuzzops: sf.Fuzzops, Hooks: sf.Operator.Hooks, Revision: sf.Operator.Revision, At: now.UTC()}
			if seen[st.key()] {
				continue
			}
			seen[st.key()] = true
			st.Failed = append(append([]string(nil), failed[h]...), global...)
			st.OK = len(st.Failed) == 0
			out = append(out, st)
		}
	}
	return out
}

// checkPins refuses a spec whose pins have not passed preflight, recently, on at least one host that can run it.
func checkPins(inv Inventory, stamps Stamps, s Spec, now time.Time) error {
	hosts := inv.UsableHosts(s)
	pins := "fuzzops " + s.Fuzzops
	if s.Operator.Hooks != "" {
		pins += " with hooks " + s.Operator.Hooks + " " + s.Operator.Revision
	}
	var why []string
	for _, h := range hosts {
		var found *PreflightStamp
		for i, st := range stamps.Stamps {
			if st.Host == h && st.Fuzzops == s.Fuzzops && st.Hooks == s.Operator.Hooks && st.Revision == s.Operator.Revision {
				found = &stamps.Stamps[i]
			}
		}
		switch {
		case found == nil:
			why = append(why, h+": no preflight on record")
		case !found.OK:
			why = append(why, fmt.Sprintf("%s: preflight failed at %s: %s", h, found.At.Format("15:04Z"), strings.Join(found.Failed, "; ")))
		case now.Sub(found.At) > StampMaxAge:
			why = append(why, fmt.Sprintf("%s: preflight passed at %s, older than %s", h, found.At.Format("2006-01-02 15:04Z"), StampMaxAge))
		default:
			return nil
		}
	}
	return fmt.Errorf("the pins (%s) have not passed preflight on any host that can run them (%s): run `lanes preflight --plan FILE` first",
		pins, strings.Join(why, " | "))
}
