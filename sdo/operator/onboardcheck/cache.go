package onboardcheck

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// Inputs are what decides a verdict of the live items, so a verdict recorded
// for the same inputs can stand for a new run: the map, the files of the
// memory's diagnostics, the deployed workloads, and Salt (the build of the
// check, its bounds and how it breaks). An item whose inputs are unknown has no
// key and is never reused.
type Inputs struct {
	Map appmap.Map
	// Files are the memory's diagnostics by repository path, as the detectors
	// are built from them (.sdo/diagnostics/...).
	Files map[string]string
	// Deploy is a fingerprint of each component's deployed workload that does
	// not change when the workload is scaled or restored.
	Deploy map[string]string
	Salt   string
}

const (
	diagnosticsPrefix = ".sdo/diagnostics/"
	trafficPrefix     = ".sdo/diagnostics/traffic/"
	workloadsPrefix   = ".sdo/diagnostics/traffic/workloads/"
	detectorsPrefix   = ".sdo/diagnostics/detectors/"
)

func newKey() (*hasherSum, bool) { return &hasherSum{sha: sha256.New()}, true }

type hasherSum struct {
	sha interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
}

func (h *hasherSum) add(label string, parts ...string) {
	fmt.Fprintf(h.sha, "%s\x00%d\x00", label, len(parts))
	for _, p := range parts {
		fmt.Fprintf(h.sha, "%d\x00%s\x00", len(p), p)
	}
}

func (h *hasherSum) json(label string, v any) {
	data, _ := json.Marshal(v)
	h.add(label, string(data))
}

func (h *hasherSum) sum() string { return hex.EncodeToString(h.sha.Sum(nil)) }

// scenarioIDs are the scenarios any probe of the map declares.
func (in Inputs) scenarioIDs() []string {
	var out []string
	for _, p := range in.Map.Probes {
		out = append(out, p.Scenario)
	}
	return out
}

func mentionsAny(text string, names []string) bool {
	text = strings.ToLower(text)
	for _, n := range names {
		if named(text, n) {
			return true
		}
	}
	return false
}

// detectorGroup is the directory of the detector a file belongs to
// (".../detectors/<kind>/<id>"), or "" for any other file.
func detectorGroup(path string) (group, id string) {
	rest, ok := strings.CutPrefix(path, detectorsPrefix)
	if !ok {
		return "", ""
	}
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) < 3 {
		return "", ""
	}
	return detectorsPrefix + parts[0] + "/" + parts[1], parts[1]
}

func (in Inputs) paths() []string {
	paths := make([]string, 0, len(in.Files))
	for p := range in.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// sharedTraffic reports whether a traffic file names no scenario at all (a
// helper every generator may use, a workload with none): it belongs to every
// verdict.
func (in Inputs) sharedTraffic(path string) bool {
	return strings.HasPrefix(path, trafficPrefix) && !mentionsAny(in.Files[path], in.scenarioIDs())
}

// scope hashes what bears on the operations: their map entries, the components
// on their paths with the edges between them and their deployments, the traffic
// files that name their scenarios, and the detectors that name them or what is
// on their paths. It returns false when a deployment is unknown.
func (in Inputs) scope(h *hasherSum, opIDs []string) bool {
	ids := slices.Clone(opIDs)
	sort.Strings(ids)
	onPath := map[string]bool{}
	var names, scenarios []string
	for _, id := range ids {
		for _, op := range in.Map.Operations {
			if op.ID != id {
				continue
			}
			h.json("operation", op)
			names = append(names, op.ID)
			for _, e := range in.Map.Entries {
				if e.Service == op.Entry.Service {
					h.json("entry", e)
				}
			}
			for _, p := range in.Map.Probes {
				if slices.Contains(p.Operations, op.ID) {
					h.json("probe", p)
					scenarios = append(scenarios, p.Scenario)
				}
			}
			for _, name := range op.Path {
				onPath[name] = true
			}
		}
	}
	var comps []string
	for name := range onPath {
		comps = append(comps, name)
	}
	sort.Strings(comps)
	for _, name := range comps {
		declared := false
		for _, c := range in.Map.Components {
			declared = declared || c.Name == name && c.Workload != ""
		}
		if declared {
			fp, ok := in.Deploy[name]
			if !ok || fp == "" {
				return false
			}
			h.add("deploy", name, fp)
		} else {
			// No workload (or not declared): nothing is deployed to fingerprint
			// and it is never broken, so it must not keep its operations from
			// being reused.
			h.add("undeployed", name)
		}
		for _, c := range in.Map.Components {
			if c.Name == name {
				h.json("component", c)
				names = append(names, c.Name)
				if _, w, ok := strings.Cut(c.Workload, "/"); ok {
					names = append(names, w)
				} else if c.Workload != "" {
					names = append(names, c.Workload)
				}
				names = append(names, c.Services...)
			}
		}
	}
	for _, e := range in.Map.Edges {
		if onPath[e.From] && onPath[e.To] {
			h.json("edge", e)
		}
	}
	names = append(names, scenarios...)
	included := map[string]bool{}
	groups := map[string][]string{}
	for _, path := range in.paths() {
		text := in.Files[path]
		switch {
		case strings.HasPrefix(path, trafficPrefix):
			if in.sharedTraffic(path) || mentionsAny(text, scenarios) {
				included[path] = true
			}
		case strings.HasPrefix(path, detectorsPrefix):
			if group, _ := detectorGroup(path); group != "" {
				groups[group] = append(groups[group], path)
			}
		case strings.HasPrefix(path, diagnosticsPrefix):
			included[path] = true
		}
	}
	others := in.otherNames(names)
	for _, files := range groups {
		_, id := detectorGroup(files[0])
		var take bool
		if strings.HasPrefix(id, "traffic-") {
			// The Go-installed traffic detector follows the workload file it consumes.
			take = trafficDetectorIncluded(id, included)
		} else {
			// Safe side: a detector is left out only when it names something
			// else and nothing of this scope. One that names nothing (it matches
			// by prefix, label or a loop over workloads) is part of every verdict.
			take = mentionsDetector(in, files, names) || !mentionsDetector(in, files, others)
		}
		if take {
			for _, f := range files {
				included[f] = true
			}
		}
	}
	for _, path := range in.paths() {
		if included[path] {
			h.add("file", path, in.Files[path])
		}
	}
	return true
}

// otherNames are the names the map declares (operations, components, their
// workloads and services, scenarios) that are not among names.
func (in Inputs) otherNames(names []string) []string {
	var all []string
	for _, op := range in.Map.Operations {
		all = append(all, op.ID)
	}
	for _, c := range in.Map.Components {
		all = append(all, c.Name)
		if _, w, ok := strings.Cut(c.Workload, "/"); ok {
			all = append(all, w)
		} else if c.Workload != "" {
			all = append(all, c.Workload)
		}
		all = append(all, c.Services...)
	}
	all = append(all, in.scenarioIDs()...)
	var out []string
	for _, n := range all {
		if n != "" && !slices.Contains(names, n) && !slices.Contains(out, n) {
			out = append(out, n)
		}
	}
	return out
}

func mentionsDetector(in Inputs, files, names []string) bool {
	for _, f := range files {
		if mentionsAny(in.Files[f], names) {
			return true
		}
	}
	return false
}

// trafficDetectorIncluded: the Go-installed detector traffic-<workload> follows
// the workload file it consumes.
func trafficDetectorIncluded(id string, included map[string]bool) bool {
	w, ok := strings.CutPrefix(id, "traffic-")
	return ok && included[workloadsPrefix+w+".yaml"]
}

func (in Inputs) opsThrough(component string) []string {
	var ids []string
	for _, op := range in.Map.Operations {
		if slices.Contains(op.Path, component) {
			ids = append(ids, op.ID)
		}
	}
	return ids
}

// ComponentKey keys the verdict of breaking the component, or "" when it
// cannot be keyed.
func (in Inputs) ComponentKey(name string) string {
	ops := in.opsThrough(name)
	if len(ops) == 0 {
		return ""
	}
	h, _ := newKey()
	h.add("component-break", in.Salt, name)
	if !in.scope(h, ops) {
		return ""
	}
	return h.sum()
}

// FaultKey keys the verdict of a generic fault that hurts the operations.
func (in Inputs) FaultKey(name string, ops []string) string {
	if len(ops) == 0 {
		return ""
	}
	h, _ := newKey()
	h.add("fault", in.Salt, name)
	if !in.scope(h, ops) {
		return ""
	}
	return h.sum()
}

// BreakKeys keys every component on an operation's path and every fault.
func (in Inputs) BreakKeys(faults []Fault) map[string]string {
	keys := map[string]string{}
	for _, name := range pathComponents(in.Map) {
		if k := in.ComponentKey(name); k != "" {
			keys[name] = k
		}
	}
	for _, f := range faults {
		if k := in.FaultKey(f.Name(), f.Operations()); k != "" {
			keys[f.Name()] = k
		}
	}
	return keys
}

// DetectorKey keys the healthy-window verdict of one detector: its own files
// (the workload and generator files of a traffic detector), the map, and every
// deployment, since a quiet verdict is about the deployed application.
func (in Inputs) DetectorKey(id string) string {
	h, _ := newKey()
	h.add("detector", in.Salt, id)
	var comps []string
	for _, c := range in.Map.Components {
		if c.Workload != "" && in.Deploy[c.Name] == "" {
			return "" // a deployment is unknown: nothing can vouch for the verdict
		}
	}
	for name := range in.Deploy {
		comps = append(comps, name)
	}
	sort.Strings(comps)
	for _, name := range comps {
		h.add("deploy", name, in.Deploy[name])
	}
	own := false
	scenarios := in.scenarioIDs()
	var yamlScenarios []string
	if w, ok := strings.CutPrefix(id, "traffic-"); ok {
		yaml := in.Files[workloadsPrefix+w+".yaml"]
		for _, s := range scenarios {
			if named(strings.ToLower(yaml), s) {
				yamlScenarios = append(yamlScenarios, s)
			}
		}
		if len(yamlScenarios) == 0 {
			// No workload file of that name, or it names no scenario: the
			// detector (the Go-installed traffic-probe-call, for one) runs every
			// scenario, so every scenario file decides its verdict.
			yamlScenarios = scenarios
		}
	}
	for _, path := range in.paths() {
		text := in.Files[path]
		if group, gid := detectorGroup(path); group != "" {
			if gid == id {
				own = true
				h.add("file", path, text)
			}
			continue
		}
		switch {
		case path == workloadsPrefix+strings.TrimPrefix(id, "traffic-")+".yaml" && strings.HasPrefix(id, "traffic-"):
			h.add("file", path, text)
		case strings.HasPrefix(path, trafficPrefix) && strings.HasPrefix(id, "traffic-") && !strings.HasPrefix(path, workloadsPrefix):
			if in.sharedTraffic(path) || mentionsAny(text, yamlScenarios) {
				h.add("file", path, text)
			}
		case strings.HasPrefix(path, diagnosticsPrefix) && !strings.HasPrefix(path, trafficPrefix):
			h.add("file", path, text)
		}
	}
	if !own {
		// A detector Go installs from the map and the telemetry environment.
		h.json("map", in.Map)
	}
	return h.sum()
}

// Cache holds the verdicts of earlier runs by item, each with the key of the
// inputs it was judged on. It is the one record a check in a session keeps.
type Cache struct {
	Breaks    map[string]BreakRecord    `json:"breaks,omitempty"`
	Detectors map[string]DetectorRecord `json:"detectors,omitempty"`
}

// BreakRecord is one break item's recorded outcome.
type BreakRecord struct {
	Key     string        `json:"key"`
	At      time.Time     `json:"at"`
	Outcome StoredOutcome `json:"outcome"`
}

// DetectorRecord says a detector judged the healthy application, with no raise,
// on inputs with this key.
type DetectorRecord struct {
	Key string    `json:"key"`
	At  time.Time `json:"at"`
}

// StoredRaise is a raise with how long into the break it appeared.
type StoredRaise struct {
	Raise        Raise   `json:"raise"`
	AfterSeconds float64 `json:"after_seconds"`
}

// StoredOutcome is the part of a break outcome that reports read.
type StoredOutcome struct {
	Names     []string               `json:"names"`
	How       string                 `json:"how"`
	Found     []StoredRaise          `json:"found,omitempty"`
	Hit       *StoredRaise           `json:"hit,omitempty"`
	OpHit     map[string]StoredRaise `json:"op_hit,omitempty"`
	OpFailure map[string]StoredRaise `json:"op_failure,omitempty"`
	Stray     map[string]StoredRaise `json:"stray,omitempty"`
	Wave      []string               `json:"wave,omitempty"`
}

func storeRaise(f *foundRaise) *StoredRaise {
	if f == nil {
		return nil
	}
	return &StoredRaise{Raise: f.raise, AfterSeconds: f.after.Seconds()}
}

func (s StoredRaise) found() *foundRaise {
	return &foundRaise{raise: s.Raise, after: time.Duration(s.AfterSeconds * float64(time.Second))}
}

func storeMap(m map[string]*foundRaise) map[string]StoredRaise {
	if len(m) == 0 {
		return nil
	}
	out := map[string]StoredRaise{}
	for k, v := range m {
		out[k] = *storeRaise(v)
	}
	return out
}

func (s StoredOutcome) loadMap(m map[string]StoredRaise) map[string]*foundRaise {
	out := map[string]*foundRaise{}
	for k, v := range m {
		out[k] = v.found()
	}
	return out
}

// reusable says a fresh break outcome may stand for later runs: it ran to a
// verdict, a detector named the break (a fault: named an operation it hurts),
// and it showed no missing edge. A red or failed one is re-run, as that is the
// verdict the agent is working on; a stray failure (a missing edge) depends on
// operations outside the component's key and on the wave it ran in, so it is
// always judged again.
func reusable(out *breakOutcome, fault bool) bool {
	if out == nil || out.err != "" || out.how == "no workload" || len(out.stray) > 0 || !out.reusedAt.IsZero() {
		return false
	}
	if fault {
		return len(out.opHit) > 0
	}
	return out.hit != nil
}

func storeOutcome(out *breakOutcome) StoredOutcome {
	s := StoredOutcome{Names: out.names, How: out.how, Wave: out.wave, Hit: storeRaise(out.hit),
		OpHit: storeMap(out.opHit), OpFailure: storeMap(out.opFailure), Stray: storeMap(out.stray)}
	for i := range out.found {
		s.Found = append(s.Found, *storeRaise(&out.found[i]))
	}
	return s
}

func (s StoredOutcome) outcome(at time.Time) *breakOutcome {
	out := &breakOutcome{names: s.Names, how: s.How, wave: s.Wave, reusedAt: at,
		opHit: s.loadMap(s.OpHit), opFailure: s.loadMap(s.OpFailure), stray: s.loadMap(s.Stray)}
	for _, f := range s.Found {
		out.found = append(out.found, *f.found())
	}
	if s.Hit != nil {
		out.hit = s.Hit.found()
	}
	return out
}

// reusedLabel is how a reused line says where it comes from, in Seattle time.
func reusedLabel(at time.Time) string {
	if loc, err := time.LoadLocation("America/Los_Angeles"); err == nil {
		at = at.In(loc)
	}
	return "reused from " + at.Format("15:04 MST")
}

// MemoDetectorKey is DetectorKey computed once per detector: the quiet window
// asks for every detector on every sample.
func (in Inputs) MemoDetectorKey() func(id string) string {
	memo := map[string]string{}
	var mu sync.Mutex
	return func(id string) string {
		mu.Lock()
		defer mu.Unlock()
		if k, ok := memo[id]; ok {
			return k
		}
		k := in.DetectorKey(id)
		memo[id] = k
		return k
	}
}
