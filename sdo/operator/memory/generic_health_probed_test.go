package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/vantage"
)

// writeProbedMap writes a map of web and components in which one probed user
// operation enters at web and runs through path, over those edges.
func writeProbedMap(t *testing.T, root string, path []string, components ...string) {
	t.Helper()
	writeMap(t, root, append([]string{syntheticNoData}, components...)...)
	file := filepath.Join(root, layout.ArchitectureFile)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	edges := "edges:\n"
	for i := 1; i < len(path); i++ {
		edges += "  - {from: " + path[i-1] + ", to: " + path[i] + ", evidence: [manifest], source: x}\n"
	}
	doc := strings.Replace(string(data), "edges: []\n", edges, 1)
	doc = strings.Replace(doc, "operations: []\n", "operations:\n  - {id: get, writes: false, entry: {service: web, port: 80, method: GET, path: /}, path: ["+strings.Join(path, ", ")+"]}\n", 1)
	doc = strings.Replace(doc, "probes: []\n", "probes:\n  - {scenario: call-get, kind: call, operations: [get]}\n", 1)
	if err := os.WriteFile(file, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
}

// plannedStoreReaders lists the planned store readers as component@interval.
func plannedStoreReaders(root string) []string {
	var planned []string
	for _, d := range plannedStoreStateDetectors(root) {
		planned = append(planned, strings.TrimPrefix(d.manifest.ID, StoreStateDetectorPrefix)+"@"+d.manifest.Interval)
	}
	return planned
}

// Every combination of probed and other reads fits Go's load budget; the
// probed reads get the shortest interval that leaves room for the others,
// and the others are never read more often than they are.
func TestReadMultiplesFitTheBudgetProbedFirst(t *testing.T) {
	const budget = 8
	for p := range 33 {
		for u := range 33 {
			if p+u == 0 {
				continue
			}
			kp, ku := readMultiples(p, u, budget)
			load := 0.0
			if p > 0 {
				load += float64(p) / float64(kp)
			}
			if u > 0 {
				load += float64(u) / float64(ku)
			}
			if load > budget+1e-9 || kp < 1 || ku < kp {
				t.Fatalf("p=%d u=%d: multiples %d,%d load %.2f", p, u, kp, ku, load)
			}
			if p > 0 && kp > 1 && float64(p)/float64(kp-1) < budget && u == 0 {
				t.Fatalf("p=%d u=0: multiple %d is not the smallest", p, kp)
			}
		}
	}
}

// A store no user operation reaches cannot explain a failing user request, so
// stores on a probed operation's path are read more often: a cache on that
// path before an authoritative store off it. Every store still gets a reader,
// and together they stay within Go's half of the pod-read load.
func TestStoreStateReadersPreferStoresOnProbedPaths(t *testing.T) {
	root := t.TempDir()
	components := []string{
		"  - {name: z-db, workload: StatefulSet/z-db, services: [z-db], holds_data: true, data: {engine: redis, role: authoritative}}",
		"  - {name: a-cache, workload: Deployment/a-cache, services: [a-cache], holds_data: false, data: {engine: redis, role: derived, derived_from: [z-db]}}",
	}
	for i := range StoreStateLoad {
		components = append(components, fmt.Sprintf("  - {name: idle-%d, workload: StatefulSet/idle-%d, services: [idle-%d], holds_data: true, data: {engine: redis, role: authoritative}}", i, i, i))
	}
	writeProbedMap(t, root, []string{"web", "a-cache", "z-db"}, components...)
	want := []string{"z-db@30s", "a-cache@30s"}
	for i := range StoreStateLoad {
		want = append(want, fmt.Sprintf("idle-%d@1m0s", i))
	}
	if got := plannedStoreReaders(root); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("planned store readers %v, want %v", got, want)
	}
}

// A map without probes keeps the old order (authoritative first, then name)
// and spreads every store over one interval.
func TestStoreStateReadersWithoutProbesKeepTheOldOrder(t *testing.T) {
	root := t.TempDir()
	components := []string{syntheticStore}
	for i := range StoreStateLoad + 2 {
		components = append(components, fmt.Sprintf("  - {name: copy-%d, workload: Deployment/copy-%d, services: [copy-%d], holds_data: false, data: {engine: redis, role: derived, derived_from: [kv]}}", i, i, i))
	}
	writeMap(t, root, components...)
	want := []string{"kv@1m0s"}
	for i := range StoreStateLoad + 2 {
		want = append(want, fmt.Sprintf("copy-%d@1m0s", i))
	}
	if got := plannedStoreReaders(root); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("planned store readers %v, want %v", got, want)
	}
}

// A client link is on a user path when both its ends are probed: such links
// are dialed more often than links to components no user operation reaches,
// even those an entry component calls. Every link is dialed.
func TestClientLinksPreferLinksOnProbedPaths(t *testing.T) {
	root := t.TempDir()
	components := []string{
		"  - {name: api, workload: Deployment/api, services: [api], holds_data: false}",
		"  - {name: kv, workload: StatefulSet/kv, services: [kv], holds_data: true, data: {engine: redis, role: authoritative}}",
	}
	links := "  - {from: api, to: kv, port: 6379, protocol: redis}\n  - {from: web, to: api, port: 80, protocol: tcp}\n"
	for i := range ClientLinkLoad {
		components = append(components, fmt.Sprintf("  - {name: idle-%d, workload: Deployment/idle-%d, services: [idle-%d], holds_data: false}", i, i, i))
		links += fmt.Sprintf("  - {from: web, to: idle-%d, port: 80, protocol: tcp}\n", i)
	}
	writeProbedMap(t, root, []string{"web", "api", "kv"}, components...)
	writeLinks(t, root, links)
	var planned []string
	for _, l := range plannedClientLinks(root) {
		planned = append(planned, l.Link.From+">"+l.Link.To+"@"+l.Interval.String())
	}
	want := []string{"web>api@30s", "api>kv@30s"}
	for i := range ClientLinkLoad {
		want = append(want, fmt.Sprintf("web>idle-%d@1m0s", i))
	}
	if strings.Join(planned, " ") != strings.Join(want, " ") {
		t.Fatalf("planned client links %v, want %v", planned, want)
	}
}

// A synthetic hotel-like map: ten stores matter and sixteen links are mapped.
// The front calls search, profile, user, reserve and rate; every service is
// configured to use a registry that no operation path lists; reserve's probe
// is knownBroken; rate's operation has no probe. Every store gets a reader
// and every link a dial: the registry and the broken path's store count as
// probed, rate's stores do not, and the load stays within Go's budget.
func TestHotelLikeMapReadsEveryStoreAndDialsEveryLink(t *testing.T) {
	root := t.TempDir()
	services := []string{"search", "geo", "profile", "user", "reserve", "rate"}
	stores := map[string]string{
		"geo-db": "mongodb", "profile-db": "mongodb", "profile-cache": "memcached", "user-db": "mongodb",
		"reserve-db": "mongodb", "reserve-cache": "memcached", "rate-db": "mongodb", "rate-cache": "memcached",
		"registry": "consul", "spare-db": "mongodb",
	}
	var components []string
	for _, name := range services {
		components = append(components, fmt.Sprintf("  - {name: %s, workload: Deployment/%s, services: [%s], holds_data: false}", name, name, name))
	}
	for _, name := range sortedKeys(stores) {
		components = append(components, fmt.Sprintf("  - {name: %s, workload: StatefulSet/%s, services: [%s], holds_data: true, data: {engine: %s, role: authoritative}}", name, name, name, stores[name]))
	}
	calls := [][2]string{
		{"web", "search"}, {"search", "geo"}, {"geo", "geo-db"}, {"web", "profile"}, {"profile", "profile-db"}, {"profile", "profile-cache"},
		{"web", "user"}, {"user", "user-db"}, {"web", "reserve"}, {"reserve", "reserve-db"}, {"reserve", "reserve-cache"},
		{"web", "rate"}, {"rate", "rate-db"}, {"rate", "rate-cache"},
	}
	for _, name := range append([]string{"web"}, services...) {
		calls = append(calls, [2]string{name, "registry"})
	}
	edges, links := "edges:\n", ""
	for _, c := range calls {
		edges += "  - {from: " + c[0] + ", to: " + c[1] + ", evidence: [config], source: x}\n"
	}
	// Sixteen mapped links: the request hops, then two registry dials.
	for _, c := range append(calls[:14], [2]string{"web", "registry"}, [2]string{"search", "registry"}) {
		links += "  - {from: " + c[0] + ", to: " + c[1] + ", port: 80, protocol: tcp}\n"
	}
	ops := "operations:\n" +
		"  - {id: search, writes: false, entry: {service: web, port: 80, method: GET, path: /search}, path: [web, search, geo, geo-db]}\n" +
		"  - {id: profile, writes: false, entry: {service: web, port: 80, method: GET, path: /profile}, path: [web, profile, profile-cache, profile-db]}\n" +
		"  - {id: login, writes: false, entry: {service: web, port: 80, method: GET, path: /login}, path: [web, user, user-db]}\n" +
		"  - {id: reserve, writes: true, entry: {service: web, port: 80, method: POST, path: /reserve}, path: [web, reserve, reserve-cache, reserve-db]}\n" +
		"  - {id: rate, writes: false, entry: {service: web, port: 80, method: GET, path: /rate}, path: [web, rate, rate-cache, rate-db]}\n"
	probes := "probes:\n  - {scenario: call-search, kind: call, operations: [search]}\n  - {scenario: call-profile, kind: call, operations: [profile]}\n" +
		"  - {scenario: call-login, kind: call, operations: [login]}\n  - {scenario: call-reserve, kind: call, operations: [reserve]}\n"
	writeMap(t, root, append([]string{syntheticNoData}, components...)...)
	file := filepath.Join(root, layout.ArchitectureFile)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.Replace(string(data), "edges: []\n", edges, 1)
	doc = strings.Replace(doc, "operations: []\n", ops, 1)
	doc = strings.Replace(doc, "probes: []\n", probes, 1)
	if err := os.WriteFile(file, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	writeLinks(t, root, links)
	workloads := "apiVersion: sdo.dev/v1alpha1\nkind: TrafficWorkload\nname: probe-call\npurpose: health-probe\nscenarios:\n" +
		"  - id: call-search\n  - id: call-profile\n  - id: call-login\n  - id: call-reserve\n    knownBroken: {reason: synthetic, failsWith: [\"500\"]}\n"
	if err := os.WriteFile(filepath.Join(root, layout.TrafficWorkloadsDir, "probe-call.yaml"), []byte(workloads), 0o644); err != nil {
		t.Fatal(err)
	}
	_, coverage, ok := acceptedMap(root)
	if !ok || !coverage.Components["reserve-db"].Probed() || !coverage.Components["registry"].Probed() || coverage.Components["rate-db"].Probed() {
		t.Fatalf("coverage: reserve-db %+v, registry %+v, rate-db %+v", coverage.Components["reserve-db"], coverage.Components["registry"], coverage.Components["rate-db"])
	}

	readers := plannedStoreStateDetectors(root)
	if len(readers) != len(stores) {
		t.Fatalf("planned %d store readers, want one per store: %v", len(readers), plannedStoreReaders(root))
	}
	var queries []podread.Query
	interval := map[string]string{}
	for _, d := range readers {
		interval[strings.TrimPrefix(d.manifest.ID, StoreStateDetectorPrefix)] = d.manifest.Interval
		queries = append(queries, podread.Query{Interval: mustDuration(t, d.manifest.Interval)})
	}
	if load := podread.Load(queries); load > StoreStateLoad {
		t.Fatalf("store readers load %.2f reads per %s, over Go's %d", load, podread.MinInterval, StoreStateLoad)
	}
	for _, probed := range []string{"geo-db", "profile-db", "profile-cache", "user-db", "reserve-db", "reserve-cache", "registry"} {
		for _, other := range []string{"rate-db", "rate-cache", "spare-db"} {
			if mustDuration(t, interval[probed]) > mustDuration(t, interval[other]) {
				t.Errorf("%s read every %s, less often than %s (%s)", probed, interval[probed], other, interval[other])
			}
		}
	}

	dials := plannedClientLinks(root)
	if len(dials) != 16 {
		t.Fatalf("planned %d client links, want all 16", len(dials))
	}
	var probesOf []vantage.Probe
	for _, l := range dials {
		probesOf = append(probesOf, vantage.Probe{Interval: l.Interval})
	}
	if load := vantage.Load(probesOf); load > ClientLinkLoad {
		t.Fatalf("client links load %.2f, over Go's %d", load, ClientLinkLoad)
	}
	t.Logf("store readers %v", plannedStoreReaders(root))
	for _, l := range dials {
		t.Logf("link %s>%s every %s", l.Link.From, l.Link.To, l.Interval)
	}
}

func mustDuration(t *testing.T, text string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(text)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func sortedKeys(m map[string]string) []string {
	var out []string
	for key := range m {
		out = append(out, key)
	}
	slices.Sort(out)
	return out
}
