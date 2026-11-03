//go:build integration

package sim_test

// The conformance harness runs one table of small scenarios against a real
// kube-apiserver (envtest, pinned by scripts/envtest_assets.sh) and against the
// simulated API, then compares what a client observes. A scenario is a list of
// HTTP-level steps; each step's status, reason and normalized body must match.
// Differences are either fixed in package sim or listed in a scenario's
// accepted map, each with the reason that appears in docs/simulation-fidelity.md.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/runtime/envtestio"
	"sdo.dev/operator/runtime/sim"
)

// step is one request. Body is marshalled as JSON unless it is a string; Fn
// builds the body from earlier results.
type step struct {
	Name, Method, Path string
	ContentType        string
	Query              map[string]string
	Body               any
	Fn                 func(*run) any
	// Accept overrides the Accept header (metadata-only reads).
	Accept string
}

// scenario is a named step list. Accepted maps a diff path ("step: dotted.path",
// list indices written []) to the reason the simulation is allowed to differ.
type scenario struct {
	Name     string
	Steps    []step
	Accepted map[string]string
	// Project reduces both sides' objects (by step name) before comparison, for
	// observations that cover more than the simulation serves, such as discovery.
	Project func(step string, object any) any
	// Monotonic requires object versions to never decrease across steps.
	Monotonic bool
	Skip      string
}

type observed struct {
	Step   string
	Code   int
	Reason string
	Object any
}

type run struct {
	ns      string
	results map[string]any
	uids    map[string]string
	rvs     map[string]string
	rvSeen  []int
	t       *testing.T
}

func (r *run) rv(step string) string  { return r.rvs[step] }
func (r *run) uid(step string) string { return r.uids[step] }

type backend struct {
	name string
	rest rest.Interface
	// nsSetup creates the namespace a scenario runs in (real API only).
	nsSetup bool
}

func startEnvtest(t *testing.T) *backend {
	t.Helper()
	env := envtestio.Start(t)
	cfg := rest.CopyConfig(env.Config)
	cfg.UserAgent = "conformance"
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &backend{name: "envtest", rest: client.Discovery().RESTClient(), nsSetup: true}
}

func newSim(t *testing.T) *backend {
	t.Helper()
	w := sim.NewWorld(1)
	client, err := w.ClientFor(&rest.Config{UserAgent: "conformance"})
	if err != nil {
		t.Fatal(err)
	}
	return &backend{name: "sim", rest: client.Discovery().RESTClient()}
}

var counter int

func (b *backend) play(t *testing.T, sc scenario) []observed {
	t.Helper()
	counter++
	r := &run{ns: "c" + strconv.Itoa(counter), results: map[string]any{}, uids: map[string]string{}, rvs: map[string]string{}, t: t}
	ctx := context.Background()
	if b.nsSetup {
		body := fmt.Sprintf(`{"apiVersion":"v1","kind":"Namespace","metadata":{"name":%q}}`, r.ns)
		if err := b.rest.Post().AbsPath("/api/v1/namespaces").SetHeader("Content-Type", "application/json").Body([]byte(body)).Do(ctx).Error(); err != nil {
			t.Fatalf("namespace: %v", err)
		}
	}
	var out []observed
	for _, s := range sc.Steps {
		out = append(out, b.do(ctx, r, s))
	}
	return out
}

func (b *backend) do(ctx context.Context, r *run, s step) observed {
	body := s.Body
	if s.Fn != nil {
		body = s.Fn(r)
	}
	var raw []byte
	switch v := body.(type) {
	case nil:
	case string:
		raw = []byte(v)
	default:
		raw, _ = json.Marshal(v)
	}
	raw = []byte(strings.ReplaceAll(string(raw), "{ns}", r.ns))
	ctype := s.ContentType
	if ctype == "" {
		ctype = "application/json"
	}
	verb := s.Method
	if verb == "WATCH" {
		verb = "GET"
	}
	req := b.rest.Verb(verb).AbsPath(strings.ReplaceAll(s.Path, "{ns}", r.ns))
	req.SetHeader("Content-Type", ctype)
	accept := "application/json"
	if s.Accept != "" {
		accept = s.Accept
	}
	req.SetHeader("Accept", accept)
	for k, v := range s.Query {
		if strings.HasPrefix(v, "@") {
			v = r.rv(v[1:])
		}
		req.Param(k, v)
	}
	if s.Method == "WATCH" {
		return b.watch(ctx, r, s)
	}
	if raw != nil {
		req.Body(raw)
	}
	res := req.Do(ctx)
	obs := observed{Step: s.Name}
	var httpCode int
	res.StatusCode(&httpCode)
	data, err := res.Raw()
	if err != nil {
		status := apierrors.APIStatus(nil)
		if st, ok := err.(apierrors.APIStatus); ok {
			status = st
		}
		if status == nil {
			obs.Code, obs.Reason, obs.Object = -1, "TransportError", err.Error()
			return obs
		}
		st := status.Status()
		obs.Code, obs.Reason = int(st.Code), string(st.Reason)
		details := map[string]any{}
		if st.Details != nil {
			details["name"] = st.Details.Name
			details["kind"] = st.Details.Kind
			details["group"] = st.Details.Group
			if len(st.Details.Causes) > 0 {
				var causes []string
				for _, c := range st.Details.Causes {
					causes = append(causes, string(c.Type)+":"+c.Field)
				}
				details["causes"] = causes
			}
		}
		obs.Object = map[string]any{"details": details, "message": strings.ReplaceAll(st.Message, r.ns, "{ns}")}
		return obs
	}
	obs.Code = httpCode
	var body2 any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &body2); err != nil {
			obs.Object = string(data)
			return obs
		}
	}
	obs.Object = body2
	if m, ok := body2.(map[string]any); ok {
		if md, ok := m["metadata"].(map[string]any); ok {
			if uid, ok := md["uid"].(string); ok {
				r.uids[s.Name] = uid
			}
			if rv, ok := md["resourceVersion"].(string); ok {
				r.rvs[s.Name] = rv
			}
		}
		if m["kind"] == "Status" && m["status"] == "Success" {
			obs.Object = m
		}
	}
	r.results[s.Name] = body2
	return obs
}

// ---- normalization -----------------------------------------------------------

var ipRe = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+$`)

type normalizer struct {
	ns   string
	uids map[string]string
	rvs  map[string]string
	t    *testing.T
	name string
}

func (n *normalizer) rv(v string) string {
	if l, ok := n.rvs[v]; ok {
		return l
	}
	l := "rv" + strconv.Itoa(len(n.rvs)+1)
	n.rvs[v] = l
	return l
}

func (n *normalizer) walk(path string, v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, val := range x {
			p := path + "." + k
			switch {
			case strings.HasSuffix(path, ".metadata") && k == "uid":
				s, _ := val.(string)
				if _, ok := n.uids[s]; !ok {
					n.uids[s] = "uid" + strconv.Itoa(len(n.uids)+1)
				}
				out[k] = n.uids[s]
			case strings.HasSuffix(path, ".metadata") && k == "resourceVersion":
				out[k] = n.rv(val.(string))
			case strings.HasSuffix(path, ".metadata") && k == "selfLink":
			case strings.HasSuffix(path, ".metadata") && (k == "creationTimestamp" || k == "deletionTimestamp") && val != nil:
				out[k] = "<time>"
			case k == "time" && strings.Contains(path, "managedFields"):
				out[k] = "<time>"
			case k == "namespace" || (path == ".metadata" && k == "namespace"):
				if s, ok := val.(string); ok && s == n.ns {
					out[k] = "{ns}"
				} else {
					out[k] = n.walk(p, val)
				}
			case k == "clusterIP" && val != "None" && val != "":
				out[k] = "<cluster-ip>"
			case k == "clusterIPs":
				ips, _ := val.([]any)
				norm := make([]any, len(ips))
				for i, ip := range ips {
					norm[i] = ip
					if str, _ := ip.(string); str != "None" {
						norm[i] = "<cluster-ip>"
					}
				}
				out[k] = norm
			case k == "nodePort":
				out[k] = "<node-port>"
			default:
				out[k] = n.walk(p, val)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = n.walk(path+"[]", e)
		}
		return out
	case string:
		for uid, label := range n.uids {
			x = strings.ReplaceAll(x, uid, label)
		}
		return strings.ReplaceAll(x, n.ns, "{ns}")
	default:
		return v
	}
}

// normalizeList also folds the list-level resourceVersion, which depends on
// unrelated writes in a shared store, into a marker checked separately.
func collectUIDs(v any, into map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		if md, ok := x["metadata"].(map[string]any); ok {
			if uid, ok := md["uid"].(string); ok {
				if _, seen := into[uid]; !seen {
					into[uid] = "uid" + strconv.Itoa(len(into)+1)
				}
			}
		}
		for _, e := range x {
			collectUIDs(e, into)
		}
	case []any:
		for _, e := range x {
			collectUIDs(e, into)
		}
	}
}

func (n *normalizer) obs(o observed) observed {
	collectUIDs(o.Object, n.uids)
	if m, ok := o.Object.(map[string]any); ok {
		if md, ok := m["metadata"].(map[string]any); ok && strings.HasSuffix(fmt.Sprint(m["kind"]), "List") {
			md["resourceVersion"] = "<list>"
			delete(md, "remainingItemCount")
		}
	}
	o.Object = n.walk("", o.Object)
	return o
}

func flatten(prefix string, v any, out map[string]string) {
	switch x := v.(type) {
	case map[string]any:
		if len(x) == 0 {
			out[prefix] = "{}"
		}
		for k, val := range x {
			flatten(prefix+"."+k, val, out)
		}
	case []any:
		if len(x) == 0 {
			out[prefix] = "[]"
		}
		for i, e := range x {
			flatten(prefix+"["+strconv.Itoa(i)+"]", e, out)
		}
	default:
		b, _ := json.Marshal(x)
		out[prefix] = string(b)
	}
}

var indexRe = regexp.MustCompile(`\[\d+\]`)

// diffs lists "step: path" -> "real | sim" for every observable difference.
func diffs(real, got []observed) map[string]string {
	out := map[string]string{}
	for i := range real {
		step := real[i].Step
		if i >= len(got) {
			out[step+": <missing step>"] = ""
			continue
		}
		a, b := map[string]string{}, map[string]string{}
		flatten("", real[i].Object, a)
		flatten("", got[i].Object, b)
		a[".<code>"], b[".<code>"] = strconv.Itoa(real[i].Code), strconv.Itoa(got[i].Code)
		a[".<reason>"], b[".<reason>"] = real[i].Reason, got[i].Reason
		keys := map[string]bool{}
		for k := range a {
			keys[k] = true
		}
		for k := range b {
			keys[k] = true
		}
		for k := range keys {
			if a[k] != b[k] {
				va, vb := a[k], b[k]
				if _, ok := a[k]; !ok {
					va = "<absent>"
				}
				if _, ok := b[k]; !ok {
					vb = "<absent>"
				}
				out[step+": "+indexRe.ReplaceAllString(k, "[]")] = va + "  |  " + vb
			}
		}
	}
	return out
}

// checkRevisions requires every list's version to be at least as new as its
// items. With monotonic set, the versions that steps return as their own object
// (the newest each time) must never decrease.
func checkRevisions(t *testing.T, name string, steps []observed, monotonic bool) {
	t.Helper()
	last := 0
	for _, o := range steps {
		m, ok := o.Object.(map[string]any)
		if !ok {
			continue
		}
		md, _ := m["metadata"].(map[string]any)
		n, err := strconv.Atoi(fmt.Sprint(md["resourceVersion"]))
		if err != nil {
			continue
		}
		if items, ok := m["items"].([]any); ok {
			for _, item := range items {
				im, _ := item.(map[string]any)
				imd, _ := im["metadata"].(map[string]any)
				if in, err := strconv.Atoi(fmt.Sprint(imd["resourceVersion"])); err == nil && in > n {
					t.Errorf("%s: step %s: item version %d is newer than its list's %d", name, o.Step, in, n)
				}
			}
			continue
		}
		if monotonic && n < last {
			t.Errorf("%s: step %s: resourceVersion %d went back from %d", name, o.Step, n, last)
		}
		if n > last {
			last = n
		}
	}
}

func runScenario(t *testing.T, real, simulated *backend, sc scenario) {
	if sc.Skip != "" {
		t.Skip(sc.Skip)
	}
	a := real.play(t, sc)
	b := simulated.play(t, sc)
	na := &normalizer{ns: "", uids: map[string]string{}, rvs: map[string]string{}}
	nb := &normalizer{ns: "", uids: map[string]string{}, rvs: map[string]string{}}
	var ra, rb []observed
	for i := range a {
		ra = append(ra, na.withNS(counter-1).obs(a[i]))
		rb = append(rb, nb.withNS(counter).obs(b[i]))
	}
	if sc.Project != nil {
		for i := range ra {
			ra[i].Object = sc.Project(ra[i].Step, ra[i].Object)
		}
		for i := range rb {
			rb[i].Object = sc.Project(rb[i].Step, rb[i].Object)
		}
	}
	checkRevisions(t, "envtest", a, sc.Monotonic)
	checkRevisions(t, "sim", b, sc.Monotonic)
	d := diffs(ra, rb)
	var unexpected, stale []string
	// "step: *" accepts every difference of a step whose whole result the
	// simulation does not model (an error it never returns).
	whole := func(key string) bool {
		step, _, _ := strings.Cut(key, ": ")
		_, ok := sc.Accepted[step+": *"]
		return ok
	}
	for k, v := range d {
		if _, ok := sc.Accepted[k]; !ok && !whole(k) {
			unexpected = append(unexpected, k+"\n      real | sim: "+v)
		}
	}
	for k := range sc.Accepted {
		if step, field, _ := strings.Cut(k, ": "); field == "*" {
			found := false
			for dk := range d {
				if strings.HasPrefix(dk, step+": ") {
					found = true
				}
			}
			if !found {
				stale = append(stale, k)
			}
		} else if _, ok := d[k]; !ok {
			stale = append(stale, k)
		}
	}
	sort.Strings(unexpected)
	sort.Strings(stale)
	if len(unexpected) > 0 {
		t.Errorf("simulation differs from the real API server:\n  %s", strings.Join(unexpected, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("accepted differences no longer differ (delete them):\n  %s", strings.Join(stale, "\n  "))
	}
	if os.Getenv("SDO_CONFORMANCE_DUMP") != "" {
		dump, _ := json.MarshalIndent(map[string]any{"real": ra, "sim": rb}, "", " ")
		_ = os.WriteFile(os.Getenv("SDO_CONFORMANCE_DUMP")+"/"+strings.ReplaceAll(sc.Name, "/", "_")+".json", dump, 0o644)
	}
}

func (n *normalizer) withNS(i int) *normalizer {
	n.ns = "c" + strconv.Itoa(i)
	return n
}

var _ = reflect.DeepEqual

// watch reads a one-second watch to its end and returns the events in order.
func (b *backend) watch(ctx context.Context, r *run, s step) observed {
	req := b.rest.Get().AbsPath(strings.ReplaceAll(s.Path, "{ns}", r.ns)).Param("watch", "true").Param("timeoutSeconds", "1")
	for k, v := range s.Query {
		if strings.HasPrefix(v, "@") {
			v = r.rv(v[1:])
		}
		req.Param(k, v)
	}
	obs := observed{Step: s.Name, Code: 200}
	stream, err := req.Stream(ctx)
	if err != nil {
		obs.Code, obs.Reason, obs.Object = -1, "TransportError", err.Error()
		return obs
	}
	defer stream.Close()
	var events []any
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 1<<20), 1<<24)
	for scanner.Scan() {
		var event map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			obs.Object = err.Error()
			return obs
		}
		events = append(events, event)
	}
	obs.Object = events
	return obs
}
