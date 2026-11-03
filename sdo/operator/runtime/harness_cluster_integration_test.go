//go:build integration

package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"sdo.dev/operator/runtime/sim"
)

// The harness cluster is sim.Kube served over the simulated API transport, the
// same server TestKubeConformance compares with a real kube-apiserver. Every
// client here is a real client-go client; the fault layer is a RoundTripper in
// front of the simulated server, so a fault is a wire-level event (an error
// status, a lost reply, a dropped or expired watch), not a reactor on a fake.

var (
	errHarnessAPI     = errors.New("simulated API server error")
	errHarnessTimeout = errors.New("simulated API timeout after commit")
	errHarnessDead    = errors.New("dial tcp: connect: connection refused (process is dead)")
)

// incarnation is one process of the controller. Killing it is a kill -9: every
// later request from it fails, its open watch connections break, and nothing it
// had in memory survives. Deferred cleanups still run in the goroutines, but
// they cannot reach the cluster, the agent or the recorder.
type incarnation struct {
	n      int
	dead   atomic.Bool
	cancel func()
	mu     sync.Mutex
	conns  map[io.Closer]struct{}
}

func (i *incarnation) track(c io.Closer) {
	i.mu.Lock()
	if !i.dead.Load() {
		i.conns[c] = struct{}{}
		i.mu.Unlock()
		return
	}
	i.mu.Unlock()
	_ = c.Close()
}

func (i *incarnation) untrack(c io.Closer) {
	i.mu.Lock()
	delete(i.conns, c)
	i.mu.Unlock()
}

// sever breaks every open connection of the process.
func (i *incarnation) sever() {
	i.mu.Lock()
	conns := i.conns
	i.conns = map[io.Closer]struct{}{}
	i.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// crashBoundary names an effect boundary at which a process can die.
type crashBoundary string

const (
	// crashControlWriteBefore: the nth write to the control namespace is never
	// received. crashControlWriteAfter: it commits and the process dies before it
	// sees the reply.
	crashControlWriteBefore crashBoundary = "control-write-before"
	crashControlWriteAfter  crashBoundary = "control-write-after"
	// crashLeaseAfter: the nth Lease write commits and the process dies.
	crashLeaseAfter crashBoundary = "lease-after"
	// crashListBefore: the nth informer list is interrupted by the death.
	crashListBefore crashBoundary = "list-before"
	// crashDispatchBefore: the process dies as it hands the nth incident to the
	// agent. crashDispatchAfter: the agent has acted, the process dies before it
	// reads the answer.
	crashDispatchBefore crashBoundary = "dispatch-before"
	crashDispatchAfter  crashBoundary = "dispatch-after"
)

// crashPoint is one planned death: the ordinal-th crossing of a boundary, counted
// across all incarnations, kills the process that crosses it.
type crashPoint struct {
	Boundary crashBoundary
	Ordinal  int
}

// boundary counts a crossing of b and reports whether the plan kills the
// process there.
func (h *harness) boundary(b crashBoundary) bool {
	if !h.faultsOn() {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.crossings[b]++
	n := h.crossings[b]
	for _, p := range h.plan.crashPoints() {
		if p.Boundary == b && p.Ordinal == n {
			return true
		}
	}
	return false
}

// request is what the fault layer needs to know about one HTTP call.
type requestInfo struct {
	verb, resource, namespace, name string
}

func classify(req *http.Request) requestInfo {
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	var rest []string
	switch {
	case len(parts) >= 3 && parts[0] == "api":
		rest = parts[2:]
	case len(parts) >= 4 && parts[0] == "apis":
		rest = parts[3:]
	default:
		return requestInfo{}
	}
	info := requestInfo{}
	if len(rest) >= 3 && rest[0] == "namespaces" {
		info.namespace, rest = rest[1], rest[2:]
	}
	info.resource = rest[0]
	if len(rest) > 1 {
		info.name = rest[1]
	}
	query := req.URL.Query()
	switch req.Method {
	case http.MethodGet:
		switch {
		case query.Get("watch") == "true" || query.Get("watch") == "1":
			info.verb = "watch"
		case info.name != "":
			info.verb = "get"
		default:
			info.verb = "list"
		}
	case http.MethodPost:
		info.verb = "create"
	case http.MethodPut:
		info.verb = "update"
	case http.MethodPatch:
		info.verb = "patch"
	case http.MethodDelete:
		info.verb = "delete"
	}
	return info
}

func (i requestInfo) key() string {
	return i.verb + "/" + i.resource + "/" + i.namespace + "/" + i.name
}

func (i requestInfo) isWrite() bool {
	switch i.verb {
	case "create", "update", "patch", "delete":
		return true
	}
	return false
}

// faultTransport sits between a client and the simulated server. inc is nil for
// clients that outlive controller restarts (the agent, the environment).
type faultTransport struct {
	h    *harness
	inc  *incarnation
	next http.RoundTripper
}

func (f *faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	h := f.h
	if f.inc != nil && f.inc.dead.Load() {
		return nil, errHarnessDead
	}
	info := classify(req)
	if info.verb == "" || !h.faultsOn() {
		return f.forward(req, info)
	}
	counted := func(prefix string) int {
		h.mu.Lock()
		defer h.mu.Unlock()
		key := prefix + info.key()
		n := h.callCounts[key]
		h.callCounts[key]++
		return n
	}
	seed := h.plan.Seed
	switch {
	case info.verb == "get":
		if chance(seed, info.key(), counted(""), h.plan.ReadErrorPercent) {
			return nil, errHarnessAPI
		}
	case info.verb == "list":
		if chance(seed, "list/"+info.key(), counted("list/"), h.plan.ListErrorPercent) {
			return nil, errHarnessAPI
		}
		if f.inc != nil && h.boundary(crashListBefore) {
			h.kill(f.inc, "list")
			return nil, errHarnessDead
		}
	case info.verb == "watch":
		if req.URL.Query().Get("resourceVersion") != "" && chance(seed, "watch/"+info.key(), counted("watch/"), h.plan.WatchExpiredPercent) {
			// The version the watch resumes from is gone: the client must relist.
			return gone(req), nil
		}
	case info.isWrite():
		if chance(seed, info.key(), counted(""), h.plan.APIErrorPercent) {
			return nil, errHarnessAPI
		}
		control := info.resource == "configmaps" && info.namespace == harnessControlNamespace
		if f.inc != nil && control && h.boundary(crashControlWriteBefore) {
			h.kill(f.inc, "control write dropped")
			return nil, errHarnessDead
		}
		timeout := chance(seed, "t"+info.key(), counted("t"), h.plan.CommitThenTimeoutPercent)
		var crash string
		switch {
		case f.inc != nil && control && h.boundary(crashControlWriteAfter):
			crash = "control write committed"
		case f.inc != nil && info.resource == "leases" && h.boundary(crashLeaseAfter):
			crash = "lease write committed"
		}
		if timeout || crash != "" {
			response, err := f.forward(req, info)
			if err == nil {
				_ = response.Body.Close()
			}
			if crash != "" {
				h.kill(f.inc, crash)
				return nil, errHarnessDead
			}
			return nil, errHarnessTimeout
		}
	}
	return f.forward(req, info)
}

func (f *faultTransport) forward(req *http.Request, info requestInfo) (*http.Response, error) {
	response, err := f.next.RoundTrip(req)
	if err != nil || info.verb != "watch" || response.StatusCode != http.StatusOK {
		return response, err
	}
	body := &trackedBody{ReadCloser: response.Body, inc: f.inc}
	if f.inc != nil {
		f.inc.track(body)
	}
	if f.h.faultsOn() && f.h.plan.WatchCutPercent > 0 {
		n := f.h.nextWatch()
		if chance(f.h.plan.Seed, "cut/"+info.key(), n, f.h.plan.WatchCutPercent) {
			// The connection drops a few seconds in; the client resumes from its
			// last version.
			timer := time.AfterFunc(time.Duration(2+n%7)*time.Second, func() { _ = body.Close() })
			body.setStop(timer.Stop)
		}
	}
	response.Body = body
	return response, nil
}

func (h *harness) nextWatch() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.watches++
	return h.watches
}

type trackedBody struct {
	io.ReadCloser
	inc  *incarnation
	mu   sync.Mutex // guards stop: the cut timer can fire as it is set
	stop func() bool
	once sync.Once
}

func (b *trackedBody) setStop(stop func() bool) {
	b.mu.Lock()
	b.stop = stop
	b.mu.Unlock()
}

func (b *trackedBody) Close() error {
	var err error
	b.once.Do(func() {
		b.mu.Lock()
		stop := b.stop
		b.mu.Unlock()
		if stop != nil {
			stop()
		}
		if b.inc != nil {
			b.inc.untrack(b)
		}
		err = b.ReadCloser.Close()
	})
	return err
}

// gone is the server's answer to a watch from a version it no longer holds.
func gone(req *http.Request) *http.Response {
	status := apierrors.NewResourceExpired("too old resource version").Status()
	status.TypeMeta.Kind, status.TypeMeta.APIVersion = "Status", "v1"
	data, _ := json.Marshal(status)
	return &http.Response{StatusCode: http.StatusGone, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data))), Request: req}
}

// clientFor is a real client-go client over the simulated server, behind the
// fault layer. inc binds it to one controller process; nil makes it permanent.
func (h *harness) clientFor(agent string, inc *incarnation, faulty bool) kubernetes.Interface {
	config := &rest.Config{QPS: -1, UserAgent: agent}
	if faulty {
		config.WrapTransport = func(rt http.RoundTripper) http.RoundTripper { return &faultTransport{h: h, inc: inc, next: rt} }
	}
	client, err := h.world.ClientFor(config)
	if err != nil {
		h.t.Fatal(err)
	}
	return client
}

// kill is a kill -9 of one controller process.
func (h *harness) kill(inc *incarnation, why string) {
	if !inc.dead.CompareAndSwap(false, true) {
		return
	}
	h.t.Logf("%s crash: %s", h.now().Sub(h.start).Round(time.Millisecond), why)
	h.rec.Add(sim.ControllerDown{At: h.now()})
	inc.sever()
	inc.cancel()
}

// current is the live controller process, if any.
func (h *harness) current() *incarnation {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.inc
}

func describeCrashes(points []crashPoint) string {
	var parts []string
	for _, p := range points {
		parts = append(parts, fmt.Sprintf("%s#%d", p.Boundary, p.Ordinal))
	}
	return strings.Join(parts, ",")
}
