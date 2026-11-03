package sim

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

// watchEvent is one change in resourceVersion order.
type watchEvent struct {
	Type    string
	Object  *unstructured.Unstructured
	version int
}

// watchLog is the history of every committed change and the live watchers. A
// watch replays the history after its resourceVersion, then streams new events.
type watchLog struct {
	mu       sync.Mutex
	events   []watchEvent
	watchers map[*watcher]struct{}
}

type watcher struct {
	info      kindInfo
	namespace string
	selector  labels.Selector
	fields    fields.Selector
	out       chan watchEvent
}

func (l *watchLog) publish(kind string, object *unstructured.Unstructured, version int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	event := watchEvent{Type: kind, Object: object.DeepCopy(), version: version}
	l.events = append(l.events, event)
	for w := range l.watchers {
		if w.matches(event.Object) {
			select {
			case w.out <- event:
			default:
				// A watcher that cannot keep up is cut off, as the real server
				// does; its client relists.
				delete(l.watchers, w)
				close(w.out)
			}
		}
	}
}

func (w *watcher) matches(o *unstructured.Unstructured) bool {
	if o.GetKind() != w.info.kind || o.GetAPIVersion() != w.info.groupVersion() || (w.namespace != "" && o.GetNamespace() != w.namespace) {
		return false
	}
	if !w.selector.Matches(labels.Set(o.GetLabels())) {
		return false
	}
	set := fields.Set{"metadata.name": o.GetName(), "metadata.namespace": o.GetNamespace()}
	if phase, ok, _ := unstructured.NestedString(o.Object, "status", "phase"); ok {
		set["status.phase"] = phase
	}
	return w.fields.Matches(set)
}

// record publishes a write that did not go through the HTTP handlers (a scenario
// driver's Put, an executor's Patch) so watchers see every change.
func (k *Kube) record(object *unstructured.Unstructured, existed bool) {
	kind := "MODIFIED"
	if !existed {
		kind = "ADDED"
	}
	k.server().log.publish(kind, object, k.version)
}

// serveWatch streams events as the server's watch does: the history after the
// requested version (or the current objects as ADDED when none is given), then
// each new change, until the client goes away or timeoutSeconds passes.
func (s *apiServer) serveWatch(r request, req *http.Request) *http.Response {
	selector, err := labels.Parse(r.q("labelSelector"))
	if err != nil {
		selector = labels.Everything()
	}
	fieldSel, err := fields.ParseSelector(r.q("fieldSelector"))
	if err != nil {
		fieldSel = fields.Everything()
	}
	w := &watcher{info: r.info, namespace: r.namespace, selector: selector, fields: fieldSel, out: make(chan watchEvent, 1024)}
	from, haveFrom := 0, false
	if rv := r.q("resourceVersion"); rv != "" && rv != "0" {
		from, _ = strconv.Atoi(rv)
		haveFrom = true
	}
	var replay []watchEvent
	s.kube.mu.Lock()
	s.log.mu.Lock()
	if haveFrom {
		for _, e := range s.log.events {
			if e.version > from && w.matches(e.Object) {
				replay = append(replay, e)
			}
		}
	} else {
		for _, o := range s.kube.objectsLocked() {
			if w.matches(o) {
				replay = append(replay, watchEvent{Type: "ADDED", Object: o})
			}
		}
	}
	if s.log.watchers == nil {
		s.log.watchers = map[*watcher]struct{}{}
	}
	s.log.watchers[w] = struct{}{}
	s.log.mu.Unlock()
	s.kube.mu.Unlock()

	pipeReader, writer := io.Pipe()
	reader := &watchBody{PipeReader: pipeReader, closed: make(chan struct{})}
	ctx := req.Context()
	timeout := make(<-chan time.Time)
	if secs, err := strconv.Atoi(r.q("timeoutSeconds")); err == nil && secs > 0 {
		timer := time.NewTimer(time.Duration(secs) * time.Second)
		timeout = timer.C
	}
	go func() {
		defer func() {
			s.log.mu.Lock()
			delete(s.log.watchers, w)
			s.log.mu.Unlock()
			_ = writer.Close()
		}()
		encoder := json.NewEncoder(writer)
		send := func(e watchEvent) bool {
			return encoder.Encode(map[string]any{"type": e.Type, "object": e.Object.Object}) == nil
		}
		for _, e := range replay {
			if !send(e) {
				return
			}
		}
		for {
			select {
			case e, open := <-w.out:
				if !open {
					return
				}
				if lag := s.kube.watchLag(); lag > 0 {
					if err := waitFor(ctx, reader.closed, lag); err != nil {
						return
					}
				}
				if !send(e) {
					return
				}
			case <-ctx.Done():
				return
			case <-reader.closed:
				return
			case <-timeout:
				return
			}
		}
	}()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: reader, Request: req}
}

// waitFor sleeps d or until ctx ends.
func waitFor(ctx context.Context, closed <-chan struct{}, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-closed:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// watchBody is the response body of a watch; closing it ends the stream, as a
// client dropping the connection does.
type watchBody struct {
	*io.PipeReader
	closed chan struct{}
	once   sync.Once
}

func (b *watchBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.PipeReader.Close()
}
