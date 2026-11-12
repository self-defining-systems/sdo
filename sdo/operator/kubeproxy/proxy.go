// Package kubeproxy adapts Kubernetes HTTP traffic to transport-neutral history.
package kubeproxy

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"mime"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/kubeproxy/internal/proxyio"
)

type Recorder interface {
	Record(apihistory.Event, []byte)
	Gap()
}

// Admitter is implemented by a Recorder that can guarantee a write's record before the write is forwarded.
type Admitter interface {
	// Admit reserves the record of one write-class request. When it fails the request must not be forwarded.
	Admit() error
}

// Config is the policy of one recording proxy.
type Config struct {
	// Recorder stores history. A recorder that is also an Admitter makes every write, exec, attach and
	// port-forward fail closed: it is refused with 503 unless its record is reserved first.
	Recorder Recorder
	// Gate is the incident mutation fence; nil for scopes without one.
	Gate *apihistory.MutationGate
	// FieldManager, when set, replaces the fieldManager of every write the proxy forwards.
	FieldManager string
	// ReadOnly refuses writes and exec, attach and port-forward sessions with 403, recording each refusal.
	ReadOnly bool
}

// New forwards request and response bytes through the incident connection pool.
// Mutation requests cannot opt into transport retries using idempotency headers.
func New(upstream *url.URL, readTransport, writeTransport http.RoundTripper, recorder Recorder) http.Handler {
	return NewRecording(upstream, readTransport, writeTransport, Config{Recorder: recorder})
}

// NewWithMutationGate keeps the incident fence below all worker API callers,
// including raw kubectl and GET-based exec/attach upgrades.
func NewWithMutationGate(upstream *url.URL, readTransport, writeTransport http.RoundTripper, recorder Recorder, gate *apihistory.MutationGate) http.Handler {
	return NewRecording(upstream, readTransport, writeTransport, Config{Recorder: recorder, Gate: gate})
}

// NewRecording is the proxy every agent Job reaches Kubernetes through.
func NewRecording(upstream *url.URL, readTransport, writeTransport http.RoundTripper, config Config) http.Handler {
	recorder, gate := config.Recorder, config.Gate
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e := requestEvent(r)
		var uncertain atomic.Bool
		session := e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward"
		write := mutating(r.Method) || session
		if write && config.ReadOnly && !nonPersistingReview(e) {
			refuseReadOnly(w, recorder, e)
			return
		}
		if write && recorder != nil {
			if admitter, ok := recorder.(Admitter); ok {
				if err := admitter.Admit(); err != nil {
					writeStatus(w, http.StatusServiceUnavailable, "ServiceUnavailable", "recording_unavailable",
						"SDO cannot record this request, so it was not forwarded")
					return
				}
				e.Reserved = true
			}
		}
		if write && !session && config.FieldManager != "" {
			e.FieldManager = config.FieldManager
		}
		if session {
			query := r.URL.Query()
			e.Container = query.Get("container")
			e.Ports = apihistory.SafePorts(query["ports"])
			if e.Subresource != "portforward" {
				e.Command = apihistory.RedactCommand(query["command"])
			}
		}
		// The transport can still be probing the request body for EOF when an
		// upstream response arrives. HTTP/1's default response flush drains and
		// closes that same body, racing the probe and aborting response copying.
		// HTTP/2 already permits concurrent reads and writes; custom writers may
		// not expose this optional control.
		// Enabled only after the early refusals above: a refused request leaves its body unread, which full
		// duplex forbids the server to drain.
		_ = http.NewResponseController(w).EnableFullDuplex()
		var requestBody *bodyPrefix
		if recorder != nil && mutating(r.Method) && !session && r.Body != nil && r.Body != http.NoBody {
			requestBody = &bodyPrefix{ReadCloser: r.Body}
			r.Body = requestBody
		}
		if gate != nil && write {
			lease, err := gate.Begin()
			if err != nil {
				warning, _ := gate.Verified()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusConflict)
				_ = json.NewEncoder(w).Encode(struct {
					Kind             string                       `json:"kind"`
					APIVersion       string                       `json:"apiVersion"`
					Status           string                       `json:"status"`
					Reason           string                       `json:"reason"`
					Message          string                       `json:"message"`
					Code             int                          `json:"code"`
					IncidentVerified *apihistory.IncidentVerified `json:"incident_verified,omitempty"`
				}{"Status", "v1", "Failure", err.Error(), "incident mutation rejected", 409, warning})
				return
			}
			defer func() { _ = lease.Finish(uncertain.Load()) }()
		}
		record := func(event apihistory.Event, body []byte) {
			if recorder != nil {
				recorder.Record(event, body)
			}
		}
		var completed *apihistory.Event
		var responseBody []byte
		// Projection starts after forwarding returns. Store.Record schedules bounded
		// workers; raw bytes never enter its recording queue.
		defer func() {
			if completed != nil {
				if completed.Status == http.StatusSwitchingProtocols {
					// ServeHTTP returns only after the upgraded stream closes.
					completed.SessionEnd = proxyio.Now().UTC()
				}
				record(*completed, responseBody)
			}
		}()
		recordEnd := func(event apihistory.Event, body []byte) { completed = &event; responseBody = body }
		record(e, nil)
		if write && !session && recorder != nil && r.Method != http.MethodPost {
			recordBeforeState(r, e, upstream, readTransport, record)
		}
		transport := readTransport
		if mutating(r.Method) {
			transport = writeTransport
		}
		proxy := httputil.NewSingleHostReverseProxy(upstream)
		proxy.Transport = transport
		proxy.ErrorLog = log.New(io.Discard, "", 0)
		proxy.FlushInterval = -1
		director := proxy.Director
		proxy.Director = func(req *http.Request) {
			director(req)
			req.Host = upstream.Host
			req.GetBody = nil
			if mutating(req.Method) {
				req.Header.Del("Idempotency-Key")
				req.Header.Del("X-Idempotency-Key")
			}
			req.Header.Del("Authorization") // upstream wrapper supplies the Pod service-account token
			if e.FieldManager != "" {
				query := req.URL.Query()
				query.Set("fieldManager", e.FieldManager)
				req.URL.RawQuery = query.Encode()
			}
		}
		annotate := func(end *apihistory.Event) {
			if requestBody == nil {
				return
			}
			if paths, ok := requestBody.changedPaths(e.Resource, r.Header.Get("Content-Type")); ok {
				end.ChangedPaths = paths
			} else {
				end.PathsOmitted = true
			}
		}
		proxy.ModifyResponse = func(res *http.Response) error {
			end := e
			end.Phase = "end"
			end.Status = res.StatusCode
			annotate(&end)
			// A 101 Body is bidirectional. Leave it intact for ReverseProxy's upgrade
			// path, including SPDY and WebSocket frames.
			if res.StatusCode == http.StatusSwitchingProtocols {
				end.Finished = proxyio.Now().UTC()
				end.Omission = "upgrade_metadata_only"
				recordEnd(end, nil)
				return nil
			}
			mode := "json"
			switch {
			case !supportedRepresentation(res.Header):
				mode = "metadata"
			case e.Verb == "watch":
				mode = "metadata"
			case e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward":
				mode = "metadata"
			case e.Subresource == "log":
				mode = "metadata"
			case e.Resource == "serviceaccounts" && e.Subresource == "token":
				mode = "metadata"
			}
			if mode == "metadata" {
				end.Omission = "redacted_stream_metadata_only"
				if !supportedRepresentation(res.Header) {
					end.Omission = "unsupported_representation"
				}
				if e.Subresource == "log" {
					end.Omission = "redacted_log_metadata_only"
				}
			}
			res.Body = &captureBody{ReadCloser: res.Body, mode: mode, finish: func(body []byte, tooLarge, broken bool) {
				end.Finished = proxyio.Now().UTC()
				if broken {
					uncertain.Store(true)
					end.Omission = "incomplete_response"
					body = nil
					if recorder != nil {
						recorder.Gap()
					}
				}
				if tooLarge {
					end.Partial = true
					end.Omission = "redacted_projection_budget"
					body = nil
					if recorder != nil {
						recorder.Gap()
					}
				}
				recordEnd(end, body)
			}}
			return nil
		}
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			uncertain.Store(true)
			end := e
			end.Phase = "end"
			end.Status = http.StatusBadGateway
			annotate(&end)
			end.Finished = proxyio.Now().UTC()
			end.Omission = "upstream_error"
			recordEnd(end, nil)
			// Transport errors can contain URLs. Do not echo them into history or the
			// response, especially credential-bearing query arguments.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"BadGateway","message":"Kubernetes upstream unavailable","code":502}`)
		}
		proxy.ServeHTTP(w, r)
	})
}

// bodyPrefix keeps the first bytes of a request body for field-path extraction while the request streams on.
// The bytes are never stored; only the key paths derived from them are.
type bodyPrefix struct {
	io.ReadCloser
	mu       sync.Mutex
	prefix   []byte
	overflow bool
}

func (b *bodyPrefix) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > 0 && !b.overflow {
		if len(b.prefix)+n > apihistory.MaxRequestBodyBytes {
			b.prefix, b.overflow = nil, true
		} else {
			b.prefix = append(b.prefix, p[:n]...)
		}
	}
	return n, err
}

func (b *bodyPrefix) changedPaths(resource, contentType string) ([]string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.overflow {
		return nil, false
	}
	return apihistory.ChangedPaths(resource, contentType, b.prefix)
}

// nonPersistingReview reports creates that only ask the API server a question, so a read-only scope may use them.
func nonPersistingReview(e apihistory.Event) bool {
	if e.Verb != "post" || e.Subresource != "" || (e.Group != "authorization.k8s.io" && e.Group != "authentication.k8s.io") {
		return false
	}
	switch e.Resource {
	case "subjectaccessreviews", "selfsubjectaccessreviews", "localsubjectaccessreviews", "selfsubjectrulesreviews", "selfsubjectreviews", "tokenreviews":
		return true
	}
	return false
}

func refuseReadOnly(w http.ResponseWriter, recorder Recorder, e apihistory.Event) {
	if recorder != nil {
		recorder.Record(e, nil)
		end := e
		end.Phase, end.Status, end.Omission = "end", http.StatusForbidden, "scope_read_only"
		end.Finished = proxyio.Now().UTC()
		recorder.Record(end, nil)
	}
	writeStatus(w, http.StatusForbidden, "Forbidden", "scope_read_only", "this agent scope is read-only at the SDO API proxy")
}

func writeStatus(w http.ResponseWriter, code int, reason, message, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Kind       string `json:"kind"`
		APIVersion string `json:"apiVersion"`
		Status     string `json:"status"`
		Reason     string `json:"reason"`
		Message    string `json:"message"`
		Code       int    `json:"code"`
	}{"Status", "v1", "Failure", reason, message + ": " + detail, code})
}

func mutating(method string) bool { return method != "GET" && method != "HEAD" && method != "OPTIONS" }
func requestEvent(r *http.Request) apihistory.Event {
	var id [16]byte
	proxyio.Read(id[:])
	e := apihistory.Event{RequestID: hex.EncodeToString(id[:]), Phase: "start", Started: proxyio.Now().UTC(), Verb: strings.ToLower(r.Method)}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) >= 2 && parts[0] == "api" {
		e.Version = parts[1]
		parts = parts[2:]
	} else if len(parts) >= 3 && parts[0] == "apis" {
		e.Group = parts[1]
		e.Version = parts[2]
		parts = parts[3:]
	} else {
		return e
	}
	if len(parts) > 0 && parts[0] == "watch" {
		e.Verb = "watch"
		parts = parts[1:]
	}
	namespaceSubresource := len(parts) == 3 && (parts[2] == "status" || parts[2] == "finalize")
	if len(parts) >= 3 && parts[0] == "namespaces" && !namespaceSubresource {
		e.Namespace = parts[1]
		parts = parts[2:]
	}
	if len(parts) > 0 {
		e.Resource = parts[0]
	}
	if len(parts) > 1 {
		e.Name = parts[1]
	}
	if len(parts) > 2 {
		e.Subresource = parts[2]
	}
	if r.Method == "GET" && e.Name == "" && e.Verb != "watch" {
		e.Verb = "list"
	}
	if queryTrue(r, "watch") {
		e.Verb = "watch"
	}
	// Only recognized dry-run writes can move the mutation boundary. Connection
	// subresources never acquire dry-run semantics from a query parameter.
	values := r.URL.Query()["dryRun"]
	e.DryRun = mutating(r.Method) && len(values) == 1 && values[0] == "All" &&
		(e.Subresource == "" || e.Subresource == "status" || e.Subresource == "scale" || e.Subresource == "finalize")
	e.Partial = r.URL.Query().Get("labelSelector") != "" || r.URL.Query().Get("fieldSelector") != "" || r.URL.Query().Get("continue") != "" || r.URL.Query().Get("limit") != ""
	return e
}

type captureBody struct {
	io.ReadCloser
	mode     string
	body     []byte
	tooLarge bool
	eof      bool
	broken   bool
	once     sync.Once
	finish   func([]byte, bool, bool)
}

func (b *captureBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		switch b.mode {
		case "json":
			if !b.tooLarge {
				if len(b.body)+n > apihistory.MaxProjectionBytes {
					b.body = nil
					b.tooLarge = true
				} else {
					b.body = append(b.body, p[:n]...)
				}
			}
		}
	}
	if err == io.EOF {
		b.eof = true
		b.complete()
	} else if err != nil {
		b.broken = true
		b.complete()
	}
	return n, err
}
func (b *captureBody) complete() {
	b.once.Do(func() { b.finish(b.body, b.tooLarge, b.broken || !b.eof) })
}
func (b *captureBody) Close() error { err := b.ReadCloser.Close(); b.complete(); return err }

func queryTrue(r *http.Request, name string) bool {
	value, _ := strconv.ParseBool(r.URL.Query().Get(name))
	return value
}

// Only declared JSON with identity encoding reaches the body projector. Header
// values are inspected in memory, never persisted or echoed in diagnostics.
func supportedRepresentation(header http.Header) bool {
	if len(header.Values("Content-Type")) != 1 {
		return false
	}
	mediaType, params, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return false
	}
	for key, value := range params {
		if key != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	encodings := header.Values("Content-Encoding")
	return len(encodings) == 0 || (len(encodings) == 1 && strings.EqualFold(strings.TrimSpace(encodings[0]), "identity"))
}

// beforeStateLimit bounds the object the proxy keeps as a write's "before" evidence.
const beforeStateLimit = 1 << 20

// recordBeforeState reads the object a write is about to change and records it (phase "before", the write's request
// ID) so every write path has the evidence a plan application captures first. A create has no prior object; a read
// that fails or is too large records nothing, and never blocks or alters the write.
func recordBeforeState(r *http.Request, e apihistory.Event, upstream *url.URL, transport http.RoundTripper, record func(apihistory.Event, []byte)) {
	if e.Name == "" {
		return
	}
	target := *upstream
	target.Path, target.RawPath = r.URL.Path, r.URL.RawPath
	read, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		return
	}
	read.Header.Set("Accept", "application/json")
	response, err := transport.RoundTrip(read)
	if err != nil {
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, beforeStateLimit+1))
	if err != nil || len(body) > beforeStateLimit || response.StatusCode != http.StatusOK {
		return
	}
	before := e
	before.Phase, before.Verb, before.Status = "before", "get", response.StatusCode
	before.Reserved = false
	before.Finished = proxyio.Now().UTC()
	record(before, body)
}
