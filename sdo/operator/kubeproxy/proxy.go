// Package kubeproxy adapts Kubernetes HTTP traffic to transport-neutral history.
package kubeproxy

import (
	"crypto/rand"
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
	"time"

	"sdo.dev/operator/apihistory"
)

type Recorder interface {
	Record(apihistory.Event, []byte)
	Gap()
}

// New forwards the original request and response bytes. writeTransport must use
// fresh connections: net/http can retry a reused connection with idempotency
// headers even for POST. The production adapter disables keepalives for writes.
func New(upstream *url.URL, readTransport, writeTransport http.RoundTripper, recorder Recorder) http.Handler {
	return NewWithMutationGate(upstream, readTransport, writeTransport, recorder, nil)
}

// NewWithMutationGate keeps the incident fence below all worker API callers,
// including raw kubectl and GET-based exec/attach upgrades.
func NewWithMutationGate(upstream *url.URL, readTransport, writeTransport http.RoundTripper, recorder Recorder, gate *apihistory.MutationGate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The transport can still be probing the request body for EOF when an
		// upstream response arrives. HTTP/1's default response flush drains and
		// closes that same body, racing the probe and aborting response copying.
		// HTTP/2 already permits concurrent reads and writes; custom writers may
		// not expose this optional control.
		_ = http.NewResponseController(w).EnableFullDuplex()
		e := requestEvent(r)
		var uncertain atomic.Bool
		if gate != nil && (mutating(r.Method) || e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward") {
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
				record(*completed, responseBody)
			}
		}()
		recordEnd := func(event apihistory.Event, body []byte) { completed = &event; responseBody = body }
		record(e, nil)
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
			req.Header.Del("Authorization") // upstream wrapper supplies the Pod service-account token
		}
		proxy.ModifyResponse = func(res *http.Response) error {
			end := e
			end.Phase = "end"
			end.Status = res.StatusCode
			// A 101 Body is bidirectional. Leave it intact for ReverseProxy's upgrade
			// path, including SPDY and WebSocket frames.
			if res.StatusCode == http.StatusSwitchingProtocols {
				end.Finished = time.Now().UTC()
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
				end.Finished = time.Now().UTC()
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
			end.Finished = time.Now().UTC()
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
func mutating(method string) bool { return method != "GET" && method != "HEAD" && method != "OPTIONS" }
func requestEvent(r *http.Request) apihistory.Event {
	var id [16]byte
	_, _ = rand.Read(id[:])
	e := apihistory.Event{RequestID: hex.EncodeToString(id[:]), Phase: "start", Started: time.Now().UTC(), Verb: strings.ToLower(r.Method)}
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
