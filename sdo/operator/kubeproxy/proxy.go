// Package kubeproxy adapts Kubernetes HTTP traffic to transport-neutral history.
package kubeproxy

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e := requestEvent(r)
		record := func(event apihistory.Event, body []byte) {
			if recorder != nil {
				recorder.Record(event, body)
			}
		}
		record(e, nil)
		transport := readTransport
		if mutating(r.Method) {
			transport = writeTransport
		}
		proxy := httputil.NewSingleHostReverseProxy(upstream)
		proxy.Transport = transport
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
				record(end, nil)
				return nil
			}
			mode := "json"
			switch {
			case r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1":
				mode = "metadata"
			case e.Subresource == "exec" || e.Subresource == "attach" || e.Subresource == "portforward":
				mode = "metadata"
			case e.Subresource == "log" && (r.URL.Query().Get("follow") == "true" || r.URL.Query().Get("follow") == "1"):
				mode = "metadata"
			case e.Subresource == "log":
				mode = "tail"
				end.BodyFormat = "log_tail"
			case e.Resource == "serviceaccounts" && e.Subresource == "token":
				mode = "metadata"
			}
			if mode == "metadata" {
				end.Omission = "stream_metadata_only"
			}
			res.Body = &captureBody{ReadCloser: res.Body, mode: mode, finish: func(body []byte, tooLarge, broken bool) {
				end.Finished = time.Now().UTC()
				if broken {
					end.Omission = "incomplete_response"
					body = nil
					if recorder != nil {
						recorder.Gap()
					}
				}
				if tooLarge {
					end.Partial = true
					end.Omission = "body_cap"
					if end.BodyFormat == "log_tail" {
						end.Omission = "log_tail_capped"
					} else {
						body = nil
					}
					if recorder != nil {
						recorder.Gap()
					}
				}
				record(end, body)
			}}
			return nil
		}
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			end := e
			end.Phase = "end"
			end.Status = http.StatusBadGateway
			end.Finished = time.Now().UTC()
			end.Omission = "upstream_error"
			record(end, nil)
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
	e := apihistory.Event{RequestID: hex.EncodeToString(id[:]), Phase: "start", Started: time.Now().UTC(), Verb: strings.ToLower(r.Method), DryRun: r.URL.Query().Get("dryRun") != ""}
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
	if len(parts) >= 3 && parts[0] == "namespaces" {
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
	if r.Method == "GET" && e.Name == "" {
		e.Verb = "list"
	}
	if r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1" {
		e.Verb = "watch"
	}
	e.Partial = r.URL.Query().Get("labelSelector") != "" || r.URL.Query().Get("fieldSelector") != "" || r.URL.Query().Get("continue") != "" || r.URL.Query().Get("limit") != ""
	return e
}

type captureBody struct {
	seen int64
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
				if len(b.body)+n > apihistory.MaxBodyBytes {
					b.body = nil
					b.tooLarge = true
				} else {
					b.body = append(b.body, p[:n]...)
				}
			}
		case "tail":
			b.seen += int64(n)
			if b.seen > apihistory.MaxLogBytes {
				b.tooLarge = true
			}
			if n >= apihistory.MaxLogBytes {
				b.body = append(b.body[:0], p[n-apihistory.MaxLogBytes:n]...)
			} else {
				if len(b.body)+n > apihistory.MaxLogBytes {
					b.body = append(b.body[:0], b.body[len(b.body)+n-apihistory.MaxLogBytes:]...)
				}
				b.body = append(b.body, p[:n]...)
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

// ValidateListen restricts this unauthenticated proxy to the responder Pod's
// loopback interface. It is never a controller Service.
func ValidateListen(address string) error {
	if address != "127.0.0.1:18080" {
		return fmt.Errorf("proxy must listen on 127.0.0.1:18080")
	}
	return nil
}
