package kubeproxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sdo.dev/operator/apihistory"
)

type simRoundTripper func(*http.Request) (*http.Response, error)

func (f simRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type simResponseBody struct {
	raw    []byte
	chunk  int
	fault  bool
	closed bool
}

func (b *simResponseBody) Read(p []byte) (int, error) {
	if len(b.raw) > 0 {
		n := copy(p, b.raw[:min(len(b.raw), b.chunk)])
		b.raw = b.raw[n:]
		return n, nil
	}
	if b.fault {
		return 0, errors.New("malformed MIME header: DP1_SYNTHETIC_TRAILER_CANARY")
	}
	return 0, io.EOF
}
func (b *simResponseBody) Close() error { b.closed = true; return nil }

type simForwardResult struct {
	status  int
	header  http.Header
	body    []byte
	aborted bool
	calls   int32
}

func simForward(t *testing.T, recorder Recorder, method, path string, status, chunk int, payload []byte, fault byte, entered, release chan struct{}) simForwardResult {
	t.Helper()
	var calls atomic.Int32
	var upstreamBody *simResponseBody
	transport := simRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if string(requestBody) != "DP1_SYNTHETIC_REQUEST_CANARY" || r.Method != method || r.URL.RequestURI() != path {
			t.Errorf("request changed: %s %s %q", r.Method, r.URL.RequestURI(), requestBody)
		}
		if r.Header.Get("Idempotency-Key") != "synthetic" {
			t.Error("idempotency header lost")
		}
		if entered != nil {
			close(entered)
			<-release
		}
		if fault == 1 {
			return nil, errors.New("connection reset DP1_SYNTHETIC_RESET_CANARY")
		}
		upstreamBody = &simResponseBody{raw: append([]byte(nil), payload...), chunk: chunk, fault: fault == 2}
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}, "X-Upstream": {"first", "second"}, "Set-Cookie": {"a=one", "b=two"}}, Body: upstreamBody, ContentLength: -1}, nil
	})
	upstream, _ := url.Parse("http://synthetic.invalid")
	req := httptest.NewRequest(method, path, strings.NewReader("DP1_SYNTHETIC_REQUEST_CANARY"))
	req.Header.Set("Authorization", "Bearer DP1_SYNTHETIC_AUTH_CANARY")
	req.Header.Set("Idempotency-Key", "synthetic")
	// With ServerContextKey set, body-copy errors must abort the downstream stream.
	req = req.WithContext(context.WithValue(req.Context(), http.ServerContextKey, &http.Server{}))
	out := httptest.NewRecorder()
	aborted := false
	func() {
		defer func() {
			if p := recover(); p != nil {
				if p != http.ErrAbortHandler {
					panic(p)
				}
				aborted = true
			}
		}()
		New(upstream, transport, transport, recorder).ServeHTTP(out, req)
	}()
	if calls.Load() != 1 {
		t.Fatalf("%s retried %d times", method, calls.Load())
	}
	if upstreamBody != nil && !upstreamBody.closed {
		t.Error("upstream body not closed")
	}
	return simForwardResult{out.Code, out.Header().Clone(), append([]byte(nil), out.Body.Bytes()...), aborted, calls.Load()}
}

// ReverseProxy suppresses ErrAbortHandler outside a server; model real serving.
func runProxySchedule(t *testing.T, seed int64, schedule []byte) {
	t.Helper()
	schedule = schedule[:min(len(schedule), 48)]
	rng := rand.New(rand.NewSource(seed))
	choice := func(i, n int) int {
		if i < len(schedule) {
			return int(schedule[i]) % n
		}
		return rng.Intn(n)
	}
	dir := filepath.Join(t.TempDir(), "history")
	limits := apihistory.Limits{Queue: 1 + choice(0, 32), Events: uint64(8 + choice(1, 80)), Bytes: int64(2048 + choice(2, 16000))}
	methods := []string{"GET", "GET", "GET", "PATCH", "POST", "POST", "GET", "GET", "POST", "GET"}
	paths := []string{"/api/v1/namespaces/app/pods/p", "/api/v1/namespaces/app/pods?limit=1&continue=DP1_SYNTHETIC_QUERY_CANARY", "/api/v1/namespaces/app/pods?labelSelector=private%3DDP1_SYNTHETIC_QUERY_CANARY", "/api/v1/namespaces/app/pods/p?dryRun=All", "/api/v1/namespaces/app/pods", "/api/v1/namespaces/app/pods/p?dryRun=All", "/api/v1/namespaces/app/pods?watch=true", "/api/v1/namespaces/app/pods/p/log?follow=true", "/api/v1/namespaces/app/pods/p/exec?dryRun=All&command=DP1_SYNTHETIC_QUERY_CANARY", "/api/v1/watch/namespaces/app/pods"}
	statuses := []int{200, 201, 204, 400, 404, 409, 429, 500, 503}
	for epoch := 0; epoch < 2; epoch++ {
		lease, err := apihistory.BeginWriter(dir)
		if err != nil {
			t.Fatal(err)
		}
		store, err := apihistory.Open(dir, "synthetic", limits)
		if err != nil {
			lease.Close()
			t.Fatal(err)
		}
		// Fail storage before serving. Forwarding must match the no-recorder oracle.
		blobFault := false
		switch choice(3+epoch, 4) {
		case 1:
			if err := os.Rename(filepath.Join(dir, "blobs"), dir+".held-blobs"); err != nil {
				t.Fatal(err)
			}
			blobFault = true
		case 2:
			if err := os.Mkdir(filepath.Join(dir, "manifest.tmp"), 0700); err != nil {
				t.Fatal(err)
			}
		case 3:
			store.Gap()
		}
		for wave := 0; wave < 2; wave++ {
			type pending struct {
				entered, release chan struct{}
				done             chan simForwardResult
				want             simForwardResult
				operation        int
				status, fault    int
				payload          []byte
			}
			batch := make([]pending, 4)
			for i := range batch {
				index := epoch*16 + wave*4 + i
				operation := choice(5+index, len(paths))
				status := statuses[choice(21+index, len(statuses))]
				fault := byte(choice(37+index, 3))
				payload := []byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"app","resourceVersion":"1"},"spec":{"token":"DP1_SYNTHETIC_BODY_CANARY"}}`)
				if operation == 1 || operation == 2 {
					payload = []byte(`{"apiVersion":"v1","kind":"PodList","metadata":{"continue":"DP1_SYNTHETIC_BODY_CANARY"},"items":[]}`)
				}
				if choice(9+index, 16) == 0 {
					payload = bytes.Repeat([]byte("DP1_SYNTHETIC_BODY_CANARY"), 12000)
				}
				chunk := 1 + choice(13+index, 257)
				batch[i] = pending{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan simForwardResult, 1), operation: operation, status: status, fault: int(fault), payload: payload}
				batch[i].want = simForward(t, nil, methods[operation], paths[operation], status, chunk, payload, fault, nil, nil)
				p := &batch[i]
				go func() {
					p.done <- simForward(t, store, methods[p.operation], paths[p.operation], p.status, chunk, p.payload, byte(p.fault), p.entered, p.release)
				}()
			}
			for i := range batch {
				select {
				case <-batch[i].entered:
				case <-time.After(3 * time.Second):
					t.Fatal("request blocked before upstream")
				}
			}
			for _, i := range rng.Perm(len(batch)) {
				close(batch[i].release)
				var got simForwardResult
				select {
				case got = <-batch[i].done:
				case <-time.After(3 * time.Second):
					t.Fatal("storage blocked forwarding")
				}
				if !reflect.DeepEqual(got, batch[i].want) {
					t.Fatalf("seed=%d schedule=%x epoch=%d wave=%d request=%d transparency: got=%+v want=%+v", seed, schedule, epoch, wave, i, got, batch[i].want)
				}
				// Reflection can read while other upstream requests are still blocked.
				if h, err := apihistory.Read(dir); err != nil || h.Manifest.Complete {
					t.Fatalf("live read: %+v %v", h.Manifest, err)
				}
			}
		}
		store.Close()
		lease.Close()
		// Repair only fault fixtures between epochs; production restart does recovery.
		if info, err := os.Stat(filepath.Join(dir, "manifest.tmp")); err == nil && info.IsDir() {
			if err := os.Remove(filepath.Join(dir, "manifest.tmp")); err != nil {
				t.Fatal(err)
			}
		}
		if blobFault {
			if err := os.Rename(dir+".held-blobs", filepath.Join(dir, "blobs")); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.MkdirAll(filepath.Join(dir, "blobs"), 0700); err != nil {
			t.Fatal(err)
		}
		h, err := apihistory.Read(dir)
		if err != nil {
			t.Fatal(err)
		}
		if h.Manifest.Events > limits.Events || h.Manifest.Bytes > limits.Bytes {
			t.Fatalf("caps exceeded: %+v", h.Manifest)
		}
		if h.Manifest.Complete && !h.Manifest.Open {
			phases := map[string]map[string]bool{}
			for _, e := range h.Events {
				if phases[e.RequestID] == nil {
					phases[e.RequestID] = map[string]bool{}
				}
				phases[e.RequestID][e.Phase] = true
			}
			if len(phases) != 8 || len(h.Events) != 16 {
				t.Fatalf("false completeness: %+v", h.Manifest)
			}
			for id, p := range phases {
				if !p["start"] || !p["end"] {
					t.Fatalf("incomplete %s", id)
				}
			}
		}
		err = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			b, err := os.ReadFile(p)
			if bytes.Contains(b, []byte("DP1_SYNTHETIC_")) {
				t.Errorf("credential persisted: %s", p)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestSimProxySchedule(t *testing.T) {
	seeds := []int64{1, 21, 81, 135, 20261022}
	if raw := os.Getenv("SDO_CHAOS_SEED"); raw != "" {
		seed, err := strconv.ParseInt(raw, 0, 64)
		if err != nil {
			t.Fatal(err)
		}
		seeds = []int64{seed}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runProxySchedule(t, seed, nil) })
	}
}
func FuzzProxyHistorySchedule(f *testing.F) {
	f.Add(int64(1), []byte{31, 72, 255, 0, 0})
	f.Add(int64(81), []byte{0, 0, 0, 1, 2})
	f.Add(int64(135), []byte{8, 64, 0, 2, 1, 8, 6, 7, 3})
	f.Fuzz(func(t *testing.T, seed int64, schedule []byte) { runProxySchedule(t, seed, schedule) })
}

// Real sockets complement the scheduled RoundTripper: upgrades retain duplex
// framing, and malformed chunk trailers retain the caller's partial body/error.
func TestSimProxyWireFaults(t *testing.T) {
	for _, seed := range []int64{1, 81, 135} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			protocol := []string{"websocket", "SPDY/3.1"}[rng.Intn(2)]
			payload := fmt.Sprintf("synthetic-%016x", rng.Uint64())
			p, historyDir, store := proxyServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, rw, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				if r.Header.Get("Upgrade") != "" {
					fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", protocol)
					rw.Flush()
					b := make([]byte, len(payload))
					if _, err := io.ReadFull(rw, b); err == nil {
						rw.Write(b)
						rw.Flush()
					}
					return
				}
				rw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Upstream: kept\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ndata\r\n0\r\nDP1_SYNTHETIC_TRAILER_CANARY\r\n\r\n")
				rw.Flush()
			}))
			conn, err := net.DialTimeout("tcp", strings.TrimPrefix(p.URL, "http://"), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(3 * time.Second))
			fmt.Fprintf(conn, "GET /api/v1/namespaces/app/pods/p/exec?dryRun=All HTTP/1.1\r\nHost: synthetic\r\nConnection: Upgrade\r\nUpgrade: %s\r\n\r\n", protocol)
			reader := bufio.NewReader(conn)
			res, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal(err)
			}
			if res.StatusCode != 101 || res.Header.Get("Upgrade") != protocol {
				t.Fatalf("upgrade changed: %+v", res)
			}
			io.WriteString(conn, payload)
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(reader, got); err != nil || string(got) != payload {
				t.Fatalf("duplex bytes: %q %v", got, err)
			}
			conn.Close()
			response, err := p.Client().Get(p.URL + "/api/v1/namespaces/app/pods/p")
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				if readErr == nil {
					t.Fatal("broken upstream became clean EOF")
				}
				if response.StatusCode != 200 || string(body) != "data" || response.Header.Get("X-Upstream") != "kept" {
					t.Fatalf("broken response changed: %d %q", response.StatusCode, body)
				}
			}
			p.Close()
			store.Close()
			h, err := apihistory.Read(historyDir)
			if err != nil {
				t.Fatal(err)
			}
			if h.Manifest.Complete || h.Manifest.Dropped == 0 {
				t.Fatalf("broken stream certified after exit: %+v", h.Manifest)
			}
		})
	}
}
