package journeycheck

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"sdo.dev/operator/sdk/traffic"
)

// alterations are the wrong answers given to each request: an error status, no
// body, the last element missing, the probe's own text changed, and the answer
// the same request got in an earlier run.
var alterations = []string{"503", "empty", "drop", "text", "stale"}

var variable = regexp.MustCompile(`[0-9a-f]{16,}|\d+`)

// alterer passes requests through and alters the answer of one distinct
// request of an iteration, every time it is sent (a step that waits for its
// effect sends the same request again).
type alterer struct {
	client   traffic.Doer
	marker   string
	previous map[int][]byte

	mu      sync.Mutex
	step    int
	kind    string
	seen    []string
	applied bool
	line    string
}

func (a *alterer) start(step int, kind string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.step, a.kind, a.seen, a.applied, a.line = step, kind, nil, false, ""
}

// index is which distinct request of the iteration this is. Consecutive reads
// of one URL are one step (it polls); every write is its own.
func (a *alterer) index(r *http.Request) int {
	key := r.Method + " " + variable.ReplaceAllString(r.URL.RequestURI(), "N")
	if n := len(a.seen); n > 0 && a.seen[n-1] == key && (r.Method == http.MethodGet || r.Method == http.MethodHead) {
		return n - 1
	}
	a.seen = append(a.seen, key)
	return len(a.seen) - 1
}

// Do implements traffic.Doer.
func (a *alterer) Do(r *http.Request) (*http.Response, error) {
	a.mu.Lock()
	at, step, kind := a.index(r), a.step, a.kind
	a.mu.Unlock()
	response, err := a.client.Do(r)
	if err != nil {
		return response, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, traffic.MaxResponseBody+1))
	_ = response.Body.Close()
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if step < 0 {
		if _, ok := a.previous[at]; !ok {
			a.previous[at] = body
		}
	}
	if at == step {
		altered, status := alter(kind, body, a.marker, a.previous[at], response.StatusCode)
		if !bytes.Equal(altered, body) || status != response.StatusCode {
			a.applied, a.line = true, r.Method+" "+r.URL.Path
			body, response.StatusCode, response.Status = altered, status, http.StatusText(status)
		}
	}
	response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
	response.Header.Del("Content-Length")
	return response, nil
}

func alter(kind string, body []byte, marker string, previous []byte, status int) ([]byte, int) {
	switch kind {
	case "503":
		return []byte("altered"), http.StatusServiceUnavailable
	case "empty":
		return nil, status
	case "drop":
		var list []json.RawMessage
		if json.Unmarshal(body, &list) == nil && len(list) > 0 {
			if short, err := json.Marshal(list[:len(list)-1]); err == nil {
				return short, status
			}
		}
	case "text":
		if marker != "" {
			changed := marker[:len(marker)-1] + "~"
			return []byte(strings.ReplaceAll(string(body), marker, changed)), status
		}
	case "stale":
		if previous != nil && len(previous) > 0 {
			return previous, status
		}
	}
	return body, status
}
