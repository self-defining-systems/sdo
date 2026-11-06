package kubeproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sdo.dev/operator/apihistory"
	"strings"
	"testing"
)

type historyTransport func(*http.Request) (*http.Response, error)

func (f historyTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type historyRecorder struct {
	gaps   int
	events []apihistory.Event
}

func (r *historyRecorder) Record(e apihistory.Event, _ []byte) { r.events = append(r.events, e) }
func (r *historyRecorder) Gap()                                { r.gaps++ }

type brokenHistoryBody struct{}

func (brokenHistoryBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenHistoryBody) Close() error             { return nil }

func TestReadBodyOmissionsPreserveMutationHistory(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		broken             bool
		want               int
	}{
		{"discovery", "GET", "/openapi/v2", false, 0},
		{"watch", "GET", "/apis/apps/v1/namespaces/app/deployments?watch=true", true, 0},
		{"write", "PATCH", "/api/v1/namespaces/app/configmaps/config", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &historyRecorder{}
			upstream, _ := url.Parse("http://synthetic.invalid")
			transport := historyTransport(func(*http.Request) (*http.Response, error) {
				var body io.ReadCloser = io.NopCloser(strings.NewReader(strings.Repeat("x", apihistory.MaxProjectionBytes+1)))
				if tc.broken {
					body = brokenHistoryBody{}
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: body}, nil
			})
			func() {
				defer func() {
					if value := recover(); value != nil && value != http.ErrAbortHandler {
						panic(value)
					}
				}()
				New(upstream, transport, transport, recorder).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tc.method, tc.path, nil))
			}()
			if recorder.gaps != tc.want {
				t.Fatalf("gaps=%d want %d", recorder.gaps, tc.want)
			}
			if len(recorder.events) != 2 {
				t.Fatalf("missing request envelopes: %v", recorder.events)
			}
		})
	}
}
