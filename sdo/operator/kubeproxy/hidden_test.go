package kubeproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

type countingTransport func(*http.Request) (*http.Response, error)

func (f countingTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// With HiddenNamespaces set, the proxy refuses SDO's control namespace and the cluster-wide lists that could show
// its state; the application's namespace and other cluster reads pass. With none set nothing is refused.
func TestHiddenNamespacesRefuseSDOStateOnly(t *testing.T) {
	forwarded := 0
	upstream := countingTransport(func(*http.Request) (*http.Response, error) {
		forwarded++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("{}")), Header: http.Header{}}, nil
	})
	target, err := url.Parse("https://synthetic-apiserver.invalid")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path    string
		refused bool
	}{
		{"/api/v1/namespaces/demo-sdo/configmaps/sdo-runtime-state", true},
		{"/api/v1/namespaces/demo-sdo/pods/sdo-controller-0/log", true},
		{"/api/v1/namespaces/demo-sdo/pods", true},
		{"/apis/batch/v1/namespaces/demo-sdo/jobs", true},
		{"/api/v1/configmaps", true},
		{"/api/v1/secrets", true},
		{"/api/v1/events", true},
		{"/apis/events.k8s.io/v1/events", true},
		{"/apis/coordination.k8s.io/v1/leases", true},
		{"/apis/sdo.dev/v1/incidents", true},
		{"/api/v1/namespaces/demo/configmaps", false},
		{"/api/v1/namespaces/demo/pods/web-0/log", false},
		{"/api/v1/pods", false},
		{"/api/v1/nodes", false},
		{"/api/v1/namespaces/demo-sdo", false},
		{"/version", false},
	}
	for _, hidden := range [][]string{nil, {"demo-sdo"}} {
		handler := NewRecording(target, upstream, upstream, Config{HiddenNamespaces: hidden})
		for _, tc := range cases {
			before := forwarded
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))
			refused := recorder.Code == http.StatusForbidden
			want := tc.refused && hidden != nil
			if refused != want || (forwarded == before) != want {
				t.Errorf("hidden %v, GET %s: status %d, forwarded %v; want refused %v", hidden, tc.path, recorder.Code, forwarded > before, want)
			}
		}
	}
}
