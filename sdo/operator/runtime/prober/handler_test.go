package prober_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type handlerDoer struct{ handler http.Handler }

func (c handlerDoer) Do(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	w := httptest.NewRecorder()
	c.handler.ServeHTTP(w, r)
	return w.Result(), nil
}
func handlerCall(t *testing.T, c handlerDoer, method, path string, body io.Reader) (*http.Response, error) {
	t.Helper()
	request, err := http.NewRequest(method, "http://in-memory"+path, body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	return c.Do(request)
}
