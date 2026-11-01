//go:build integration

package prometheus

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDefaultHTTPClientRejectsRedirects(t *testing.T) {
	redirected := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			redirected = true
			respond(`{"status":"success","data":{"alerts":[]}}`)(w, r)
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	defer server.Close()
	_, err := (Client{BaseURL: server.URL}).Alerts(context.Background())
	if err == nil || !strings.Contains(err.Error(), "302") || redirected {
		t.Fatalf("redirected %v, error %v", redirected, err)
	}
}
