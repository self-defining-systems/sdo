package deployment

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func reply(status int, body string, header http.Header) (*http.Response, error) {
	if header == nil {
		header = http.Header{}
	}
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func TestRegistryResolverPinsTagWithAnonymousToken(t *testing.T) {
	manifest := `{"schemaVersion":2,"synthetic":true}`
	const host = "registry.example:5000"
	resolver := RegistryResolver{InsecureHost: host, Client: doerFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/token":
			return reply(200, `{"token":"t"}`, nil)
		case "/v2/acme/cache/manifests/1.2":
			if r.Header.Get("Authorization") != "Bearer t" {
				return reply(401, "", http.Header{"Www-Authenticate": {`Bearer realm="http://` + host + `/token",service="s",scope="repository:acme/cache:pull"`}})
			}
			return reply(200, manifest, nil)
		}
		return reply(404, "", nil)
	})}
	got, err := resolver.ResolveImage(context.Background(), host+"/acme/cache:1.2")
	if err != nil {
		t.Fatal(err)
	}
	if want := host + "/acme/cache@" + sha256Digest([]byte(manifest)); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if _, err := resolver.ResolveImage(context.Background(), host+"/acme/cache:9"); err == nil || !strings.Contains(err.Error(), "no such tag") {
		t.Fatalf("missing tag error = %v", err)
	}
}

func TestRegistryResolverRefusesInternalHostsAndBadDigests(t *testing.T) {
	for _, ref := range []string{"127.0.0.1:5000/a:1", "localhost/a:1", "10.0.0.1/a:1"} {
		if _, err := (RegistryResolver{}).ResolveImage(context.Background(), ref); err == nil {
			t.Fatalf("%s must be refused", ref)
		}
	}
	lying := doerFunc(func(*http.Request) (*http.Response, error) {
		return reply(200, "{}", http.Header{"Docker-Content-Digest": {synthDigest}})
	})
	if _, err := (RegistryResolver{InsecureHost: "r.example:5000", Client: lying}).ResolveImage(context.Background(), "r.example:5000/a:1"); err == nil {
		t.Fatal("digest header that disagrees with the bytes must be refused")
	}
}

func TestRegistryResolverUsesHTTPSAndDockerHubHost(t *testing.T) {
	var seen string
	client := doerFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.URL.String()
		return reply(200, "{}", nil)
	})
	got, err := (RegistryResolver{Client: client}).ResolveImage(context.Background(), "redis:6.2.4")
	if err != nil {
		t.Fatal(err)
	}
	if seen != "https://registry-1.docker.io/v2/library/redis/manifests/6.2.4" || !strings.HasPrefix(got, "redis@sha256:") {
		t.Fatalf("url %s, pinned %s", seen, got)
	}
}
