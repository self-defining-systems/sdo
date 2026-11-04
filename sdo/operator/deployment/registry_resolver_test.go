package deployment

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"
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
	resolver := &RegistryResolver{InsecureHost: host, Client: doerFunc(func(r *http.Request) (*http.Response, error) {
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

// neverCalled is a transport the guard must keep every request away from: it
// fails the test if the resolver reaches it, so deleting the guard fails.
func neverCalled(t *testing.T) doerFunc {
	return func(r *http.Request) (*http.Response, error) {
		t.Fatalf("the resolver contacted %s; the guard must refuse the target first", r.URL)
		return nil, nil
	}
}

func TestRegistryResolverRefusesInternalHostsBeforeAnyRequest(t *testing.T) {
	for _, ref := range []string{"127.0.0.1:5000/a:1", "localhost/a:1", "10.0.0.1/a:1", "[::1]:5000/a:1", "[fd00::1]/a:1", "[::ffff:10.0.0.1]:5000/a:1", "app.localhost/a:1"} {
		_, err := (&RegistryResolver{Client: neverCalled(t)}).ResolveImage(context.Background(), ref)
		var resolve *ResolveError
		if !errors.As(err, &resolve) || resolve.Kind != ResolveRefused {
			t.Fatalf("%s must be refused as a target, got %v", ref, err)
		}
	}
}

func TestRegistryResolverRefusesInternalTokenRealm(t *testing.T) {
	const host = "registry.example"
	client := doerFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != host {
			t.Fatalf("contacted the token realm %s", r.URL)
		}
		return reply(401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://10.0.0.7/token",service="s"`}})
	})
	_, err := (&RegistryResolver{Client: client}).ResolveImage(context.Background(), host+"/a:1")
	var resolve *ResolveError
	if !errors.As(err, &resolve) || resolve.Kind != ResolveRefused {
		t.Fatalf("an internal token realm must be refused, got %v", err)
	}
}

func TestRegistryResolverVerifiesBytesWhenItMustGet(t *testing.T) {
	lying := doerFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodHead {
			return reply(200, "", nil) // no digest header: the resolver must GET
		}
		return reply(200, "{}", http.Header{"Docker-Content-Digest": {synthDigest}})
	})
	if _, err := (&RegistryResolver{Client: lying}).ResolveImage(context.Background(), "r.example/a:1"); err == nil {
		t.Fatal("digest header that disagrees with the bytes must be refused")
	}
}

// A HEAD with a digest header is the whole exchange: Docker Hub does not count
// it as a pull, and no manifest body is fetched.
func TestRegistryResolverAsksWithHeadAndCachesTheDigest(t *testing.T) {
	var methods []string
	clock := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	resolver := &RegistryResolver{Now: func() time.Time { return clock }, Client: doerFunc(func(r *http.Request) (*http.Response, error) {
		methods = append(methods, r.Method)
		return reply(200, "", http.Header{"Docker-Content-Digest": {synthDigest}})
	})}
	for range 3 {
		got, err := resolver.ResolveImage(context.Background(), "redis:6.2.4")
		if err != nil || got != "redis@"+synthDigest {
			t.Fatalf("got %q, %v", got, err)
		}
	}
	if len(methods) != 1 || methods[0] != http.MethodHead {
		t.Fatalf("requests = %v, want one HEAD for three resolutions", methods)
	}
	clock = clock.Add(resolveCacheTTL + time.Second)
	if _, err := resolver.ResolveImage(context.Background(), "redis:6.2.4"); err != nil || len(methods) != 2 {
		t.Fatalf("an expired entry must be resolved again: %v %v", methods, err)
	}
}

func TestRegistryResolverClassifiesFailures(t *testing.T) {
	status := func(code int) deploymentio.Doer {
		return doerFunc(func(*http.Request) (*http.Response, error) { return reply(code, "", nil) })
	}
	for name, tc := range map[string]struct {
		client deploymentio.Doer
		want   ResolveErrorKind
	}{
		"rate limited":  {status(429), ResolveTransient},
		"registry down": {status(503), ResolveTransient},
		"network":       {doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection reset") }), ResolveTransient},
		"timeout":       {doerFunc(func(*http.Request) (*http.Response, error) { return nil, context.DeadlineExceeded }), ResolveTransient},
		"missing tag":   {status(404), ResolveMissing},
		"forbidden":     {status(403), ResolveUnauthorized},
		"token refused": {doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/token" {
				return reply(403, "", nil)
			}
			return reply(401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://auth.example/token"`}})
		}), ResolveUnauthorized},
		"token rate limited": {doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/token" {
				return reply(429, "", nil)
			}
			return reply(401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://auth.example/token"`}})
		}), ResolveTransient},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := (&RegistryResolver{Client: tc.client}).ResolveImage(context.Background(), "r.example/a:1")
			var resolve *ResolveError
			if !errors.As(err, &resolve) || resolve.Kind != tc.want {
				t.Fatalf("kind = %v (%v), want %s", resolve, err, tc.want)
			}
		})
	}
}

type fakeClusterImages [][]string

func (f fakeClusterImages) NodeImageNames(context.Context) ([][]string, error) { return f, nil }

// An image the cluster already holds under the tag (kind load, a mirror) is
// pinned to the digest the node reports: the Pod runs what is on the node and
// the registry is not asked.
func TestRegistryResolverPrefersTheDigestTheClusterHolds(t *testing.T) {
	const other = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	held := []string{"docker.io/library/redis:6.2.4", "docker.io/library/redis@" + synthDigest}
	for name, tc := range map[string]struct {
		cluster fakeClusterImages
		want    string // empty: the registry answers
	}{
		"held with a digest":       {fakeClusterImages{held}, "redis@" + synthDigest},
		"two nodes agree":          {fakeClusterImages{held, held}, "redis@" + synthDigest},
		"nodes disagree":           {fakeClusterImages{held, {"docker.io/library/redis:6.2.4", "docker.io/library/redis@" + other}}, ""},
		"held without a digest":    {fakeClusterImages{{"docker.io/library/redis:6.2.4"}}, ""},
		"another tag of the image": {fakeClusterImages{{"docker.io/library/redis:7", "docker.io/library/redis@" + other}}, ""},
		"another repository's tag": {fakeClusterImages{{"docker.io/library/valkey:6.2.4", "docker.io/library/valkey@" + other}}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			asked := false
			resolver := &RegistryResolver{Cluster: tc.cluster, Client: doerFunc(func(*http.Request) (*http.Response, error) {
				asked = true
				return reply(200, "", http.Header{"Docker-Content-Digest": {"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}})
			})}
			got, err := resolver.ResolveImage(context.Background(), "redis:6.2.4")
			if err != nil {
				t.Fatal(err)
			}
			if tc.want != "" && (got != tc.want || asked) {
				t.Fatalf("got %s (registry asked: %v), want %s from the cluster", got, asked, tc.want)
			}
			if tc.want == "" && (!asked || !strings.HasSuffix(got, "ccc")) {
				t.Fatalf("got %s (registry asked: %v), want the registry's digest", got, asked)
			}
		})
	}
}

func TestRegistryResolverUsesHTTPSAndDockerHubHost(t *testing.T) {
	var seen string
	client := doerFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.URL.String()
		return reply(200, "{}", nil)
	})
	got, err := (&RegistryResolver{Client: client}).ResolveImage(context.Background(), "redis:6.2.4")
	if err != nil {
		t.Fatal(err)
	}
	if seen != "https://registry-1.docker.io/v2/library/redis/manifests/6.2.4" || !strings.HasPrefix(got, "redis@sha256:") {
		t.Fatalf("url %s, pinned %s", seen, got)
	}
}
