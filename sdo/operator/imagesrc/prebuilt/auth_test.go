package prebuilt

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"

	"sdo.dev/operator/imagesrc"
)

const synthManifest = `{"schemaVersion":2,"synthetic":true}`

func provider(hosts map[string]imagesrc.RegistryLogin, asked *[]string) imagesrc.RegistryLogins {
	return imagesrc.RegistryLoginsFunc(func(_ context.Context, host string) (imagesrc.RegistryLogin, bool, error) {
		*asked = append(*asked, host)
		cred, ok := hosts[host]
		return cred, ok, nil
	})
}

func basic(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

// A registry that answers a Basic challenge (the shape of the cloud container
// registries) gets the configured login, and only the login for the image's host.
func TestSourceAnswersABasicChallengeWithTheConfiguredLogin(t *testing.T) {
	var asked []string
	source := &Source{
		Credentials: provider(map[string]imagesrc.RegistryLogin{"private.example": {Username: "u", Password: "pw"}, "other.example": {Username: "x", Password: "y"}}, &asked),
		Client: doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != basic("u", "pw") {
				return reply(401, "", http.Header{"Www-Authenticate": {`Basic realm="private.example"`}})
			}
			return reply(200, synthManifest, nil)
		}),
	}
	got, err := resolveRef(context.Background(), source, "private.example/team/app:1")
	if err != nil {
		t.Fatal(err)
	}
	if want := "private.example/team/app@" + sha256Digest([]byte(synthManifest)); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	if len(asked) != 1 || asked[0] != "private.example" {
		t.Fatalf("credentials were asked for %v; only the image's own host may be looked up", asked)
	}
}

// A Bearer challenge is answered by presenting the login to the token realm,
// then the token to the registry.
func TestSourcePresentsTheLoginToTheTokenRealm(t *testing.T) {
	var asked []string
	source := &Source{
		Credentials: provider(map[string]imagesrc.RegistryLogin{"private.example": {Username: "u", Password: "pw"}}, &asked),
		Client: doerFunc(func(r *http.Request) (*http.Response, error) {
			switch r.URL.Path {
			case "/token":
				if r.Header.Get("Authorization") != basic("u", "pw") {
					return reply(401, "", nil)
				}
				return reply(200, `{"token":"t"}`, nil)
			default:
				if r.Header.Get("Authorization") == "Bearer t" {
					return reply(200, synthManifest, nil)
				}
				return reply(401, "", http.Header{"Www-Authenticate": {`Bearer realm="https://private.example/token",service="s"`}})
			}
		}),
	}
	if _, err := resolveRef(context.Background(), source, "private.example/team/app:1"); err != nil {
		t.Fatal(err)
	}
}

// With no login configured for the host the request stays anonymous.
func TestSourceStaysAnonymousWithoutALogin(t *testing.T) {
	var asked []string
	source := &Source{
		Credentials: provider(nil, &asked),
		Client: doerFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "" {
				t.Fatalf("anonymous request carried Authorization %q", r.Header.Get("Authorization"))
			}
			return reply(401, "", http.Header{"Www-Authenticate": {`Basic realm="private.example"`}})
		}),
	}
	_, err := resolveRef(context.Background(), source, "private.example/team/app:1")
	var resolve *imagesrc.ResolveError
	if !errors.As(err, &resolve) || resolve.Kind != imagesrc.ResolveUnauthorized {
		t.Fatalf("err = %v, want unauthorized", err)
	}
}

// A rejected login fails closed as unauthorized and never echoes the secret.
func TestSourceReportsARejectedLoginWithoutLeakingIt(t *testing.T) {
	var asked []string
	source := &Source{
		Credentials: provider(map[string]imagesrc.RegistryLogin{"private.example": {Username: "u", Password: "hunter2"}}, &asked),
		Client: doerFunc(func(*http.Request) (*http.Response, error) {
			return reply(401, "", http.Header{"Www-Authenticate": {`Basic realm="private.example"`}})
		}),
	}
	_, err := resolveRef(context.Background(), source, "private.example/team/app:1")
	var resolve *imagesrc.ResolveError
	if !errors.As(err, &resolve) || resolve.Kind != imagesrc.ResolveUnauthorized {
		t.Fatalf("err = %v, want unauthorized", err)
	}
	if strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "configured credentials") {
		t.Fatalf("error = %v", err)
	}
}

// A credential source that cannot be read is transient (it may recover), and a
// malformed one is refused, never silently downgraded to anonymous.
func TestSourceFailsWhenTheCredentialSourceFails(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want imagesrc.ResolveErrorKind
	}{
		"unreadable": {&imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: errors.New("api down")}, imagesrc.ResolveTransient},
		"malformed":  {errors.New("registry credentials are not a Docker config JSON"), imagesrc.ResolveRefused},
	} {
		t.Run(name, func(t *testing.T) {
			source := &Source{
				Credentials: imagesrc.RegistryLoginsFunc(func(context.Context, string) (imagesrc.RegistryLogin, bool, error) {
					return imagesrc.RegistryLogin{}, false, tc.err
				}),
				Client: neverCalled(t),
			}
			_, err := resolveRef(context.Background(), source, "private.example/team/app:1")
			var resolve *imagesrc.ResolveError
			if !errors.As(err, &resolve) || resolve.Kind != tc.want {
				t.Fatalf("err = %v, want kind %s", err, tc.want)
			}
		})
	}
}
