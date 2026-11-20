// Registry logins: the credential seam of the image sources. It answers
// which login, if any, to present to a registry host. It knows nothing about
// any cloud. A platform binding keeps a standard Docker config JSON (the
// .dockerconfigjson of an imagePullSecret) fresh, for example by rotating a
// short-lived token into the Secret, and SDO reads it on every lookup.

package imagesrc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// RegistryLogin is a registry login. It is never logged or placed in an error.
type RegistryLogin struct{ Username, Password string }

// RegistryLogins supplies the login for a registry host (host[:port], docker.io for
// Docker Hub). A host with no login reports ok=false and is used anonymously.
type RegistryLogins interface {
	For(ctx context.Context, host string) (cred RegistryLogin, ok bool, err error)
}

// RegistryLoginsFunc adapts a function, such as a credential-helper style token
// source, to RegistryLogins.
type RegistryLoginsFunc func(ctx context.Context, host string) (RegistryLogin, bool, error)

// For implements RegistryLogins.
func (f RegistryLoginsFunc) For(ctx context.Context, host string) (RegistryLogin, bool, error) {
	return f(ctx, host)
}

// RegistryLoginsFromDockerConfig serves logins from a Docker config JSON ({"auths": {...}}).
// load runs on every lookup, so a rotated Secret takes effect without a restart.
func RegistryLoginsFromDockerConfig(load func(ctx context.Context) ([]byte, error)) RegistryLogins {
	return RegistryLoginsFunc(func(ctx context.Context, host string) (RegistryLogin, bool, error) {
		data, err := load(ctx)
		if err != nil {
			return RegistryLogin{}, false, err
		}
		return lookup(data, host)
	})
}

type authEntry struct {
	Auth          string `json:"auth"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	IdentityToken string `json:"identitytoken"`
}

func lookup(data []byte, host string) (RegistryLogin, bool, error) {
	var config struct {
		Auths map[string]authEntry `json:"auths"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		return RegistryLogin{}, false, errors.New("registry credentials are not a Docker config JSON")
	}
	want := canonicalHost(host)
	for key, entry := range config.Auths {
		if canonicalHost(key) == want {
			return entry.credential(key)
		}
	}
	return RegistryLogin{}, false, nil
}

func (e authEntry) credential(key string) (RegistryLogin, bool, error) {
	switch {
	case e.Username != "" || e.Password != "":
		return RegistryLogin{Username: e.Username, Password: e.Password}, true, nil
	case e.Auth != "":
		raw, err := base64.StdEncoding.DecodeString(e.Auth)
		if err != nil {
			return RegistryLogin{}, false, fmt.Errorf("registry credentials for %q: auth is not base64", key)
		}
		user, password, found := strings.Cut(string(raw), ":")
		if !found {
			return RegistryLogin{}, false, fmt.Errorf("registry credentials for %q: auth is not user:password", key)
		}
		return RegistryLogin{Username: user, Password: password}, true, nil
	case e.IdentityToken != "":
		return RegistryLogin{}, false, fmt.Errorf("registry credentials for %q: identity tokens are not supported; use a username and password", key)
	}
	return RegistryLogin{}, false, nil
}

// canonicalHost reduces a Docker config key or a lookup host to host[:port],
// folding the Docker Hub spellings together.
func canonicalHost(key string) string {
	key = strings.TrimSpace(key)
	if !strings.Contains(key, "://") {
		key = "https://" + key
	}
	parsed, err := url.Parse(key)
	if err != nil || parsed.Host == "" {
		return strings.ToLower(key)
	}
	host := strings.ToLower(parsed.Host)
	switch host {
	case "index.docker.io", "registry-1.docker.io", "registry.hub.docker.com":
		return "docker.io"
	}
	return host
}
