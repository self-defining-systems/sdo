package deployment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/distribution/reference"
	"sdo.dev/operator/deployment/internal/deploymentio"
)

const maxManifestBytes = 4 << 20

const manifestAccept = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

// RegistryResolver pins tag references by asking the registry the reference
// names, which is the registry the kubelet pulls from. It performs anonymous
// reads only: no credential is ever sent, so private images fail closed.
type RegistryResolver struct {
	// Client defaults to a bounded HTTPS-redirect-only client.
	Client deploymentio.Doer
	// InsecureHost is the one host[:port] reachable over plain HTTP (the
	// operator's explicitly configured insecure registry). Every other host
	// uses HTTPS, and loopback or IP-literal hosts are refused.
	InsecureHost string
}

func (r RegistryResolver) ResolveImage(ctx context.Context, ref string) (string, error) {
	named, err := reference.ParseNormalizedNamed(ref)
	if err != nil {
		return "", fmt.Errorf("invalid image reference: %w", err)
	}
	if _, digested := named.(reference.Digested); digested {
		return reference.FamiliarString(named), nil
	}
	tagged, ok := reference.TagNameOnly(named).(reference.Tagged)
	if !ok {
		return "", errors.New("image reference has no tag")
	}
	domain, repository := reference.Domain(named), reference.Path(named)
	host, scheme := domain, "https"
	if domain == "docker.io" {
		host = "registry-1.docker.io"
	}
	if r.InsecureHost != "" && domain == r.InsecureHost {
		scheme = "http"
	} else if ip := net.ParseIP(strings.Trim(strings.SplitN(host, ":", 2)[0], "[]")); ip != nil || strings.HasPrefix(host, "localhost") {
		return "", fmt.Errorf("registry host %q is not a public registry name", host)
	}
	client := r.Client
	if client == nil {
		client = deploymentio.NewHTTPClient(30 * time.Second)
	}
	manifestURL := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", scheme, host, repository, tagged.Tag())
	body, header, err := r.fetch(ctx, client, manifestURL, "", scheme)
	if err != nil {
		return "", err
	}
	digest := sha256Digest(body)
	if claimed := header.Get("Docker-Content-Digest"); claimed != "" && claimed != digest {
		return "", fmt.Errorf("registry digest header %s does not match manifest bytes %s", claimed, digest)
	}
	return reference.FamiliarName(named) + "@" + digest, nil
}

func sha256Digest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func (r RegistryResolver) fetch(ctx context.Context, client deploymentio.Doer, target, token, scheme string) ([]byte, http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Accept", manifestAccept)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, fmt.Errorf("registry request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	switch {
	case response.StatusCode == http.StatusUnauthorized && token == "":
		token, err := r.token(ctx, client, response.Header.Get("WWW-Authenticate"), scheme)
		if err != nil {
			return nil, nil, err
		}
		return r.fetch(ctx, client, target, token, scheme)
	case response.StatusCode == http.StatusNotFound:
		return nil, nil, errors.New("registry has no such tag")
	case response.StatusCode != http.StatusOK:
		return nil, nil, fmt.Errorf("registry answered %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxManifestBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(body) > maxManifestBytes {
		return nil, nil, errors.New("registry manifest is too large")
	}
	return body, response.Header, nil
}

// token performs the anonymous Bearer exchange a registry challenge asks for.
func (r RegistryResolver) token(ctx context.Context, client deploymentio.Doer, challenge, scheme string) (string, error) {
	rest, found := strings.CutPrefix(challenge, "Bearer ")
	if !found {
		return "", errors.New("registry requires authentication; only anonymous pulls are supported")
	}
	params := map[string]string{}
	for _, part := range strings.Split(rest, ",") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			params[key] = strings.Trim(value, `"`)
		}
	}
	realm, err := url.Parse(params["realm"])
	if err != nil || realm.Host == "" || (realm.Scheme != "https" && !(scheme == "http" && realm.Scheme == "http")) {
		return "", errors.New("registry token realm is not acceptable")
	}
	query := realm.Query()
	for _, key := range []string{"service", "scope"} {
		if params[key] != "" {
			query.Set(key, params[key])
		}
	}
	realm.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, realm.String(), nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("registry token request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("registry denied an anonymous pull token (%s); the image is private or does not exist", response.Status)
	}
	var document struct{ Token, AccessToken string }
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return "", err
	}
	if document.Token == "" {
		document.Token = document.AccessToken
	}
	if document.Token == "" {
		return "", errors.New("registry returned no pull token")
	}
	return document.Token, nil
}
