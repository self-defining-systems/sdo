// Package prebuilt is the imagesrc source for images that already exist in a
// registry: it pins a tag to the digest the cluster or the registry reports.
// It never builds and never sends a credential.
package prebuilt

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
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/distribution/reference"
	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/prebuilt/internal/registryio"
)

const maxManifestBytes = 4 << 20

// resolveCacheTTL bounds how long one tag keeps its pinned digest. Retried
// deploys inside the window reuse it, so the same bytes run and the registry
// is not asked again; a moved tag is picked up afterwards.
const resolveCacheTTL = 10 * time.Minute

const manifestAccept = "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func resolveError(kind imagesrc.ResolveErrorKind, format string, args ...any) error {
	return &imagesrc.ResolveError{Kind: kind, Err: fmt.Errorf(format, args...)}
}

// statusError classifies a registry HTTP status.
func statusError(response *http.Response, what string) error {
	switch status := response.StatusCode; {
	case status == http.StatusNotFound:
		return resolveError(imagesrc.ResolveMissing, "registry has no such tag")
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return resolveError(imagesrc.ResolveUnauthorized, "%s denied anonymous access (%s); the image is private or does not exist", what, response.Status)
	case status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500:
		return resolveError(imagesrc.ResolveTransient, "%s answered %s", what, response.Status)
	}
	return resolveError(imagesrc.ResolveRefused, "%s answered %s", what, response.Status)
}

// ClusterImages lists the image names each node reports, so a tag the cluster
// already holds is pinned to the digest the cluster has.
type ClusterImages interface {
	NodeImageNames(ctx context.Context) ([][]string, error)
}

// Source pins tag references. A tag the cluster's nodes already hold
// (preloaded with kind load, a mirror, an earlier pull) is pinned to the digest
// those nodes report, so the Pod runs what is on the node and the kubelet needs
// no pull. Otherwise it asks the registry the reference names, anonymously,
// with a HEAD request (Docker Hub does not count it as a pull) and a GET only
// when the registry omits the digest header. Resolved digests are cached for
// resolveCacheTTL. It never sends a credential, so private images fail closed.
type Source struct {
	// Client defaults to a guarded client that connects only to public addresses.
	Client registryio.Doer
	// InsecureHost is the one host[:port] reachable over plain HTTP (the
	// operator's explicitly configured insecure registry) and exempt from the
	// public-address guard. Every other host uses HTTPS.
	InsecureHost string
	// Cluster, when set, is consulted before the registry.
	Cluster ClusterImages
	// Now is the cache clock; nil uses the wall clock.
	Now func() time.Time

	mu    sync.Mutex
	cache map[string]cachedDigest
}

type cachedDigest struct {
	pinned string
	at     time.Time
}

func (r *Source) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return registryio.Now()
}

// Resolve implements imagesrc.Source. The tree and checkout are not used: the
// registry is the only input. A reference already pinned by digest is returned
// unchanged.
func (r *Source) Resolve(ctx context.Context, req imagesrc.Request, _ *contractsv1alpha1.PrebuiltImageConfig) (imagesrc.Resolved, error) {
	if err := ctx.Err(); err != nil {
		return imagesrc.Resolved{}, err
	}
	pinned, err := r.pin(ctx, req.Image)
	if err != nil {
		return imagesrc.Resolved{}, err
	}
	return imagesrc.Resolved{Ref: pinned, Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
}

// ResolveImage satisfies deployment.ImageResolver.
//
// Transitional shim, removed with the strategy registry in S3-13b.
func (r *Source) ResolveImage(ctx context.Context, ref string) (string, error) {
	return r.pin(ctx, ref)
}

func (r *Source) pin(ctx context.Context, ref string) (string, error) {
	named, err := reference.ParseNormalizedNamed(ref)
	if err != nil {
		return "", resolveError(imagesrc.ResolveRefused, "invalid image reference: %w", err)
	}
	if _, digested := named.(reference.Digested); digested {
		return reference.FamiliarString(named), nil
	}
	tagged, ok := reference.TagNameOnly(named).(reference.Tagged)
	if !ok {
		return "", resolveError(imagesrc.ResolveRefused, "image reference has no tag")
	}
	key := reference.FamiliarString(tagged)
	r.mu.Lock()
	hit, found := r.cache[key]
	r.mu.Unlock()
	if found && r.now().Sub(hit.at) < resolveCacheTTL {
		return hit.pinned, nil
	}
	pinned, err := r.resolve(ctx, named, tagged)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	if r.cache == nil {
		r.cache = map[string]cachedDigest{}
	}
	r.cache[key] = cachedDigest{pinned: pinned, at: r.now()}
	r.mu.Unlock()
	return pinned, nil
}

func (r *Source) resolve(ctx context.Context, named reference.Named, tagged reference.Tagged) (string, error) {
	if digest := r.clusterDigest(ctx, named, tagged); digest != "" {
		return reference.FamiliarName(named) + "@" + digest, nil
	}
	domain, repository := reference.Domain(named), reference.Path(named)
	host, scheme := domain, "https"
	if domain == "docker.io" {
		host = "registry-1.docker.io"
	}
	if r.InsecureHost != "" && domain == r.InsecureHost {
		scheme = "http"
	} else if refusedHost(host) {
		return "", resolveError(imagesrc.ResolveRefused, "registry host %q is not a public registry name", host)
	}
	client := r.Client
	if client == nil {
		client = registryio.NewGuardedClient(registryio.ClientOptions{Timeout: 15 * time.Second, AllowHost: r.InsecureHost})
	}
	manifestURL := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", scheme, host, repository, tagged.Tag())
	digest, err := r.digest(ctx, client, manifestURL, scheme)
	if err != nil {
		return "", err
	}
	return reference.FamiliarName(named) + "@" + digest, nil
}

// refusedHost is a registry host that is an IP literal (bracketed IPv6
// included) or a local name. The guarded client also checks the address each
// connection resolves to, which covers DNS names, redirects and token realms.
func refusedHost(host string) bool {
	name := (&url.URL{Host: host}).Hostname()
	if _, err := netip.ParseAddr(name); err == nil {
		return true
	}
	return net.ParseIP(name) != nil || strings.EqualFold(name, "localhost") || strings.HasSuffix(strings.ToLower(name), ".localhost")
}

// clusterDigest is the digest every node that holds the tag reports for it,
// or empty when no node holds it or the nodes disagree.
func (r *Source) clusterDigest(ctx context.Context, named reference.Named, tagged reference.Tagged) string {
	if r.Cluster == nil {
		return ""
	}
	nodes, err := r.Cluster.NodeImageNames(ctx)
	if err != nil {
		return ""
	}
	digests := map[string]bool{}
	for _, names := range nodes {
		holds, found := false, []string{}
		for _, name := range names {
			parsed, err := reference.ParseNormalizedNamed(name)
			if err != nil || parsed.Name() != named.Name() {
				continue
			}
			if node, ok := parsed.(reference.Tagged); ok && node.Tag() == tagged.Tag() {
				holds = true
			}
			if node, ok := parsed.(reference.Digested); ok && digestPattern.MatchString(node.Digest().String()) {
				found = append(found, node.Digest().String())
			}
		}
		if holds {
			for _, digest := range found {
				digests[digest] = true
			}
		}
	}
	if len(digests) != 1 {
		return ""
	}
	for digest := range digests {
		return digest
	}
	return ""
}

func sha256Digest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// digest asks for the manifest digest: HEAD first, and a GET (verified against
// the bytes) when the registry does not answer HEAD with a digest header.
func (r *Source) digest(ctx context.Context, client registryio.Doer, target, scheme string) (string, error) {
	_, header, err := r.fetch(ctx, client, http.MethodHead, target, "", scheme)
	var head *imagesrc.ResolveError
	if err == nil {
		if claimed := header.Get("Docker-Content-Digest"); digestPattern.MatchString(claimed) {
			return claimed, nil
		}
	} else if !errors.As(err, &head) || head.Kind != imagesrc.ResolveRefused {
		return "", err
	}
	body, header, err := r.fetch(ctx, client, http.MethodGet, target, "", scheme)
	if err != nil {
		return "", err
	}
	digest := sha256Digest(body)
	if claimed := header.Get("Docker-Content-Digest"); claimed != "" && claimed != digest {
		return "", resolveError(imagesrc.ResolveRefused, "registry digest header %s does not match manifest bytes %s", claimed, digest)
	}
	return digest, nil
}

func (r *Source) fetch(ctx context.Context, client registryio.Doer, method, target, token, scheme string) ([]byte, http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return nil, nil, resolveError(imagesrc.ResolveRefused, "registry request: %w", err)
	}
	request.Header.Set("Accept", manifestAccept)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, requestError("registry request", err)
	}
	defer func() { _ = response.Body.Close() }()
	switch {
	case response.StatusCode == http.StatusUnauthorized && token == "":
		token, err := r.token(ctx, client, response.Header.Get("WWW-Authenticate"), scheme)
		if err != nil {
			return nil, nil, err
		}
		return r.fetch(ctx, client, method, target, token, scheme)
	case response.StatusCode != http.StatusOK:
		return nil, nil, statusError(response, "registry")
	}
	if method == http.MethodHead {
		return nil, response.Header, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxManifestBytes+1))
	if err != nil {
		return nil, nil, requestError("registry response", err)
	}
	if len(body) > maxManifestBytes {
		return nil, nil, resolveError(imagesrc.ResolveRefused, "registry manifest is too large")
	}
	return body, response.Header, nil
}

// requestError classifies a transport failure: a refused address is the
// guard's answer; anything else (timeout, DNS, reset) may pass on retry.
func requestError(what string, err error) error {
	if errors.Is(err, registryio.ErrRefusedTarget) {
		return &imagesrc.ResolveError{Kind: imagesrc.ResolveRefused, Err: fmt.Errorf("%s: %w", what, err)}
	}
	return &imagesrc.ResolveError{Kind: imagesrc.ResolveTransient, Err: fmt.Errorf("%s: %w", what, err)}
}

// token performs the anonymous Bearer exchange a registry challenge asks for.
func (r *Source) token(ctx context.Context, client registryio.Doer, challenge, scheme string) (string, error) {
	rest, found := strings.CutPrefix(challenge, "Bearer ")
	if !found {
		return "", resolveError(imagesrc.ResolveUnauthorized, "registry requires authentication; only anonymous pulls are supported")
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
		return "", resolveError(imagesrc.ResolveRefused, "registry token realm is not acceptable")
	}
	if scheme != "http" && refusedHost(realm.Host) {
		return "", resolveError(imagesrc.ResolveRefused, "registry token realm host %q is not a public name", realm.Host)
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
		return "", resolveError(imagesrc.ResolveRefused, "registry token request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return "", requestError("registry token request", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", statusError(response, "registry token endpoint")
	}
	var document struct{ Token, AccessToken string }
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return "", resolveError(imagesrc.ResolveRefused, "registry token response: %w", err)
	}
	if document.Token == "" {
		document.Token = document.AccessToken
	}
	if document.Token == "" {
		return "", resolveError(imagesrc.ResolveRefused, "registry returned no pull token")
	}
	return document.Token, nil
}
