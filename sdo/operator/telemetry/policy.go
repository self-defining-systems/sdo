package telemetry

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Mode is the environment's statement of who may choose telemetry sources.
type Mode string

const (
	// ModeOpen lets the detection designer bind and deploy sources within the
	// policy's grants. It is the default.
	ModeOpen Mode = "open"
	// ModePinned fixes sources and backends to the environment's: the
	// designer may query them and add detectors, nothing else.
	ModePinned Mode = "pinned"
	// ModeFloor admits the Kubernetes API and SDO probes only.
	ModeFloor Mode = "floor"
)

// Valid reports whether m is a known mode.
func (m Mode) Valid() bool { return m == ModeOpen || m == ModePinned || m == ModeFloor }

// ClusterCapability is an installer-granted cluster read bound to a
// ServiceAccount of the control namespace. Agents choose to use a granted
// capability and never create RBAC.
type ClusterCapability string

const (
	CapPodDiscovery ClusterCapability = "pod-discovery"
	CapNodeProxy    ClusterCapability = "node-proxy"
	CapPodLogs      ClusterCapability = "pod-logs"
)

// Valid reports whether c is a known cluster capability.
func (c ClusterCapability) Valid() bool {
	return c == CapPodDiscovery || c == CapNodeProxy || c == CapPodLogs
}

// Recipe describes a backend recipe the trusted applier can deploy. Declares
// lists the cluster capabilities the recipe itself needs; a capability the
// recipe does not declare is never reachable from it. node-proxy is a strong
// credential, so only a recipe that declares it can use it.
type Recipe struct {
	Name     string              `json:"name"`
	Declares []ClusterCapability `json:"declares,omitempty"`
}

// Policy is caller-owned environment policy for telemetry bindings. It is
// never loaded from candidate memory. Everything here is data: no I/O, so the
// broker, the agent self-check and the installer evaluate the same rules.
type Policy struct {
	// Mode zero means ModeOpen.
	Mode                 Mode   `json:"mode,omitempty"`
	ControlNamespace     string `json:"controlNamespace,omitempty"`
	ApplicationNamespace string `json:"applicationNamespace,omitempty"`
	// ExtraNamespaces are environment-granted namespaces a service endpoint may
	// name besides the control and application namespaces.
	ExtraNamespaces []string `json:"extraNamespaces,omitempty"`
	// ExternalHosts are exact hosts outside the cluster an endpoint may name.
	ExternalHosts []string `json:"externalHosts,omitempty"`
	// ClusterDomain is the suffix after ".svc"; empty means "cluster.local".
	ClusterDomain string `json:"clusterDomain,omitempty"`
	// Credentials are the names of environment-granted credentials. Values
	// never appear in memory.
	Credentials []string `json:"credentials,omitempty"`
	// Capabilities are the granted cluster reads a backend may use.
	Capabilities []ClusterCapability `json:"capabilities,omitempty"`
	// Recipes are the backend recipes the applier can deploy.
	Recipes []Recipe `json:"recipes,omitempty"`
	// Provided are operator-provided sources. They join the effective set with
	// origin operator; designer files may reference but never redefine them.
	Provided []Source `json:"provided,omitempty"`
}

var (
	namespaceName  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	dnsLabel       = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	credentialName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	recipeName     = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)
)

// DefaultClusterDomain is the cluster domain assumed when Policy leaves it empty.
const DefaultClusterDomain = "cluster.local"

// EffectiveMode is Mode, or ModeOpen when unset.
func (p Policy) EffectiveMode() Mode {
	if p.Mode == "" {
		return ModeOpen
	}
	return p.Mode
}

// Namespaces lists every namespace a service endpoint may name.
func (p Policy) Namespaces() []string {
	var out []string
	for _, ns := range append([]string{p.ControlNamespace, p.ApplicationNamespace}, p.ExtraNamespaces...) {
		if ns != "" && !slices.Contains(out, ns) {
			out = append(out, ns)
		}
	}
	return out
}

// Recipe returns the named recipe.
func (p Policy) Recipe(name string) (Recipe, bool) {
	for _, r := range p.Recipes {
		if r.Name == name {
			return r, true
		}
	}
	return Recipe{}, false
}

// Granted reports whether the environment grants capability c.
func (p Policy) Granted(c ClusterCapability) bool { return slices.Contains(p.Capabilities, c) }

// Validate checks the policy itself, so a typo in installer wiring fails at
// construction instead of silently widening what a designer may bind.
func (p Policy) Validate() error {
	if !p.EffectiveMode().Valid() {
		return fmt.Errorf("telemetry policy mode %q must be open, pinned or floor", p.Mode)
	}
	for _, ns := range p.Namespaces() {
		if !namespaceName.MatchString(ns) {
			return fmt.Errorf("telemetry policy namespace %q is not a Kubernetes namespace name", ns)
		}
	}
	for _, host := range p.ExternalHosts {
		if err := checkHostName(host); err != nil {
			return fmt.Errorf("telemetry policy external host %q: %w", host, err)
		}
	}
	for _, name := range p.Credentials {
		if !credentialName.MatchString(name) {
			return fmt.Errorf("telemetry policy credential name %q must match %s", name, credentialName)
		}
	}
	for _, c := range p.Capabilities {
		if !c.Valid() {
			return fmt.Errorf("telemetry policy grants unknown capability %q", c)
		}
	}
	for _, r := range p.Recipes {
		if !recipeName.MatchString(r.Name) {
			return fmt.Errorf("telemetry policy recipe name %q must match %s", r.Name, recipeName)
		}
		for _, c := range r.Declares {
			if !c.Valid() {
				return fmt.Errorf("telemetry policy recipe %q declares unknown capability %q", r.Name, c)
			}
		}
	}
	return nil
}

// BindingCheck is the outcome of Policy.CheckBinding, split by what failed so a
// caller can name its own rule.
type BindingCheck struct{ Endpoint, Credential error }

// Err is the first failure, or nil.
func (c BindingCheck) Err() error {
	if c.Endpoint != nil {
		return fmt.Errorf("endpoint: %w", c.Endpoint)
	}
	if c.Credential != nil {
		return fmt.Errorf("credential: %w", c.Credential)
	}
	return nil
}

// CheckBinding is the one policy check on a binding's endpoint and credential.
// The broker runs it when a sources file is published and the controller runs
// it when it opens the accepted bindings, so a policy narrowed after
// publication, or memory that reached the branch by another path, is never
// honored.
func (p Policy) CheckBinding(b Binding) BindingCheck {
	return BindingCheck{Endpoint: p.CheckEndpoint(b.Endpoint.URL), Credential: p.CheckCredential(b.Endpoint.Credential)}
}

// CheckCredential accepts an empty name or one the environment granted.
func (p Policy) CheckCredential(name string) error {
	if name == "" || slices.Contains(p.Credentials, name) {
		return nil
	}
	return fmt.Errorf("credential %q is not granted by the environment (granted: %s)", name, listOrNone(p.Credentials))
}

// metadataHosts are names that reach cloud metadata services.
var metadataHosts = []string{"metadata", "metadata.google.internal", "instance-data", "instance-data.ec2.internal"}

// checkHostName rejects what can never be an allowed host: IP literals, the
// loopback name, metadata names and anything that is not a plain DNS name.
func checkHostName(host string) error {
	if host == "" {
		return errors.New("endpoint has no host")
	}
	if net.ParseIP(strings.Trim(host, "[]")) != nil || strings.HasPrefix(host, "[") || allDigitsAndDots(host) {
		return errors.New("host is an IP literal; name a service or an allowlisted host")
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return errors.New("host is localhost")
	}
	if slices.Contains(metadataHosts, host) {
		return errors.New("host is a cloud metadata name")
	}
	for _, label := range strings.Split(host, ".") {
		if !dnsLabel.MatchString(label) {
			return fmt.Errorf("host %q is not a plain DNS name (no trailing dot, userinfo or special characters)", host)
		}
	}
	return nil
}

// allDigitsAndDots catches decimal and dotted-number forms of an address that
// net.ParseIP does not accept ("2130706433", "127.1").
func allDigitsAndDots(host string) bool {
	if host == "" {
		return false
	}
	for _, r := range host {
		if (r < '0' || r > '9') && r != '.' {
			return false
		}
	}
	return true
}

// CheckEndpoint is the static endpoint rule. The controller issues requests
// with its own network identity, so an unrestricted endpoint would be an SSRF
// and exfiltration surface: the URL must be http or https without userinfo,
// query or fragment, and its host must be "<svc>.<ns>.svc[.<cluster-domain>]"
// with <ns> granted by the policy, or an exact allowlisted external host.
// Names are lowercased before comparison. No resolution happens here.
func (p Policy) CheckEndpoint(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return errors.New("endpoint is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("endpoint is not a URL: %v", err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return fmt.Errorf("endpoint scheme %q must be http or https", u.Scheme)
	case u.User != nil:
		return errors.New("endpoint must not carry userinfo; credentials are granted by name")
	case u.RawQuery != "" || u.ForceQuery:
		return errors.New("endpoint must not carry a query")
	case u.Fragment != "" || strings.Contains(raw, "#"):
		return errors.New("endpoint must not carry a fragment")
	case u.Opaque != "":
		return errors.New("endpoint must be an absolute http(s) URL")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("endpoint port %q is out of range", port)
		}
	}
	host := strings.ToLower(u.Hostname())
	if err := checkHostName(host); err != nil {
		return err
	}
	if slices.Contains(p.ExternalHosts, host) {
		return nil
	}
	labels := strings.Split(host, ".")
	if len(labels) >= 3 && labels[2] == "svc" {
		domain := p.ClusterDomain
		if domain == "" {
			domain = DefaultClusterDomain
		}
		rest := strings.Join(labels[3:], ".")
		if rest == "" || rest == domain {
			if slices.Contains(p.Namespaces(), labels[1]) {
				return nil
			}
			return fmt.Errorf("endpoint namespace %q is not granted (granted: %s)", labels[1], listOrNone(p.Namespaces()))
		}
	}
	return fmt.Errorf("endpoint host %q is neither a service in a granted namespace (<svc>.<ns>.svc, namespaces: %s) nor on the external host allowlist (%s)", host, listOrNone(p.Namespaces()), listOrNone(p.ExternalHosts))
}

func listOrNone(items []string) string {
	if len(items) == 0 {
		return "none"
	}
	return strings.Join(items, ", ")
}
