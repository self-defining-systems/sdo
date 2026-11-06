package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
)

// MaxProfileBytes bounds a profile document.
const MaxProfileBytes = 64 << 10

// MaxProfileSources bounds the operator-provided sources of one profile.
const MaxProfileSources = 16

var profileName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// Profile is the environment's statement of application telemetry: the pinning
// mode, the backend recipes the installer applies, the cluster reads it grants
// and the sources it provides. It is data chosen by the operator outside the
// repository (`--telemetry-profile`); the installer derives the controller's
// Environment and the broker's Policy from it, so both agree by construction.
type Profile struct {
	Name string `json:"name"`
	Mode Mode   `json:"mode,omitempty"`
	// Recipes are SDO-owned backend recipes the installer applies.
	Recipes []string `json:"recipes,omitempty"`
	// Capabilities are the cluster reads the installer grants.
	Capabilities []ClusterCapability `json:"capabilities,omitempty"`
	// Sources are provided to the controller with origin operator.
	Sources []ProfileSource `json:"sources,omitempty"`
	// MetricsPublishURL is the base URL applications may push OTLP metrics to;
	// empty when the profile offers none.
	MetricsPublishURL string `json:"metricsPublishUrl,omitempty"`
}

// ProfileSource is one provided source in a profile document. Credentials are
// not part of profiles yet, so none can be named.
type ProfileSource struct {
	ID       string          `json:"id"`
	Adapter  string          `json:"adapter"`
	Kinds    []Kind          `json:"kinds"`
	Endpoint string          `json:"endpoint"`
	Default  bool            `json:"default,omitempty"`
	Options  json.RawMessage `json:"options,omitempty"`
}

// Binding is the source as the registry opens it, with origin operator.
func (s ProfileSource) Binding() Binding {
	return Binding{ID: s.ID, Adapter: s.Adapter, Kinds: s.Kinds, Endpoint: Endpoint{URL: s.Endpoint}, Default: s.Default, Origin: OriginOperator, Options: s.Options}
}

// ParseProfile decodes and validates a profile document strictly.
func ParseProfile(data []byte) (Profile, error) {
	if len(data) > MaxProfileBytes {
		return Profile{}, fmt.Errorf("telemetry profile is %d bytes; the limit is %d", len(data), MaxProfileBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var p Profile
	if err := decoder.Decode(&p); err != nil {
		return Profile{}, fmt.Errorf("telemetry profile: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Profile{}, errors.New("telemetry profile: trailing data after the document")
	}
	if err := p.Validate(); err != nil {
		return Profile{}, err
	}
	return p, nil
}

// Marshal encodes the profile as the document ParseProfile reads.
func (p Profile) Marshal() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(p)
}

// Validate checks the profile's shape. Whether the named adapters and recipes
// exist is checked where the registries are known.
func (p Profile) Validate() error {
	if !profileName.MatchString(p.Name) {
		return fmt.Errorf("telemetry profile name %q must match %s", p.Name, profileName)
	}
	if p.Mode != "" && !p.Mode.Valid() {
		return fmt.Errorf("telemetry profile %q mode %q is not open, pinned or floor", p.Name, p.Mode)
	}
	recipes := map[string]bool{}
	for _, name := range p.Recipes {
		if !profileName.MatchString(name) || recipes[name] {
			return fmt.Errorf("telemetry profile %q recipe %q is malformed or listed twice", p.Name, name)
		}
		recipes[name] = true
	}
	if len(p.Sources) > MaxProfileSources {
		return fmt.Errorf("telemetry profile %q has %d sources; the limit is %d", p.Name, len(p.Sources), MaxProfileSources)
	}
	seen := map[string]bool{}
	defaults := 0
	for _, s := range p.Sources {
		if !SourceIDPattern.MatchString(s.ID) || seen[s.ID] {
			return fmt.Errorf("telemetry profile %q source id %q is malformed or provided twice", p.Name, s.ID)
		}
		seen[s.ID] = true
		if s.Adapter == "" || len(s.Kinds) == 0 {
			return fmt.Errorf("telemetry profile %q source %q needs an adapter and kinds", p.Name, s.ID)
		}
		if parsed, err := url.Parse(s.Endpoint); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("telemetry profile %q source %q endpoint %q must be an http or https URL", p.Name, s.ID, s.Endpoint)
		}
		if s.Default {
			defaults++
		}
	}
	if defaults > 1 {
		return fmt.Errorf("telemetry profile %q has %d default sources; at most one", p.Name, defaults)
	}
	if p.MetricsPublishURL != "" {
		if parsed, err := url.Parse(p.MetricsPublishURL); err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("telemetry profile %q metricsPublishUrl %q must be an http or https URL", p.Name, p.MetricsPublishURL)
		}
	}
	return nil
}

// WithSource returns the profile with one more provided source. A source
// becomes the default when it asks to or when the profile has none yet and the
// source serves metrics. The result is validated.
func (p Profile) WithSource(s ProfileSource) (Profile, error) {
	hasDefault := false
	for _, existing := range p.Sources {
		hasDefault = hasDefault || existing.Default
	}
	if !hasDefault && !s.Default {
		for _, k := range s.Kinds {
			if k == KindMetrics {
				s.Default = true
			}
		}
	}
	p.Sources = append(append([]ProfileSource(nil), p.Sources...), s)
	if err := p.Validate(); err != nil {
		return Profile{}, err
	}
	return p, nil
}

// Environment is the document the controller reads, derived from the profile.
func (p Profile) Environment() Environment {
	env := Environment{Version: EnvironmentVersion, Mode: p.Mode, MetricsPublishURL: p.MetricsPublishURL}
	for _, s := range p.Sources {
		env.Provided = append(env.Provided, s.Binding())
	}
	return env
}

// HasRecipe reports whether the profile applies the named recipe.
func (p Profile) HasRecipe(name string) bool {
	for _, r := range p.Recipes {
		if r == name {
			return true
		}
	}
	return false
}
