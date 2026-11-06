package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// EnvironmentVersion is the only environment-document version.
const EnvironmentVersion = 1

// MaxEnvironmentBytes bounds an environment document.
const MaxEnvironmentBytes = 64 << 10

// Environment is the operator's statement of telemetry that the controller
// reads at start (`--telemetry-environment <file>`): the sources the operator
// provides, the credential directory their secrets are mounted under, and the
// pinning mode. It carries no designer data; the designer's bindings come from
// the accepted memory revision. Credential values never appear in it, only
// names resolved under CredentialDir.
type Environment struct {
	Version int  `json:"version"`
	Mode    Mode `json:"mode,omitempty"`
	// Provided are the operator-provided bindings, each with origin operator.
	Provided []Binding `json:"provided,omitempty"`
	// CredentialDir holds one directory per granted credential, each with a
	// file named "token", read per request so rotation needs no restart.
	CredentialDir string `json:"credentialDir,omitempty"`
	// MetricsPublishURL is the base URL applications may push OTLP metrics to,
	// informational for agents; empty when the environment offers none.
	MetricsPublishURL string `json:"metricsPublishUrl,omitempty"`
	// Capabilities are the installer-granted cluster reads agents may rely on.
	Capabilities []ClusterCapability `json:"capabilities,omitempty"`
}

// ParseEnvironment decodes and validates an environment document strictly.
func ParseEnvironment(data []byte) (Environment, error) {
	if len(data) > MaxEnvironmentBytes {
		return Environment{}, fmt.Errorf("telemetry environment is %d bytes; the limit is %d", len(data), MaxEnvironmentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var env Environment
	if err := decoder.Decode(&env); err != nil {
		return Environment{}, fmt.Errorf("telemetry environment: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return Environment{}, errors.New("telemetry environment: trailing data after the document")
	}
	if err := env.Validate(); err != nil {
		return Environment{}, err
	}
	return env, nil
}

// LoadEnvironment reads and parses the environment document at path.
func LoadEnvironment(path string) (Environment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Environment{}, fmt.Errorf("read telemetry environment: %w", err)
	}
	return ParseEnvironment(data)
}

// Marshal encodes the environment as the document ParseEnvironment reads.
func (e Environment) Marshal() ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(e, "", "  ")
}

// Validate checks the document's shape: version, mode, unique well-formed
// provided bindings and a credential directory when one is needed.
func (e Environment) Validate() error {
	if e.Version != EnvironmentVersion {
		return fmt.Errorf("telemetry environment version is %d; this controller reads %d", e.Version, EnvironmentVersion)
	}
	if e.Mode != "" && !e.Mode.Valid() {
		return fmt.Errorf("telemetry environment mode %q is not open, pinned or floor", e.Mode)
	}
	seen := map[string]bool{}
	for _, b := range e.Provided {
		if !SourceIDPattern.MatchString(b.ID) {
			return fmt.Errorf("telemetry environment source id %q does not match %s", b.ID, SourceIDPattern)
		}
		if seen[b.ID] {
			return fmt.Errorf("telemetry environment source %q is provided twice", b.ID)
		}
		seen[b.ID] = true
		if b.Origin != "" && b.Origin != OriginOperator {
			return fmt.Errorf("telemetry environment source %q has origin %q; provided sources are operator", b.ID, b.Origin)
		}
		if b.Endpoint.Credential != "" {
			if !credentialName.MatchString(b.Endpoint.Credential) {
				return fmt.Errorf("telemetry environment source %q names credential %q, which is not a credential name", b.ID, b.Endpoint.Credential)
			}
			if e.CredentialDir == "" {
				return fmt.Errorf("telemetry environment source %q names credential %q but the environment has no credentialDir", b.ID, b.Endpoint.Credential)
			}
		}
	}
	for _, c := range e.Capabilities {
		if !c.Valid() {
			return fmt.Errorf("telemetry environment capability %q is not a known cluster capability", c)
		}
	}
	if e.CredentialDir != "" && !filepath.IsAbs(e.CredentialDir) {
		return fmt.Errorf("telemetry environment credentialDir %q is not an absolute path", e.CredentialDir)
	}
	if e.MetricsPublishURL != "" {
		parsed, err := url.Parse(e.MetricsPublishURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("telemetry environment metricsPublishUrl %q must be an http or https URL", e.MetricsPublishURL)
		}
	}
	return nil
}

// ProvidedBindings are the operator bindings as the registry opens them, with
// origin operator set. A floor environment provides none.
func (e Environment) ProvidedBindings() []Binding {
	if e.EffectiveMode() == ModeFloor {
		return nil
	}
	out := make([]Binding, len(e.Provided))
	for i, b := range e.Provided {
		b.Origin = OriginOperator
		out[i] = b
	}
	return out
}

// EffectiveMode is Mode, or ModeOpen when unset.
func (e Environment) EffectiveMode() Mode {
	if e.Mode == "" {
		return ModeOpen
	}
	return e.Mode
}

// ReadCredential returns the current value of the named granted credential,
// the contents of <CredentialDir>/<name>/token without surrounding space.
func (e Environment) ReadCredential(name string) (string, error) {
	if e.CredentialDir == "" || !credentialName.MatchString(name) {
		return "", fmt.Errorf("telemetry credential %q is not granted by the environment", name)
	}
	token, err := os.ReadFile(filepath.Join(e.CredentialDir, name, "token"))
	if err != nil {
		return "", fmt.Errorf("telemetry credential %q: %w", name, err)
	}
	return strings.TrimSpace(string(token)), nil
}
