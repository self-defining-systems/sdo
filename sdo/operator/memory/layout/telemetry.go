package layout

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gopkg.in/yaml.v3"
)

// Telemetry bindings are designer-authored data under DiagnosticsDir. The
// shapes here are the stored form only: grammar, adapter, endpoint and
// credential rules belong to the shared check, which has the environment's
// policy and the adapter registry.
const (
	TelemetryDir          = DiagnosticsDir + "/telemetry"
	TelemetrySourcesFile  = TelemetryDir + "/sources.yaml"
	TelemetryBackendsFile = TelemetryDir + "/backends.yaml"

	// TelemetryVersion is the only binding-file version.
	TelemetryVersion = 1
	// MaxTelemetryBytes bounds each binding file.
	MaxTelemetryBytes = 32 << 10
	// MaxTelemetrySources and MaxTelemetryBackends bound the entries per file.
	MaxTelemetrySources  = 16
	MaxTelemetryBackends = 4
)

// TelemetrySources is the content of TelemetrySourcesFile.
type TelemetrySources struct {
	Version int               `yaml:"version"`
	Sources []TelemetrySource `yaml:"sources"`
}

// TelemetrySource binds one source id to an adapter and endpoint.
type TelemetrySource struct {
	ID         string            `yaml:"id"`
	Kinds      []string          `yaml:"kinds"`
	Adapter    string            `yaml:"adapter"`
	Endpoint   string            `yaml:"endpoint"`
	Default    bool              `yaml:"default"`
	Backend    string            `yaml:"backend"`
	Credential string            `yaml:"credential"`
	Options    map[string]any    `yaml:"options"`
	Ready      *TelemetryReady   `yaml:"ready"`
	Publish    *TelemetryPublish `yaml:"publish"`
	// Origin is decoded only so the shared check can reject it with a
	// specific rule: Go assigns origins, a designer file never sets one.
	Origin string `yaml:"origin"`
}

// TelemetryReady is the optional readiness probe of a source.
type TelemetryReady struct {
	Text       string `yaml:"text"`
	MinResults int    `yaml:"min_results"`
}

// TelemetryPublish says what applications may push to a source. It is
// informational for agents and the overlay slot.
type TelemetryPublish struct {
	Scrape *struct {
		Annotations bool `yaml:"annotations"`
	} `yaml:"scrape"`
	OTLP *struct {
		Metrics string `yaml:"metrics"`
	} `yaml:"otlp"`
}

// TelemetryBackends is the content of TelemetryBackendsFile.
type TelemetryBackends struct {
	Version  int                `yaml:"version"`
	Backends []TelemetryBackend `yaml:"backends"`
}

// TelemetryBackend is a designer-deployed backend in the control namespace.
type TelemetryBackend struct {
	ID     string         `yaml:"id"`
	Recipe string         `yaml:"recipe"`
	Uses   []string       `yaml:"uses"`
	Params map[string]any `yaml:"params"`
}

// ParseTelemetrySources decodes TelemetrySourcesFile strictly: one document,
// unknown fields and duplicate keys are errors, and so are the wrong version,
// an oversized file and too many entries.
func ParseTelemetrySources(data []byte) (TelemetrySources, error) {
	var out TelemetrySources
	if err := decodeTelemetry("telemetry sources", data, &out); err != nil {
		return TelemetrySources{}, err
	}
	if out.Version != TelemetryVersion {
		return TelemetrySources{}, fmt.Errorf("telemetry sources: version is %d; only version %d is supported", out.Version, TelemetryVersion)
	}
	if len(out.Sources) > MaxTelemetrySources {
		return TelemetrySources{}, fmt.Errorf("telemetry sources: %d sources; at most %d are allowed", len(out.Sources), MaxTelemetrySources)
	}
	return out, nil
}

// ParseTelemetryBackends decodes TelemetryBackendsFile strictly.
func ParseTelemetryBackends(data []byte) (TelemetryBackends, error) {
	var out TelemetryBackends
	if err := decodeTelemetry("telemetry backends", data, &out); err != nil {
		return TelemetryBackends{}, err
	}
	if out.Version != TelemetryVersion {
		return TelemetryBackends{}, fmt.Errorf("telemetry backends: version is %d; only version %d is supported", out.Version, TelemetryVersion)
	}
	if len(out.Backends) > MaxTelemetryBackends {
		return TelemetryBackends{}, fmt.Errorf("telemetry backends: %d backends; at most %d are allowed", len(out.Backends), MaxTelemetryBackends)
	}
	return out, nil
}

func decodeTelemetry(what string, data []byte, into any) error {
	if len(data) > MaxTelemetryBytes {
		return fmt.Errorf("%s: %d bytes; the limit is %d KiB", what, len(data), MaxTelemetryBytes>>10)
	}
	if err := RejectDuplicateKeys(data); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(into); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: the file is empty; it needs `version: %d`", what, TelemetryVersion)
		}
		return fmt.Errorf("%s: %w", what, err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%s: the file must hold exactly one YAML document", what)
	}
	return nil
}
