package layout

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// SourcePathRoot is the only tree path a followed source may be mounted at.
const SourcePathRoot = "."

var sourceKindPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Source is one followed upstream in the sources file. Config is the watcher's
// configuration as a JSON-compatible mapping; the upstream catalog decodes it
// into the watcher's own message, so this package never names a config type.
type Source struct {
	Path   string         `yaml:"path"`
	Kind   string         `yaml:"kind"`
	Config map[string]any `yaml:"config"`
}

// Sources is the content of SourcesFile.
type Sources struct {
	Sources []Source `yaml:"sources"`
}

// Validate checks the structure the layout itself owns: at most one source,
// mounted at the tree root, with a well-formed kind and a non-empty config.
// Whether the kind is registered and the config fits it is the catalog's check.
func (s Sources) Validate() error {
	if len(s.Sources) == 0 {
		return errors.New("sources must list at least one source (remove the file to follow nothing)")
	}
	if len(s.Sources) > 1 {
		return fmt.Errorf("sources lists %d sources; only one source at %q is supported", len(s.Sources), SourcePathRoot)
	}
	for i, source := range s.Sources {
		if source.Path != SourcePathRoot {
			return fmt.Errorf("sources[%d].path is %q; only %q is supported", i, source.Path, SourcePathRoot)
		}
		if !sourceKindPattern.MatchString(source.Kind) {
			return fmt.Errorf("sources[%d].kind %q must match %s", i, source.Kind, sourceKindPattern)
		}
		if len(source.Config) == 0 {
			return fmt.Errorf("sources[%d].config is required", i)
		}
	}
	return nil
}

// ParseSources decodes SourcesFile strictly: unknown fields, duplicate keys and
// structural violations are errors.
func ParseSources(data []byte) (Sources, error) {
	if err := RejectDuplicateKeys(data); err != nil {
		return Sources{}, err
	}
	var sources Sources
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&sources); err != nil {
		return Sources{}, fmt.Errorf("sources: %w", err)
	}
	if err := sources.Validate(); err != nil {
		return Sources{}, err
	}
	return sources, nil
}

// MarshalSources encodes sources in their stored form after validating them.
func MarshalSources(sources Sources) ([]byte, error) {
	if err := sources.Validate(); err != nil {
		return nil, err
	}
	return yaml.Marshal(sources)
}

// SourceConfigJSON returns the source's config as canonical JSON, the input of
// the watcher's protojson decode.
func SourceConfigJSON(source Source) ([]byte, error) {
	data, err := json.Marshal(source.Config)
	if err != nil {
		return nil, fmt.Errorf("source %q config is not JSON-compatible: %w", source.Kind, err)
	}
	return data, nil
}

// ParseSourceConfigJSON decodes the operator flag form of a source config: one
// JSON object.
func ParseSourceConfigJSON(text string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	var config map[string]any
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("source config must be a JSON object: %w", err)
	}
	if decoder.More() {
		return nil, errors.New("source config has trailing data after the JSON object")
	}
	if len(config) == 0 {
		return nil, errors.New("source config must not be empty")
	}
	return config, nil
}
