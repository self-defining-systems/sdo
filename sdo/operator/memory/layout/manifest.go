package layout

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion           = "sdo.dev/v1alpha1"
	DetectorManifestKind = "DetectorManifest"
	SDKVersion           = "v0.1"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// DetectorManifest registers one detector. It is the only detector registration
// shape: each detector owns <detector dir>/manifest.yaml, and the manifest's
// location, not a field, says which package it registers.
type DetectorManifest struct {
	APIVersion          string      `yaml:"apiVersion"`
	Kind                string      `yaml:"kind"`
	SDKVersion          string      `yaml:"sdkVersion"`
	ID                  string      `yaml:"id"`
	Constructor         string      `yaml:"constructor"`
	Class               string      `yaml:"class"`
	Owner               string      `yaml:"owner"`
	Watches             []Watch     `yaml:"watches"`
	Interval            string      `yaml:"interval"`
	Persistence         Persistence `yaml:"persistence"`
	Batching            Batching    `yaml:"batching"`
	PossiblePlaybooks   []string    `yaml:"possiblePlaybooks"`
	OriginatingIncident string      `yaml:"originatingIncident,omitempty"`
	OriginatingCommit   string      `yaml:"originatingCommit,omitempty"`
}

type Watch struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Namespace  string `yaml:"namespace,omitempty"`
}

type Persistence struct {
	Firing   int `yaml:"firing"`
	Clearing int `yaml:"clearing"`
}

type Batching struct {
	Severity string `yaml:"severity"`
	Debounce string `yaml:"debounce"`
}

// DetectorClass is the manifest's class as a typed value.
func (m DetectorManifest) DetectorClass() Class { return Class(m.Class) }

// ConstructorName is the detector constructor, defaulting to New.
func (m DetectorManifest) ConstructorName() string {
	if m.Constructor == "" {
		return "New"
	}
	return m.Constructor
}

// Validate checks the manifest against the detector registration contract.
func (m DetectorManifest) Validate() error {
	if m.APIVersion != APIVersion || m.Kind != DetectorManifestKind || m.SDKVersion != SDKVersion {
		return fmt.Errorf("detector manifest header must be %s %s sdkVersion %s", APIVersion, DetectorManifestKind, SDKVersion)
	}
	if !ValidDetectorID(m.ID) {
		return fmt.Errorf("invalid detector id %q", m.ID)
	}
	if !identifierPattern.MatchString(m.ConstructorName()) {
		return fmt.Errorf("detector %s has invalid constructor", m.ID)
	}
	class := m.DetectorClass()
	if !class.valid() {
		return fmt.Errorf("detector %s has invalid class %q", m.ID, m.Class)
	}
	if m.Owner != class.Owner() {
		return fmt.Errorf("detector %s owner must be %s", m.ID, class.Owner())
	}
	if class == ClassIncident && m.OriginatingIncident == "" {
		return fmt.Errorf("incident detector %s must identify origin", m.ID)
	}
	if m.OriginatingCommit == "" {
		return fmt.Errorf("detector %s needs originatingCommit", m.ID)
	}
	if m.PossiblePlaybooks == nil {
		return fmt.Errorf("detector %s needs possiblePlaybooks", m.ID)
	}
	playbooks := map[string]bool{}
	for _, playbook := range m.PossiblePlaybooks {
		if !ValidPlaybookID(playbook) {
			return fmt.Errorf("detector %s has invalid playbook ID %q", m.ID, playbook)
		}
		if playbooks[playbook] {
			return fmt.Errorf("detector %s has duplicate possiblePlaybooks", m.ID)
		}
		playbooks[playbook] = true
	}
	if len(m.Watches) == 0 {
		return fmt.Errorf("incomplete detector %s: no watches", m.ID)
	}
	watches := map[Watch]bool{}
	for _, watch := range m.Watches {
		if watch.APIVersion == "" || watch.Kind == "" {
			return fmt.Errorf("detector %s has invalid watch", m.ID)
		}
		if watches[watch] {
			return fmt.Errorf("detector %s has duplicate watches", m.ID)
		}
		watches[watch] = true
	}
	if duration, err := time.ParseDuration(m.Interval); err != nil || duration <= 0 {
		return fmt.Errorf("detector %s has invalid interval", m.ID)
	}
	severity := m.Batching.Severity
	if m.Persistence.Firing < 1 || m.Persistence.Clearing < 1 || (severity != "info" && severity != "warn" && severity != "critical") {
		return fmt.Errorf("detector %s has invalid persistence or batching", m.ID)
	}
	if m.Batching.Debounce != "0" {
		if duration, err := time.ParseDuration(m.Batching.Debounce); err != nil || duration <= 0 {
			return fmt.Errorf("detector %s has invalid debounce", m.ID)
		}
	}
	return nil
}

// ParseDetectorManifest decodes a manifest strictly: unknown fields and
// duplicate keys are errors. It does not apply Validate.
func ParseDetectorManifest(data []byte) (DetectorManifest, error) {
	if err := RejectDuplicateKeys(data); err != nil {
		return DetectorManifest{}, err
	}
	var manifest DetectorManifest
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return DetectorManifest{}, fmt.Errorf("detector manifest: %w", err)
	}
	return manifest, nil
}

// MarshalDetectorManifest encodes a manifest in its stored form.
func MarshalDetectorManifest(manifest DetectorManifest) ([]byte, error) {
	return yaml.Marshal(manifest)
}

// RejectDuplicateKeys fails on invalid YAML or a mapping with a repeated key.
func RejectDuplicateKeys(data []byte) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var node yaml.Node
		if err := decoder.Decode(&node); errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("invalid YAML: %w", err)
		}
		if err := uniqueKeys(&node); err != nil {
			return err
		}
	}
}

func uniqueKeys(node *yaml.Node) error {
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]bool)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i].Value
			if seen[key] {
				return fmt.Errorf("duplicate YAML key %q at line %d", key, node.Content[i].Line)
			}
			seen[key] = true
		}
	}
	for _, child := range node.Content {
		if err := uniqueKeys(child); err != nil {
			return err
		}
	}
	return nil
}

// Document is a Markdown memory file: YAML front matter and a body.
type Document struct {
	Meta map[string]any
	Body string
	// Source is the complete file text, front matter included.
	Source string
}

// ParseDocument splits front matter from the body and requires schema_version 1
// and a non-empty body.
func ParseDocument(data []byte) (Document, error) {
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" {
		return Document{}, errors.New("markdown front matter is required")
	}
	var meta map[string]any
	if err := yaml.Unmarshal([]byte(parts[1]), &meta); err != nil {
		return Document{}, fmt.Errorf("front matter: %w", err)
	}
	if meta["schema_version"] != 1 {
		return Document{}, errors.New("schema_version must be 1")
	}
	if strings.TrimSpace(parts[2]) == "" {
		return Document{}, errors.New("body must not be empty")
	}
	return Document{Meta: meta, Body: parts[2], Source: string(data)}, nil
}
