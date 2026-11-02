// Package observation reads architecture declarations, never installer authority.
package observation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"strings"
)

const CeilingEnvironment = "SDO_OBSERVATION_CEILING"

// CeilingFromEnvironment is only for trusted process startup, not proposal input.
func CeilingFromEnvironment() (sdk.ObservationScope, error) {
	var scope sdk.ObservationScope
	raw := os.Getenv(CeilingEnvironment)
	if raw == "" {
		return scope, nil
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&scope); err != nil {
		return scope, fmt.Errorf("invalid installer observation ceiling")
	}
	return scope, scope.Validate()
}
func Read(root string) (sdk.ObservationScope, error) {
	b, err := os.ReadFile(filepath.Join(root, ".sdo", "arch.md"))
	if os.IsNotExist(err) {
		return sdk.ObservationScope{}, nil
	}
	if err != nil {
		return sdk.ObservationScope{}, err
	}
	return Parse(b)
}
func Parse(data []byte) (sdk.ObservationScope, error) {
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 {
		return sdk.ObservationScope{}, fmt.Errorf("architecture front matter required")
	}
	var meta struct {
		Scope *yaml.Node `yaml:"observation_scope"`
	}
	// Decode the complete header to retain strict duplicate-key checking.
	var header map[string]yaml.Node
	if err := yaml.Unmarshal([]byte(parts[1]), &header); err != nil {
		return sdk.ObservationScope{}, err
	}
	node, ok := header["observation_scope"]
	if !ok {
		return sdk.ObservationScope{}, nil
	}
	meta.Scope = &node
	var declaration struct {
		Version    int `yaml:"version"`
		Namespaces []struct {
			Name  string `yaml:"name"`
			Kinds []struct {
				APIVersion string `yaml:"api_version"`
				Kind       string `yaml:"kind"`
			} `yaml:"kinds"`
		} `yaml:"namespaces"`
		ClusterKinds []struct {
			APIVersion string   `yaml:"api_version"`
			Kind       string   `yaml:"kind"`
			Names      []string `yaml:"names"`
		} `yaml:"cluster_kinds"`
	}
	encoded, err := yaml.Marshal(meta.Scope)
	if err != nil {
		return sdk.ObservationScope{}, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	if err := decoder.Decode(&declaration); err != nil {
		return sdk.ObservationScope{}, err
	}
	scope := sdk.ObservationScope{Version: declaration.Version}
	seenNS := map[string]bool{}
	for _, ns := range declaration.Namespaces {
		if seenNS[ns.Name] || len(ns.Kinds) == 0 {
			return scope, fmt.Errorf("duplicate or empty namespace declaration")
		}
		seenNS[ns.Name] = true
		for _, k := range ns.Kinds {
			scope.Targets = append(scope.Targets, sdk.ObservationTarget{APIVersion: k.APIVersion, Kind: k.Kind, Namespace: ns.Name})
		}
	}
	for _, k := range declaration.ClusterKinds {
		if len(k.Names) == 0 {
			return scope, fmt.Errorf("cluster observation requires explicit names")
		}
		for _, name := range k.Names {
			scope.Targets = append(scope.Targets, sdk.ObservationTarget{APIVersion: k.APIVersion, Kind: k.Kind, Name: name})
		}
	}
	return scope, scope.Validate()
}

// Accepted reads architecture from a fixed accepted revision, never a dirty file.
func Accepted(ctx context.Context, root string) (sdk.ObservationScope, string, error) {
	command := exec.CommandContext(ctx, "git", "-C", root, "rev-parse", "HEAD")
	b, err := command.Output()
	if err != nil {
		return sdk.ObservationScope{}, "", fmt.Errorf("resolve accepted observation commit: %w", err)
	}
	revision := strings.TrimSpace(string(b))
	command = exec.CommandContext(ctx, "git", "-C", root, "show", revision+":.sdo/arch.md")
	b, err = command.Output()
	if err != nil {
		return sdk.ObservationScope{}, revision, fmt.Errorf("read accepted architecture: %w", err)
	}
	scope, err := Parse(b)
	return scope, revision, err
}
