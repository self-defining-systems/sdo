// Package renderenv owns the render environment: the one fact "what cluster
// do we render for" that deploy, desired-state reads, restart verification and
// upstream advances all need.
//
// The environment is discovered once at install (Discover), recorded with the
// deployment evidence as a contracts.RenderEnvironment, carried to the
// controller by the --render-env flag, and turned into a render.Env only here
// (For). No other package constructs a non-zero render.Env; an absent
// RenderEnvironment means unknown and yields the renderer defaults, so
// evidence recorded before discovery existed keeps its EnvDigest.
package renderenv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"k8s.io/client-go/discovery"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
)

// For returns the render.Env of one operation: the recorded environment, the
// application namespace and the secret mode. A nil environment is unknown.
func For(environment *contracts.RenderEnvironment, namespace string, secrets render.SecretMode) render.Env {
	env := render.Env{Namespace: namespace, Secrets: secrets}
	if environment != nil {
		env.KubeVersion = environment.GetKubeVersion()
		if versions := environment.GetApiVersions(); len(versions) > 0 {
			env.APIVersions = slices.Clone(versions)
		}
	}
	return env
}

// Discover reads the server version and the served API groups, versions and
// kinds. It is called once, at install; nothing renders from a live read.
func Discover(client discovery.DiscoveryInterface) (*contracts.RenderEnvironment, error) {
	if client == nil {
		return nil, errors.New("render environment discovery needs a discovery client")
	}
	version, err := client.ServerVersion()
	if err != nil {
		return nil, fmt.Errorf("discover server version: %w", err)
	}
	_, lists, err := client.ServerGroupsAndResources()
	if err != nil && len(lists) == 0 {
		return nil, fmt.Errorf("discover served APIs: %w", err)
	}
	if err != nil && !discovery.IsGroupDiscoveryFailedError(err) {
		return nil, fmt.Errorf("discover served APIs: %w", err)
	}
	var versions []string
	for _, list := range lists {
		versions = append(versions, list.GroupVersion)
		for _, resource := range list.APIResources {
			versions = append(versions, list.GroupVersion+"/"+resource.Kind)
		}
	}
	slices.Sort(versions)
	environment := &contracts.RenderEnvironment{KubeVersion: version.GitVersion, ApiVersions: slices.Compact(versions)}
	if environment.KubeVersion == "" {
		return nil, errors.New("discovered server reports no version")
	}
	return environment, nil
}

// Format encodes an environment as the compact ProtoJSON --render-env accepts.
// nil encodes as the empty string (unknown).
func Format(environment *contracts.RenderEnvironment) (string, error) {
	if environment == nil {
		return "", nil
	}
	encoded, err := protojson.MarshalOptions{}.Marshal(environment)
	if err != nil {
		return "", fmt.Errorf("encode render environment: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return "", err
	}
	return compact.String(), nil
}

// Parse decodes a --render-env value. Empty is unknown (nil); unknown fields
// are rejected.
func Parse(encoded string) (*contracts.RenderEnvironment, error) {
	if encoded == "" {
		return nil, nil
	}
	environment := &contracts.RenderEnvironment{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(encoded), environment); err != nil {
		return nil, fmt.Errorf("--render-env: %w", err)
	}
	return environment, nil
}

// Same reports whether two environments are equal; nil and empty are the same
// unknown environment.
func Same(left, right *contracts.RenderEnvironment) bool {
	if left == nil {
		left = &contracts.RenderEnvironment{}
	}
	if right == nil {
		right = &contracts.RenderEnvironment{}
	}
	return proto.Equal(left, right)
}
