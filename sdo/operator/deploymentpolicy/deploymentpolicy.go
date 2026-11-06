// Package deploymentpolicy owns the install-time deployment policy: the
// registry allowlist and the namespace and cluster-scope grants a deployment
// must stay within.
//
// The policy comes from whoever installs the controller (operate and
// install-existing flags), travels to the controller as --deployment-policy,
// is enforced by deployment.Deployer on every apply, and is recorded in the
// deployment receipt and evidence. It never comes from agent input. An absent
// or empty policy is today's behavior: no allowlist, and foreign-namespace or
// cluster-scoped objects are notes.
package deploymentpolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operability/offline"
)

// Flags is the install-time flag form of the policy: comma-separated lists.
type Flags struct {
	AllowedRegistries   string
	ScopeBlocking       bool
	GrantedNamespaces   string
	GrantedClusterKinds string
}

// FromFlags builds the policy of the flags; nil when none is set.
func FromFlags(f Flags) (*contracts.DeploymentPolicy, error) {
	policy := &contracts.DeploymentPolicy{
		AllowedRegistries:   split(f.AllowedRegistries),
		ScopeBlocking:       f.ScopeBlocking,
		GrantedNamespaces:   split(f.GrantedNamespaces),
		GrantedClusterKinds: split(f.GrantedClusterKinds),
	}
	if err := Validate(policy); err != nil {
		return nil, err
	}
	if proto.Size(policy) == 0 {
		return nil, nil
	}
	return policy, nil
}

func split(list string) []string {
	var out []string
	for _, item := range strings.Split(list, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Validate checks the policy is internally consistent. Grants without
// scope_blocking would silently do nothing, so they are refused.
func Validate(policy *contracts.DeploymentPolicy) error {
	if policy == nil {
		return nil
	}
	for _, host := range policy.GetAllowedRegistries() {
		if host == "" || host != strings.ToLower(host) || strings.ContainsAny(host, " /@") {
			return fmt.Errorf("deployment policy: registry %q must be a lowercase host[:port]", host)
		}
	}
	for _, namespace := range policy.GetGrantedNamespaces() {
		if namespace == "" || strings.ContainsAny(namespace, " /,") {
			return fmt.Errorf("deployment policy: invalid granted namespace %q", namespace)
		}
	}
	for _, kind := range policy.GetGrantedClusterKinds() {
		if kind == "" || strings.ContainsAny(kind, " /,") {
			return fmt.Errorf("deployment policy: invalid granted cluster kind %q", kind)
		}
	}
	if !policy.GetScopeBlocking() && (len(policy.GetGrantedNamespaces()) > 0 || len(policy.GetGrantedClusterKinds()) > 0) {
		return fmt.Errorf("deployment policy: grants need scope blocking, otherwise nothing is blocked")
	}
	return nil
}

// Offline is the policy the offline checks and the deployer evaluate. Nil is
// the zero policy: nothing blocked, no allowlist.
func Offline(policy *contracts.DeploymentPolicy) offline.Policy {
	return offline.Policy{
		ScopeBlocking:       policy.GetScopeBlocking(),
		GrantedNamespaces:   slices.Clone(policy.GetGrantedNamespaces()),
		GrantedClusterKinds: slices.Clone(policy.GetGrantedClusterKinds()),
		AllowedRegistries:   slices.Clone(policy.GetAllowedRegistries()),
	}
}

// Format encodes a policy as the compact ProtoJSON --deployment-policy
// accepts. A nil or empty policy encodes as the empty string.
func Format(policy *contracts.DeploymentPolicy) (string, error) {
	if policy == nil || proto.Size(policy) == 0 {
		return "", nil
	}
	encoded, err := protojson.Marshal(policy)
	if err != nil {
		return "", fmt.Errorf("encode deployment policy: %w", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, encoded); err != nil {
		return "", err
	}
	return compact.String(), nil
}

// Parse decodes a --deployment-policy value. Empty is no policy (nil); unknown
// fields are rejected.
func Parse(encoded string) (*contracts.DeploymentPolicy, error) {
	if encoded == "" {
		return nil, nil
	}
	policy := &contracts.DeploymentPolicy{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(encoded), policy); err != nil {
		return nil, fmt.Errorf("--deployment-policy: %w", err)
	}
	if err := Validate(policy); err != nil {
		return nil, err
	}
	return policy, nil
}
