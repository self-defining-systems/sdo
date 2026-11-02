package kubereads

import (
	"errors"
	"fmt"
	"strings"

	"sdo.dev/operator/sdk/program"
)

// ErrOutOfScope marks a read outside the namespace the caller may observe.
var ErrOutOfScope = errors.New("read is outside the observed namespace")

// clusterScoped lists the cluster-scoped resources a namespace-scoped reader may
// read by path. Any other resource must name its namespace, so an
// all-namespace list can never reach the control namespace.
var clusterScoped = map[string]bool{
	"nodes": true, "persistentvolumes": true, "storageclasses": true, "apiservices": true,
	"customresourcedefinitions": true, "clusterroles": true, "clusterrolebindings": true,
	"mutatingwebhookconfigurations": true, "validatingwebhookconfigurations": true,
	"validatingadmissionpolicies": true, "validatingadmissionpolicybindings": true,
	"priorityclasses": true, "runtimeclasses": true, "ingressclasses": true, "csidrivers": true,
	"csinodes": true, "volumeattachments": true, "certificatesigningrequests": true,
	"componentstatuses": true,
}

// ScopeRead confines a read request to namespace and refuses the control
// namespace. Discovery paths are allowed. Namespaced resources must name the
// observed namespace; cluster-scoped ones must be on the known list.
func ScopeRead(r program.KubernetesRequest, namespace, protectedNamespace string) error {
	parts := strings.Split(strings.TrimPrefix(r.Path, "/"), "/")
	index := 2
	if parts[0] == "apis" {
		index = 3
	}
	if len(parts) <= index {
		return nil
	}
	if parts[index] == "watch" {
		index++
	}
	if len(parts) <= index {
		return nil
	}
	if parts[index] == "namespaces" {
		if len(parts) <= index+1 {
			return fmt.Errorf("%w: listing namespaces is unavailable", ErrOutOfScope)
		}
		name := parts[index+1]
		if protectedNamespace != "" && name == protectedNamespace {
			return fmt.Errorf("%w: the SDO control namespace %q is refused", ErrOutOfScope, protectedNamespace)
		}
		if len(parts) > index+2 && name != namespace {
			return fmt.Errorf("%w: namespace %q is not %q", ErrOutOfScope, name, namespace)
		}
		return nil
	}
	if !clusterScoped[parts[index]] {
		return fmt.Errorf("%w: %s must name namespace %q", ErrOutOfScope, parts[index], namespace)
	}
	return nil
}
