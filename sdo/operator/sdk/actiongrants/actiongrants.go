// Package actiongrants states the Kubernetes RBAC each program plan action
// needs. It is kept out of package program so the authoring SDK reference
// stays about what programs write; the installer and its ratchet read it.
package actiongrants

// Permission is one Kubernetes RBAC need of a plan action: the verbs the
// controller performs on a resource, in the incident namespace.
type Permission struct {
	Group    string
	Resource string
	Verbs    []string
}

// Controller is the ServiceAccount in the control namespace that performs every
// program plan action: the controller Job runs as it and the exec transport is
// its in-cluster config.
const Controller = "sdo-controller"

// ActionPermission states what one ServiceAccount's Roles must hold in the
// application namespace for one action type. Identity is the ServiceAccount
// that performs the action; Needs lists the kinds and subresources the typed
// plan client handles by name; OpenEnded marks an action whose target kind is
// chosen by the program, so only the API server's RBAC can authorize it and
// the installer cannot promise it. Every entry has Needs or OpenEnded.
type ActionPermission struct {
	Identity  string
	Needs     []Permission
	OpenEnded string
}

func each(group string, resources []string, verbs ...string) []Permission {
	out := make([]Permission, 0, len(resources))
	for _, resource := range resources {
		out = append(out, Permission{Group: group, Resource: resource, Verbs: verbs})
	}
	return out
}

func join(lists ...[]Permission) []Permission {
	var out []Permission
	for _, list := range lists {
		out = append(out, list...)
	}
	return out
}

// structuredKinds are the kinds a typed plan action may name: the kinds
// TypedPlanClient reads and writes without the generic request path.
func structuredKinds(verbs ...string) []Permission {
	return join(
		each("", []string{"configmaps", "services", "pods"}, verbs...),
		each("apps", []string{"deployments", "statefulsets"}, verbs...),
		each("networking.k8s.io", []string{"networkpolicies"}, verbs...),
		each("autoscaling", []string{"horizontalpodautoscalers"}, verbs...),
		each("policy", []string{"poddisruptionbudgets"}, verbs...),
	)
}

func workloadKinds(verbs ...string) []Permission {
	return join(each("apps", []string{"deployments", "statefulsets", "replicasets"}, verbs...))
}

// execKinds is the exec transport: the controller reads the Pod and opens a
// pods/exec stream to it (Exec, EnsureExec, EnsureHTTP's curl, database users).
func execKinds() []Permission {
	return join(each("", []string{"pods"}, "get"), each("", []string{"pods/exec"}, "get", "create"))
}

// jobKinds is what RunJob and RunScript need from the controller itself: the
// Job runs as its own ServiceAccount, but the controller creates, watches and
// deletes it and reads its Pod logs and script ConfigMap.
func jobKinds() []Permission {
	return join(
		each("batch", []string{"jobs"}, "get", "list", "watch", "create", "delete"),
		each("", []string{"configmaps"}, "get", "create", "update", "patch"),
		each("", []string{"pods/log"}, "get"),
	)
}

const openEnded = "the program names any kind; the API server's RBAC is the only authority"

// ActionPermissions has one entry for every Action type in package program. The
// installer derives the controller Role from it, and a test fails when a new
// action type has no entry, so a vocabulary addition cannot ship ungranted.
var ActionPermissions = withIdentity(map[string]ActionPermission{
	"CreateFromSource":    {Needs: structuredKinds("get", "create")},
	"DeleteObject":        {Needs: join(structuredKinds("get", "delete"), each("apps", []string{"replicasets", "daemonsets"}, "get", "delete"))},
	"ReconcileFromSource": {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"Rollback":            {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"SetField":            {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"RemoveServicePort":   {Needs: each("", []string{"services"}, "get", "patch")},
	"ScaleWorkload":       {Needs: workloadKinds("get", "patch")},
	"RestartRollout":      {Needs: each("apps", []string{"deployments"}, "get", "patch")},
	"DeletePod":           {Needs: each("", []string{"pods"}, "get", "delete")},
	"Exec":                {Needs: execKinds()},
	"EnsureDatabaseUser":  {Needs: execKinds()},
	"EnsureExec":          {Needs: execKinds()},
	"EnsureHTTP":          {Needs: execKinds()},
	"RunJob":              {Needs: jobKinds()},
	"RunScript":           {Needs: jobKinds()},
	"PatchObject":         {OpenEnded: openEnded},
	"ApplyObject":         {OpenEnded: openEnded},
	"KubernetesRequest":   {OpenEnded: openEnded},
	"WaitFor":             {OpenEnded: openEnded},
})

func withIdentity(entries map[string]ActionPermission) map[string]ActionPermission {
	for name, entry := range entries {
		entry.Identity = Controller
		entries[name] = entry
	}
	return entries
}
