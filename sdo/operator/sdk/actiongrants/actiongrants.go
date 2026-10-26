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

// ActionPermission states what the controller's Role must hold for one action
// type. Needs lists the kinds the typed plan client handles by name; OpenEnded
// marks an action whose target kind is chosen by the program, so only the API
// server's RBAC can authorize it and the installer cannot promise it.
type ActionPermission struct {
	Needs     []Permission
	OpenEnded string
	// Elsewhere names the grant that carries this action when it is not a
	// controller Role verb (an Exec transport, a script ServiceAccount).
	Elsewhere string
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

const openEnded = "the program names any kind; the API server's RBAC is the only authority"

// ActionPermissions has one entry for every Action type in package program. The
// installer derives the controller Role from it, and a test fails when a new
// action type has no entry, so a vocabulary addition cannot ship ungranted.
var ActionPermissions = map[string]ActionPermission{
	"CreateFromSource":    {Needs: structuredKinds("get", "create")},
	"DeleteObject":        {Needs: join(structuredKinds("get", "delete"), each("apps", []string{"replicasets", "daemonsets"}, "get", "delete"))},
	"ReconcileFromSource": {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"Rollback":            {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"SetField":            {Needs: join(structuredKinds("get", "patch"), each("apps", []string{"daemonsets"}, "get", "patch"))},
	"RemoveServicePort":   {Needs: each("", []string{"services"}, "get", "patch")},
	"ScaleWorkload":       {Needs: workloadKinds("get", "patch")},
	"RestartRollout":      {Needs: each("apps", []string{"deployments"}, "get", "patch")},
	"DeletePod":           {Needs: each("", []string{"pods"}, "get", "delete")},
	"Exec":                {Elsewhere: "controller pods/exec transport"},
	"EnsureDatabaseUser":  {Elsewhere: "controller pods/exec transport"},
	"EnsureExec":          {Elsewhere: "controller pods/exec transport"},
	"EnsureHTTP":          {Elsewhere: "in-cluster HTTP from the controller; no API verb"},
	"RunJob":              {Elsewhere: "sdo-script-controller Role (jobs) and the sdo-script ServiceAccount"},
	"RunScript":           {Elsewhere: "sdo-script-controller Role (jobs) and the sdo-script ServiceAccount"},
	"PatchObject":         {OpenEnded: openEnded},
	"ApplyObject":         {OpenEnded: openEnded},
	"KubernetesRequest":   {OpenEnded: openEnded},
	"WaitFor":             {OpenEnded: openEnded},
}
