package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// stageHoldsData skips a match whose plan would replace or remove the Pods of a
// workload the accepted application map marks holds_data. A learned program
// binds from a failing path, so it can pick the store behind a stall as easily
// as the service in front of it, and a restart may lose what the store held.
// The program does not know; a responder can look first and act by hand.
const stageHoldsData = "holds_data"

// acceptedDataWorkloads reads the holds_data workloads ("Kind/name") of the
// accepted application map. No map marks nothing, and an unreadable or malformed
// one is the same: the shared check keeps a broken map out of accepted memory.
func acceptedDataWorkloads(ctx context.Context, read gitRead, repository, commit string) map[string]bool {
	if repository == "" || commit == "" {
		return nil
	}
	document, err := read(ctx, repository, "show", commit+":"+layout.ArchitectureFile)
	if err != nil {
		return nil
	}
	region, found, err := appmap.Find(string(document))
	if !found {
		return nil
	}
	var parsed appmap.Map
	if err == nil {
		parsed, err = appmap.Parse(region.YAML)
	}
	if err != nil {
		slog.Warn("application map unreadable; no workload is treated as holding data", "commit", commit, "error", err)
		return nil
	}
	workloads := map[string]bool{}
	for _, component := range parsed.Components {
		if component.HoldsData {
			workloads[component.Workload] = true
		}
	}
	return workloads
}

// dataWorkloadWrite explains why plan may not run on the automatic path, or
// returns "". Every action that restarts, removes, scales down or rewrites a
// data-holding workload or one of its Pods is refused. Exec and RunScript keep
// the Pods as far as the plan can tell (their commands are opaque), and creating
// or scaling up from zero loses nothing.
func dataWorkloadWrite(view sdk.DetectionContext, plan program.Plan, workloads map[string]bool, namespace string) string {
	if len(workloads) == 0 {
		return ""
	}
	for _, action := range plan.Actions {
		ref, verb := podReplacingWrite(view, action)
		if verb == "" || (ref.Namespace != "" && ref.Namespace != namespace) {
			continue
		}
		if workload := owningWorkload(view, ref); workloads[workload] {
			return fmt.Sprintf("%s of %s/%s refused on the automatic path: the application map marks %s holds_data, and replacing its Pods may lose its data; check what it holds, then act by hand if that is right", verb, ref.Kind, ref.Name, workload)
		}
	}
	return ""
}

// podReplacingWrite names the object an action would replace or remove Pods of.
// An empty verb means it would not.
func podReplacingWrite(view sdk.DetectionContext, action program.Action) (sdk.ObjectRef, string) {
	value := reflect.ValueOf(action)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		action = value.Elem().Interface().(program.Action)
	}
	switch a := action.(type) {
	case program.RestartRollout:
		return a.Ref, "restart"
	case program.Rollback:
		return a.Ref, "rollback"
	case program.ReconcileFromSource:
		return a.Ref, "reconcile"
	case program.DeleteObject:
		return a.Ref, "delete"
	case program.DeletePod:
		return a.Ref, "delete"
	case program.RetireOldRevision:
		return a.Ref, "scale down"
	case program.ScaleWorkload:
		if scalesUp(view, a.Ref, a.Replicas) {
			return a.Ref, ""
		}
		return a.Ref, "scale"
	case program.SetField:
		if a.Path == "spec.replicas" && scalesUp(view, a.Ref, a.Value) {
			return a.Ref, ""
		}
		return a.Ref, "field write"
	case program.PatchObject:
		return a.Ref, "patch"
	case program.ApplyObject:
		return a.Ref, "apply"
	case program.KubernetesRequest:
		if a.Method == "GET" || a.Method == "HEAD" {
			return a.Ref, ""
		}
		if a.Ref.Name != "" {
			return a.Ref, "API request"
		}
		return requestTarget(a.Path), "API request"
	}
	return sdk.ObjectRef{}, ""
}

var requestTargetKinds = map[string]string{"pods": "Pod", "replicasets": "ReplicaSet", "deployments": "Deployment", "statefulsets": "StatefulSet", "daemonsets": "DaemonSet"}

// requestTarget reads the object from an API path, for the client-go style
// requests a program issues without a Ref.
func requestTarget(path string) sdk.ObjectRef {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for index := 0; index+3 < len(parts); index++ {
		if parts[index] == "namespaces" {
			return sdk.ObjectRef{Kind: requestTargetKinds[parts[index+2]], Namespace: parts[index+1], Name: parts[index+3]}
		}
	}
	return sdk.ObjectRef{}
}

// scalesUp holds only when no running Pod can go: the workload has none, or the
// plan's value is known and larger. A value from accepted source is resolved at
// apply time, so it counts only from zero.
func scalesUp(view sdk.DetectionContext, ref sdk.ObjectRef, target program.Value) bool {
	current, known := observedReplicas(view, ref)
	if !known {
		return false
	}
	if current == 0 {
		return true
	}
	if target.Origin() == program.SourceValue {
		return false
	}
	resolved, _, err := program.ResolveValue(target, program.ValueInputs{})
	if err != nil {
		return false
	}
	replicas, err := strconv.ParseInt(fmt.Sprint(resolved), 10, 32)
	return err == nil && int32(replicas) > current
}

func observedReplicas(view sdk.DetectionContext, ref sdk.ObjectRef) (int32, bool) {
	var replicas *int32
	found := false
	switch ref.Kind {
	case "Deployment":
		for _, item := range view.Deployments() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				replicas, found = item.Spec.Replicas, true
			}
		}
	case "StatefulSet":
		for _, item := range view.StatefulSets() {
			if item.Namespace == ref.Namespace && item.Name == ref.Name {
				replicas, found = item.Spec.Replicas, true
			}
		}
	}
	if !found {
		return 0, false
	}
	if replicas == nil {
		return 1, true
	}
	return *replicas, true
}

// owningWorkload walks a Pod or ReplicaSet up its controller owners to the
// "Kind/name" the application map uses.
func owningWorkload(view sdk.DetectionContext, ref sdk.ObjectRef) string {
	kind, name := ref.Kind, ref.Name
	if kind == "Pod" {
		for _, pod := range view.Pods() {
			if pod.Namespace == ref.Namespace && pod.Name == name {
				if owner := metav1.GetControllerOf(&pod); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	if kind == "ReplicaSet" {
		for _, replicaSet := range view.ReplicaSets() {
			if replicaSet.Namespace == ref.Namespace && replicaSet.Name == name {
				if owner := metav1.GetControllerOf(&replicaSet); owner != nil {
					kind, name = owner.Kind, owner.Name
				}
				break
			}
		}
	}
	return kind + "/" + name
}
