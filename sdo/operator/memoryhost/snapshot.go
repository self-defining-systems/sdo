package memoryhost

import (
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sort"
)

// Freeze copies all supported informer kinds. Secrets expose SDK metadata only.
func Freeze(view sdk.DetectionContext) sdktest.Snapshot {
	return sdktest.Snapshot{
		NamespaceName: view.Namespace(), ConfigMapList: view.ConfigMaps(), ServiceList: view.Services(),
		PodList: view.Pods(), DeploymentList: view.Deployments(), StatefulSetList: view.StatefulSets(),
		ReplicaSetList: view.ReplicaSets(), HPAList: view.HorizontalPodAutoscalers(), PDBList: view.PodDisruptionBudgets(),
		EndpointList: view.Endpoints(), EndpointSliceList: view.EndpointSlices(), NetworkPolicyList: view.NetworkPolicies(),
		EventList: view.Events(), DaemonSetList: view.DaemonSets(), JobList: view.Jobs(), CronJobList: view.CronJobs(),
		PVCList: view.PersistentVolumeClaims(), ServiceAccountList: view.ServiceAccounts(),
		IngressList: view.Ingresses(), NodeList: view.Nodes(), SecretList: view.Secrets(),
	}
}

// mirrorObjects converts the SDK projection, never raw Secret bodies.
func mirrorObjects(view sdk.DetectionContext) (map[string]*pb.MemoryObject, error) {
	if view.Namespace() == "" {
		return nil, errors.New("mirror namespace required")
	}
	data, err := json.Marshal(Freeze(view))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	objects := map[string]*pb.MemoryObject{}
	kinds := make([]string, 0, len(snapshotFields))
	for kind := range snapshotFields {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		var items []map[string]any
		// The snapshot omits empty kinds.
		if raw := fields[snapshotFields[kind]]; len(raw) > 0 {
			if err = json.Unmarshal(raw, &items); err != nil {
				return nil, err
			}
		}
		for _, item := range items {
			metadata, _ := item["metadata"].(map[string]any)
			if kind == "Secret" {
				metadata = item
			}
			name, _ := metadata["name"].(string)
			namespace, _ := metadata["namespace"].(string)
			if name == "" || (namespace != view.Namespace() && !(kind == "Node" && namespace == "")) {
				return nil, errors.New("invalid mirror object identity or namespace")
			}
			api, _ := item["apiVersion"].(string)
			if api == "" {
				api = kindAPIVersion(kind)
			}
			ref := &pb.ObjectRef{ApiVersion: api, Kind: kind, Namespace: namespace, Name: name}
			body, e := structpb.NewStruct(item)
			if e != nil {
				return nil, e
			}
			key := objectKey(ref)
			if objects[key] != nil {
				return nil, errors.New("duplicate mirror object")
			}
			objects[key] = &pb.MemoryObject{Ref: ref, Object: body}
		}
	}
	return objects, nil
}

// SnapshotRefs lists the read-only projection in stable order for diff inputs.
func SnapshotRefs(view sdk.DetectionContext) ([]sdk.ObjectRef, error) {
	objects, err := mirrorObjects(view)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(objects))
	for key := range objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var refs []sdk.ObjectRef
	for _, key := range keys {
		r := objects[key].Ref
		refs = append(refs, sdk.ObjectRef{APIVersion: r.ApiVersion, Kind: r.Kind, Namespace: r.Namespace, Name: r.Name})
	}
	return refs, nil
}

func kindAPIVersion(kind string) string {
	switch kind {
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
		return "apps/v1"
	case "Job", "CronJob":
		return "batch/v1"
	case "Ingress", "NetworkPolicy":
		return "networking.k8s.io/v1"
	case "EndpointSlice":
		return "discovery.k8s.io/v1"
	case "PodDisruptionBudget":
		return "policy/v1"
	case "HorizontalPodAutoscaler":
		return "autoscaling/v2"
	default:
		return "v1"
	}
}
