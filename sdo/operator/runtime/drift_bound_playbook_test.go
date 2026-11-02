package runtime

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// Exercise the production discovery-backed read boundary. An absent object is
// a measured API 404, rather than an omitted object in a replay snapshot.
func driftBindingObservation(snapshot sdktest.Snapshot) sdk.DetectionContext {
	var objects []kruntime.Object
	for i := range snapshot.Objects {
		objects = append(objects, snapshot.Objects[i].DeepCopy())
	}
	resources := []*metav1.APIResourceList{{GroupVersion: "rbac.authorization.k8s.io/v1", APIResources: []metav1.APIResource{{Name: "clusterrolebindings", Kind: "ClusterRoleBinding", Verbs: []string{"get", "list"}}}}}
	return core.DetectionSnapshot{NamespaceName: snapshot.NamespaceName, GenericCapture: &core.GenericCapture{
		Namespace: snapshot.NamespaceName,
		Client:    dynamicfake.NewSimpleDynamicClient(kruntime.NewScheme(), objects...),
		Discovery: &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}},
	}}
}
