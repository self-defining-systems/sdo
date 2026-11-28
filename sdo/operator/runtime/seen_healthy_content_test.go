package runtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	controllercore "sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
)

// The objects as the API server serves them (it omits what the API types omit),
// with the empty and server-populated fields a read could treat differently.
const servedDeployment = `{"apiVersion":"apps/v1","kind":"Deployment",
"metadata":{"name":"web","namespace":"app","uid":"u1","resourceVersion":"7","generation":2,"creationTimestamp":"2026-01-01T00:00:00Z",
 "labels":{"app":"web"},"annotations":{"deployment.kubernetes.io/revision":"2","note":""}},
"spec":{"replicas":0,"selector":{"matchLabels":{"app":"web"}},"strategy":{},
 "template":{"metadata":{"creationTimestamp":null,"labels":{"app":"web"}},
  "spec":{"securityContext":{},"terminationGracePeriodSeconds":30,
   "volumes":[{"name":"tmp","emptyDir":{}}],
   "containers":[{"name":"web","image":"example/web:1","env":[{"name":"EMPTY"}],
    "ports":[{"containerPort":8080,"protocol":"TCP"}],
    "resources":{"limits":{"cpu":"500m","memory":"1Gi"}},
    "readinessProbe":{"httpGet":{"path":"/","port":8080},"periodSeconds":10}},
    {"name":"sidecar","image":"example/sidecar:1","resources":{}}]}}},
"status":{"replicas":1,"observedGeneration":2}}`

const servedConfigMap = `{"apiVersion":"v1","kind":"ConfigMap",
"metadata":{"name":"settings","namespace":"app","uid":"u2","resourceVersion":"9","creationTimestamp":"2026-01-01T00:00:00Z"},
"data":{"mode":"on","empty":""},"binaryData":{"blob":"AAEC"}}`

const servedNetworkPolicy = `{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy",
"metadata":{"name":"open","namespace":"app","uid":"u3","resourceVersion":"3","creationTimestamp":"2026-01-01T00:00:00Z"},
"spec":{"podSelector":{},"ingress":[{},{"from":[{"namespaceSelector":{}}]}],"policyTypes":["Ingress"]}}`

// The stretch start and the later check may read an object from the snapshot or
// with a GET. Both must give the same content digest, or the seen-healthy
// exemption silently never applies.
func TestSnapshotAndDirectReadsHaveTheSameContent(t *testing.T) {
	served := map[string]string{
		"/api/v1":                    `{"kind":"APIResourceList","groupVersion":"v1","resources":[{"name":"configmaps","kind":"ConfigMap","namespaced":true}]}`,
		"/apis/networking.k8s.io/v1": `{"kind":"APIResourceList","groupVersion":"networking.k8s.io/v1","resources":[{"name":"networkpolicies","kind":"NetworkPolicy","namespaced":true}]}`,
		"/apis/networking.k8s.io/v1/namespaces/app/networkpolicies/open": servedNetworkPolicy,
		"/apis/apps/v1": `{"kind":"APIResourceList","groupVersion":"apps/v1","resources":[{"name":"deployments","kind":"Deployment","namespaced":true}]}`,
		"/apis/apps/v1/namespaces/app/deployments/web": servedDeployment,
		"/api/v1/namespaces/app/configmaps/settings":   servedConfigMap,
	}
	client, err := kubernetes.NewForConfig(&rest.Config{Host: "http://api.invalid", Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, found := served[r.URL.Path]
		if !found {
			t.Errorf("unexpected request %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	// The informer keeps typed objects with no TypeMeta.
	var deployment appsv1.Deployment
	var configMap corev1.ConfigMap
	var policy networkingv1.NetworkPolicy
	if err := json.Unmarshal([]byte(servedNetworkPolicy), &policy); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(servedDeployment), &deployment); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(servedConfigMap), &configMap); err != nil {
		t.Fatal(err)
	}
	deployment.TypeMeta, configMap.TypeMeta, policy.TypeMeta = metav1.TypeMeta{}, metav1.TypeMeta{}, metav1.TypeMeta{}
	view := func(snapshot controllercore.DetectionSnapshot) LiveReadView {
		snapshot.NamespaceName = "app"
		return LiveReadView{Carrier: inputs.New(snapshot), Context: context.Background(), Client: client}
	}
	captured := view(controllercore.DetectionSnapshot{DeploymentList: []appsv1.Deployment{deployment}, ConfigMapList: []corev1.ConfigMap{configMap}, NetworkPolicyList: []networkingv1.NetworkPolicy{policy}})
	uncaptured := view(controllercore.DetectionSnapshot{})
	for _, ref := range []sdk.ObjectRef{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "settings"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "open"},
	} {
		fromSnapshot, direct := objectContent(captured, ref), objectContent(uncaptured, ref)
		if !strings.HasPrefix(direct, "sha256:") || fromSnapshot != direct {
			t.Errorf("%s content: snapshot read %q, direct read %q", ref.Kind, fromSnapshot, direct)
		}
	}
}

type objectJSON string

func (o objectJSON) Object(sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object := &unstructured.Unstructured{}
	return object, object.UnmarshalJSON([]byte(o))
}

// An empty object the API keeps carries meaning ({} selects everything where
// absence selects nothing or narrows), so it is content. An empty value the API
// omits on read is the same as absence.
func TestContentKeepsEmptyValuesThatCarryMeaning(t *testing.T) {
	policy := func(spec string) string {
		return `{"apiVersion":"networking.k8s.io/v1","kind":"NetworkPolicy","metadata":{"name":"p"},"spec":` + spec + `}`
	}
	from := func(peer string) string { return policy(`{"podSelector":{},"ingress":[{"from":[` + peer + `]}]}`) }
	budget := func(spec string) string {
		return `{"apiVersion":"policy/v1","kind":"PodDisruptionBudget","metadata":{"name":"b"},"spec":` + spec + `}`
	}
	custom := func(spec string) string {
		return `{"apiVersion":"example.invalid/v1","kind":"Widget","metadata":{"name":"w"},"spec":` + spec + `}`
	}
	for name, c := range map[string]struct {
		left, right string
		same        bool
	}{
		"all pods here or in every namespace": {left: from(`{"podSelector":{}}`), right: from(`{"namespaceSelector":{}}`)},
		"every namespace or this one": {
			left:  from(`{"podSelector":{"matchLabels":{"a":"b"}},"namespaceSelector":{}}`),
			right: from(`{"podSelector":{"matchLabels":{"a":"b"}}}`),
		},
		"allow all or deny all":      {left: policy(`{"podSelector":{},"ingress":[{}]}`), right: policy(`{"podSelector":{}}`)},
		"budget selects all or none": {left: budget(`{"minAvailable":1,"selector":{}}`), right: budget(`{"minAvailable":1}`)},
		"explicit zero pointer":      {left: budget(`{"minAvailable":1,"maxUnavailable":0}`), right: budget(`{"minAvailable":1}`)},
		"custom empty object":        {left: custom(`{"selector":{}}`), right: custom(`{}`)},
		"custom empty list":          {left: custom(`{"allow":[]}`), right: custom(`{}`)},
		"omitted empty list":         {left: policy(`{"podSelector":{},"ingress":[]}`), right: policy(`{"podSelector":{}}`), same: true},
	} {
		left, right := objectContent(objectJSON(c.left), sdk.ObjectRef{}), objectContent(objectJSON(c.right), sdk.ObjectRef{})
		if !strings.HasPrefix(left, "sha256:") || (left == right) != c.same {
			t.Errorf("%s: same=%v, digests %q and %q", name, c.same, left, right)
		}
	}
}
