package core

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	discoveryfake "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clienttesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type fieldFaultRepair struct {
	ref      sdk.ObjectRef
	path     string
	want     any
	document bool
	touched  []sdk.ObjectRef
}

func (p fieldFaultRepair) Spec() program.ProgramSpec {
	kind := program.StringValue
	switch p.want.(type) {
	case int:
		kind = program.IntValue
	case bool:
		kind = program.BoolValue
	}
	return program.ProgramSpec{ID: "field-repair", Params: []program.ParamSpec{{Name: "desired", ValueKind: kind, Required: true}}}
}
func (p fieldFaultRepair) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	for _, ref := range p.touched {
		if _, err := v.Object(ref); err != nil {
			return program.Verdict{}, err
		}
	}
	object, err := v.Object(p.ref)
	if err != nil {
		return program.Verdict{}, err
	}
	value, err := program.FieldValue(object.Object, p.path)
	if err != nil {
		return program.Verdict{}, err
	}
	if p.document {
		var doc map[string]any
		if err := json.Unmarshal([]byte(fmt.Sprint(value)), &doc); err != nil {
			return program.Verdict{}, err
		}
		value, err = program.FieldValue(doc, "flags.queue.defaultVariant")
		if err != nil {
			return program.Verdict{}, err
		}
	}
	got, _ := json.Marshal(value)
	want, _ := json.Marshal(p.want)
	result := program.Match
	if string(got) == string(want) {
		result = program.NoMatch
	}
	return program.Verdict{Result: result}, nil
}
func (p fieldFaultRepair) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	field := program.SetField{Ref: p.ref, Path: p.path, Value: program.FromParam("desired")}
	if p.document {
		field.DocumentFormat = "json"
		field.DocumentPath = []string{"flags", "queue", "defaultVariant"}
	}
	return program.Plan{Actions: []program.Action{field}}, nil
}

// Hand-written mutations grounded in benchmarks/sregym/faults and the pinned
// harness injection code. No harness, cluster, model, grader or oracle is run.
func TestFirstFourProblemObjectFaultClassesCaptureAndReplay(t *testing.T) {
	deployment := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app"},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"api"}},"template":{"metadata":{"labels":{"app":"api"}},"spec":{"dnsPolicy":"Default","nodeName":"missing","containers":[{"name":"api","image":"synthetic:broken","command":["wrong"],"ports":[{"containerPort":1}],"resources":{"requests":{"memory":"100Gi"}},"readinessProbe":{"httpGet":{"path":"/wrong","port":80}}}],"initContainers":[{"name":"wait","command":["sleep","infinity"]}],"volumes":[{"name":"data","persistentVolumeClaim":{"claimName":"missing"}}]}}}}`
	claim := `{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":"data","namespace":"app"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"1Gi"}}}}`
	webhook := `{"apiVersion":"admissionregistration.k8s.io/v1","kind":"ValidatingWebhookConfiguration","metadata":{"name":"policy"},"webhooks":[{"name":"policy.example","clientConfig":{"caBundle":"synthetic-wrong-ca","service":{"name":"policy","namespace":"policy-system"}},"failurePolicy":"Fail"}]}`
	backend := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"policy","namespace":"policy-system"},"spec":{"replicas":0,"selector":{"matchLabels":{"app":"policy"}},"template":{"metadata":{"labels":{"app":"policy"}},"spec":{"containers":[{"name":"policy","image":"synthetic:policy"}]}}}}`
	flags := `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"flags","namespace":"app"},"data":{"flags.json":"{\"flags\":{\"queue\":{\"defaultVariant\":\"on\",\"variants\":{\"on\":true,\"off\":false}}}}"}}`
	for _, tc := range []struct {
		name     string
		raw      []string
		target   int
		path     string
		want     any
		document bool
	}{
		{"PVC_created_and_duplicate_mounts", []string{claim, deployment}, 1, "/spec/replicas", 1, false},
		{"PVC_claim_mismatch", []string{claim, deployment}, 1, "/spec/template/spec/volumes/0/persistentVolumeClaim/claimName", "data", false},
		{"webhook_TLS", []string{webhook, backend}, 0, "/webhooks/0/clientConfig/caBundle", "synthetic-good-ca", false},
		{"webhook_outage", []string{webhook, backend}, 1, "/spec/replicas", 1, false},
		{"ConfigMap_flag", []string{flags}, 0, "data[flags.json]", "off", true},
		{"workload_image", []string{deployment}, 0, "/spec/template/spec/containers/0/image", "synthetic:healthy", false},
		{"workload_readiness", []string{deployment}, 0, "/spec/template/spec/containers/0/readinessProbe/httpGet/path", "/ready", false},
		{"workload_resource_request", []string{deployment}, 0, "/spec/template/spec/containers/0/resources/requests/memory", "128Mi", false},
		{"workload_wrong_binary", []string{deployment}, 0, "/spec/template/spec/containers/0/command/0", "server", false},
		{"workload_dns", []string{deployment}, 0, "/spec/template/spec/dnsPolicy", "ClusterFirst", false},
		{"workload_node", []string{deployment}, 0, "/spec/template/spec/nodeName", "", false},
		{"workload_init_dependency", []string{deployment}, 0, "/spec/template/spec/initContainers/0/command/0", "true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var objects []kruntime.Object
			var refs []sdk.ObjectRef
			var resources []*metav1.APIResourceList
			kinds := map[schema.GroupVersionResource]string{}
			seen := map[string]bool{}
			for _, raw := range tc.raw {
				object := new(unstructured.Unstructured)
				if err := json.Unmarshal([]byte(raw), object); err != nil {
					t.Fatal(err)
				}
				objects = append(objects, object)
				refs = append(refs, sdk.ObjectIdentity(*object))
				gvr, _ := meta.UnsafeGuessKindToResource(object.GroupVersionKind())
				kinds[gvr] = object.GetKind() + "List"
				if !seen[gvr.String()] {
					resources = append(resources, &metav1.APIResourceList{GroupVersion: object.GetAPIVersion(), APIResources: []metav1.APIResource{{Name: gvr.Resource, Kind: object.GetKind(), Namespaced: object.GetNamespace() != "", Verbs: []string{"get", "list", "patch"}}}})
					seen[gvr.String()] = true
				}
			}
			fake := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(kruntime.NewScheme(), kinds, objects...)
			discover := &discoveryfake.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
			snapshot, err := sdktest.CaptureSnapshot(DetectionSnapshot{NamespaceName: "app", GenericCapture: &GenericCapture{Namespace: "app", Client: fake, Discovery: discover}}, "app", refs...)
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range refs {
				if _, err := snapshot.Object(ref); err != nil {
					t.Fatalf("mutated object missing: %v", err)
				}
			}
			repair := fieldFaultRepair{ref: refs[tc.target], path: tc.path, want: tc.want, document: tc.document, touched: refs}
			bindings := program.Bindings{Values: map[string]any{"desired": tc.want}}
			before := program.RunDiagnose(context.Background(), repair, &sdktest.ReplayReadView{Snapshot: snapshot}, bindings, time.Second)
			if before.Result != program.Match {
				t.Fatalf("fault not visible: %+v", before)
			}
			result, err := sdktest.CheckMitigation(repair, repair, sdktest.CorpusCase{Open: snapshot, Bindings: bindings})
			if err != nil || result.Status != sdktest.MitigationReplayPassed {
				t.Fatalf("repair replay: %+v %v", result, err)
			}
		})
	}
}
