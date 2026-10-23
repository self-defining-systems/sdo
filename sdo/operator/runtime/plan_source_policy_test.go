package runtime

import (
	"context"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"net/http"
	"net/http/httptest"
	"os"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
	"time"
)

type sourcePolicyFixture struct {
	lifecycleTestSource
	accepted []*unstructured.Unstructured
}

func (s sourcePolicyFixture) AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error) {
	return s.accepted, nil
}
func TestStreamSourcePolicyEnforcesSourceAndPreservesLegacyLiterals(t *testing.T) {
	cfg, err := LoadSourcePlanPolicy(`{"version":1,"authority":"accepted-source-and-incident","applications":{"fixture":"app"},"field_repairs":"accepted-source-only","create_kinds":["v1/ConfigMap","v1/Service"],"delete_kinds":["networking.k8s.io/v1/NetworkPolicy"],"ttl_seconds":3600}`)
	if err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	accepted := planObject(ref, map[string]any{"replicas": float64(1), "template": map[string]any{"spec": map[string]any{"automountServiceAccountToken": false, "containers": []any{map[string]any{"name": "api", "args": []any{"source"}, "resources": map[string]any{"limits": map[string]any{"memory": "64Mi"}}}}}}})
	source := sourcePolicyFixture{accepted: []*unstructured.Unstructured{accepted}, lifecycleTestSource: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: accepted}}}
	incident := IncidentRequest{IncidentID: "fixture", Application: "fixture", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions", ResponseDeadline: time.Now().Add(time.Hour)}
	authority := &SourcePlanAuthority{Config: cfg, Source: source, UID: func(context.Context, sdk.ObjectRef) (types.UID, error) { return "fixture-uid", nil }}
	for _, test := range []struct {
		name, path string
		value      any
		allowed    bool
	}{
		{"source", "spec.template.spec.containers[name=api].args", []string{"source"}, true},
		{"structured literal refused", "spec.template.spec.containers[name=api].args", []string{"unreviewed"}, false},
		{"legacy resource literal remains", "spec.template.spec.containers[0].resources.limits", map[string]any{"memory": "128Mi"}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := accepted.DeepCopy()
			before.SetUID("fixture-uid")
			before.SetResourceVersion("1")
			_ = unstructured.SetNestedSlice(before.Object, []any{map[string]any{"name": "api", "args": []any{"broken"}, "resources": map[string]any{"limits": map[string]any{"memory": "64Mi"}}}}, "spec", "template", "spec", "containers")
			client := &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}
			e := PlanExecutor{Client: client, Source: source, SourceAuthority: authority, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			result, err := e.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.SetField{Ref: ref, Path: test.path, Value: test.value}}}, false)
			if (err == nil) != test.allowed || (!test.allowed && len(client.patches) != 0) {
				t.Fatalf("allowed=%v err=%v", test.allowed, err)
			}
			if test.allowed {
				mode := "accepted-source"
				if test.name == "legacy resource literal remains" {
					mode = "legacy-literal"
				}
				if result.Patches[0].Authorization != mode {
					t.Fatal("literal/source authority split was not recorded")
				}
			}
			if test.allowed && (!result.VerificationPassed || len(client.patches) != 1) {
				t.Fatal("authorized repair did not execute")
			}
		})
	}
}

func TestStreamSourcePolicyCreatesAndPinsOnlyIncidentNamedDeletes(t *testing.T) {
	cfg, _ := LoadSourcePlanPolicy(`{"version":1,"authority":"accepted-source-and-incident","applications":{"fixture":"app"},"field_repairs":"accepted-source-only","create_kinds":["v1/ConfigMap"],"delete_kinds":["networking.k8s.io/v1/NetworkPolicy"],"ttl_seconds":3600}`)
	cm := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "missing"}
	np := sdk.ObjectRef{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "app", Name: "foreign"}
	unnamed := np
	unnamed.Name = "not-in-incident"
	accepted := planObject(cm, nil)
	delete(accepted.Object, "spec")
	accepted.Object["data"] = map[string]any{"key": "safe"}
	source := sourcePolicyFixture{accepted: []*unstructured.Unstructured{accepted}, lifecycleTestSource: lifecycleTestSource{objects: map[sdk.ObjectRef]*unstructured.Unstructured{cm: accepted}}}
	incident := IncidentRequest{IncidentID: "fixture", Application: "fixture", Namespace: "app", SourceCommit: "accepted", RepairPolicy: "recorded-actions", Findings: []sdk.Finding{{PrimaryResource: np}}}
	authority := &SourcePlanAuthority{Config: cfg, Source: source, UID: func(_ context.Context, ref sdk.ObjectRef) (types.UID, error) {
		if ref == cm {
			return "", apierrors.NewNotFound(schema.GroupResource{Resource: "configmaps"}, cm.Name)
		}
		if ref != np {
			t.Fatal("metadata read outside named incident/source scope")
		}
		return "policy-uid", nil
	}}
	policy, err := authority.Policy(context.Background(), incident)
	if err != nil {
		t.Fatal(err)
	}
	if policy.Objects[unnamed].DeleteAllowed || !policy.Objects[np].DeleteAllowed || policy.Objects[np].UID != "policy-uid" {
		t.Fatal("delete was not individually bound")
	}
	object := planObject(np, map[string]any{"podSelector": map[string]any{}})
	object.SetUID("policy-uid")
	object.SetResourceVersion("1")
	client := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{np: object}}}
	e := PlanExecutor{Client: client, Source: source, SourceAuthority: authority, AcceptedRevision: func() string { return "accepted" }, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
	result, err := e.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.CreateFromSource{Ref: cm, Reason: "fixture"}, program.DeleteObject{Ref: np, Reason: "fixture"}}}, false)
	if err != nil || !result.VerificationPassed || client.creates != 1 || client.deletes != 1 {
		t.Fatalf("source lifecycle actions unavailable: %v", err)
	}
	if result.Patches[0].Authorization != "accepted-source" || result.Patches[1].Authorization != "incident-named-delete" {
		t.Fatal("source and delete authority labels missing")
	}
	// Metadata re-reads must not refresh an incident's UID after recreation.
	replacement := object.DeepCopy()
	replacement.SetUID("replacement-uid")
	client.objects[np] = replacement
	_, err = e.Apply(context.Background(), incident, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: np, Reason: "fixture"}}}, false)
	if err == nil || client.deletes != 1 {
		t.Fatal("recreated UID acquired deletion authority")
	}
	foreign := incident
	foreign.Namespace = "foreign"
	if _, err := (&SourcePlanAuthority{Config: cfg, Source: source, UID: authority.UID}).Policy(context.Background(), foreign); err == nil {
		t.Fatal("foreign namespace accepted")
	}
}

func TestStreamPolicyMetadataReaderNeverFallsBackToOpaqueOrSecretReads(t *testing.T) {
	var requests int
	fullReply := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/namespaces/app/configmaps/flags" || r.Header.Get("Accept") != "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1" {
			t.Error("metadata transport widened")
		}
		w.Header().Set("Content-Type", "application/json")
		if fullReply {
			_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"flags","namespace":"app","uid":"fixture-uid"},"data":{"placeholder":"placeholder"}}`)
		} else {
			_, _ = io.WriteString(w, `{"apiVersion":"meta.k8s.io/v1","kind":"PartialObjectMetadata","metadata":{"name":"flags","namespace":"app","uid":"fixture-uid"}}`)
		}
	}))
	defer server.Close()
	config := metadata.ConfigFor(&rest.Config{Host: server.URL})
	config.GroupVersion = &schema.GroupVersion{Version: "v1"}
	config.APIPath = "/api"
	client, err := rest.RESTClientFor(config)
	if err != nil {
		t.Fatal(err)
	}
	read := sourcePolicyUIDReader(client)
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "flags"}
	if uid, err := read(context.Background(), ref); err != nil || uid != "fixture-uid" {
		t.Fatalf("metadata identity failed: %v", err)
	}
	fullReply = true
	if _, err := read(context.Background(), ref); err == nil {
		t.Fatal("full object response accepted as metadata")
	}
	for _, forbidden := range []sdk.ObjectRef{{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "placeholder"}, {APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "../../secrets/placeholder"}} {
		if _, err := read(context.Background(), forbidden); err == nil {
			t.Fatal("forbidden metadata request accepted")
		}
	}
	if requests != 2 {
		t.Fatal("forbidden kind or invalid name reached API")
	}
}

func TestCommittedMixed10SourcePolicyScope(t *testing.T) {
	raw, err := os.ReadFile("../config/plan-policies/mixed10.json")
	if err != nil {
		t.Fatal(err)
	}
	args := controllerConfigToArgs(&contracts.ControllerConfig{PlanSourcePolicyJson: string(raw)})
	if len(args) != 2 || args[0] != "--plan-source-policy-json" || args[1] != string(raw) {
		t.Fatal("typed launch lost source policy")
	}
	c, err := LoadSourcePlanPolicy(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Applications) != 3 || c.Applications["astronomy_shop"] != "astronomy-shop" || c.Applications["hotel_reservation"] != "hotel-res" || c.Applications["social_network"] != "social-network" {
		t.Fatal("mixed10 scope changed")
	}
	for _, bad := range []string{`{"Plan":{},"DryRun":true}`, strings.Replace(string(raw), "accepted-source-only", "responder-literals", 1), strings.Replace(string(raw), "v1/ConfigMap", "v1/Secret", 1)} {
		if _, err := LoadSourcePlanPolicy(bad); err == nil {
			t.Fatal("unreviewed or forbidden policy accepted")
		}
	}
}
