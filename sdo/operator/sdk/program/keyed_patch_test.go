package program_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestMergePatchRefusesKeyedListsAtPlanBuild(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for _, tc := range []struct{ patch, list string }{
		{`{"spec":{"template":{"spec":{"containers":[{"name":"api","resources":{"limits":{"memory":"256Mi"}}}]}}}}`, "/spec/template/spec/containers"},
		{`{"spec":{"template":{"spec":{"containers":null}}}}`, "/spec/template/spec/containers"},
		{`{"spec":{"template":{"spec":{"volumes":[{"name":"data","emptyDir":{}}]}}}}`, "/spec/template/spec/volumes"},
	} {
		err := (program.Plan{Actions: []program.Action{program.PatchObject{Ref: ref, Type: "merge", Patch: json.RawMessage(tc.patch)}}}).Validate()
		var replacement *program.KeyedListReplacementError
		if !errors.As(err, &replacement) || replacement.Path != tc.list || !strings.Contains(err.Error(), tc.list) {
			t.Fatalf("wanted typed refusal naming %s, got %v", tc.list, err)
		}
	}
}

func TestStrategicPatchPreservesKeyedMembersAndContainerFields(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	var object map[string]any
	if err := json.Unmarshal([]byte(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"app"},"spec":{"template":{"spec":{"containers":[{"name":"api","command":["serve"],"env":[{"name":"KEEP","value":"yes"},{"name":"CHANGE","value":"old"}]},{"name":"sidecar","image":"sidecar:v1"}],"volumes":[{"name":"keep","emptyDir":{}}]}}}}`), &object); err != nil {
		t.Fatal(err)
	}
	patch := json.RawMessage(`{"spec":{"template":{"spec":{"containers":[{"name":"api","resources":{"limits":{"memory":"256Mi"}},"env":[{"name":"CHANGE","value":"new"}]}],"volumes":[{"name":"added","emptyDir":{}}]}}}}`)
	next, err := program.ProjectPatch(object, program.PatchObject{Ref: ref, Type: "strategic", Patch: patch})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]any{
		"spec.template.spec.containers[name=api].command[0]":             "serve",
		"spec.template.spec.containers[name=sidecar].image":              "sidecar:v1",
		"spec.template.spec.containers[name=api].env[name=KEEP].value":   "yes",
		"spec.template.spec.containers[name=api].env[name=CHANGE].value": "new",
		"spec.template.spec.volumes[name=keep].name":                     "keep",
		"spec.template.spec.volumes[name=added].name":                    "added",
	} {
		got, e := program.FieldValue(next, path)
		if e != nil || got != want {
			t.Fatalf("%s: %v, %v", path, got, e)
		}
	}
}

func TestRawMergePatchRefusesKeyedLists(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	a := program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/apis/apps/v1/namespaces/app/deployments/api", ContentType: "application/merge-patch+json", Body: []byte(`{"spec":{"template":{"spec":{"containers":[]}}}}`)}
	var replacement *program.KeyedListReplacementError
	if err := (program.Plan{Actions: []program.Action{a}}).Validate(); !errors.As(err, &replacement) {
		t.Fatalf("raw patch bypassed keyed-list check: %v", err)
	}
}

func TestCoupledEditsRefuseOpaqueEffects(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	action := program.ScaleWorkload{Ref: ref, Replicas: program.FromParam("replicas")}
	plan := program.Plan{Actions: []program.Action{action, program.WaitFor{Ref: ref, Path: "/status/readyReplicas", Equals: 1}, action}}
	var conflict *program.AtomicWriteConflictError
	if err := plan.Validate(); !errors.As(err, &conflict) {
		t.Fatalf("coupled edits across an effect were allowed: %v", err)
	}
}

func TestRawMergePatchSchemaCannotBeBypassed(t *testing.T) {
	for _, mode := range []string{"no-ref", "charset", "wrong-ref", "wrong-url"} {
		t.Run(mode, func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			a := program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/apis/apps/v1/namespaces/app/deployments/api", ContentType: "application/merge-patch+json", Body: []byte(`{"spec":{"template":{"spec":{"containers":[]}}}}`)}
			switch mode {
			case "no-ref":
				a.Ref = sdk.ObjectRef{}
			case "charset":
				a.ContentType += "; charset=utf-8"
			case "wrong-ref":
				a.Ref.Kind = "ConfigMap"
			case "wrong-url":
				a.Path = "/api/v1/namespaces/app/configmaps/api"
			}
			err := (program.Plan{Actions: []program.Action{a}}).Validate()
			if err == nil {
				t.Fatal("unsafe raw request accepted")
			}
			if mode == "no-ref" || mode == "charset" {
				var replacement *program.KeyedListReplacementError
				if !errors.As(err, &replacement) {
					t.Fatalf("want typed keyed-list refusal, got %v", err)
				}
			}
		})
	}
}

func TestMergeObjectNamedServicePortNumberChange(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	var object map[string]any
	if err := json.Unmarshal([]byte(`{"apiVersion":"v1","kind":"Service","metadata":{"name":"api","namespace":"app"},"spec":{"ports":[{"name":"http","port":80,"targetPort":8000},{"name":"other","port":90}]}}`), &object); err != nil {
		t.Fatal(err)
	}
	next, err := program.MergeObject(object, ref, json.RawMessage(`{"spec":{"ports":[{"name":"http","port":8080}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	ports, _ := program.FieldValue(next, "spec.ports")
	if len(ports.([]any)) != 2 {
		t.Fatalf("duplicate named ports: %v", ports)
	}
	for path, want := range map[string]any{"spec.ports[name=http].port": int64(8080), "spec.ports[name=http].targetPort": int64(8000), "spec.ports[name=other].port": int64(90)} {
		got, e := program.FieldValue(next, path)
		if e != nil || got != want {
			t.Fatalf("%s: %v %v", path, got, e)
		}
	}
}

func TestRawMergePatchRequiresMatchingKnownSchema(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for _, mode := range []string{"known", "inferred", "unknown", "subresource", "name", "namespace", "version"} {
		t.Run(mode, func(t *testing.T) {
			a := program.KubernetesRequest{Ref: ref, Method: "PATCH", Path: "/apis/apps/v1/namespaces/app/deployments/api", ContentType: "application/merge-patch+json; charset=utf-8", Body: []byte(`{"spec":{"replicas":2}}`)}
			switch mode {
			case "inferred":
				a.Ref = sdk.ObjectRef{}
			case "unknown":
				a.Ref = sdk.ObjectRef{}
				a.Path = "/apis/example.com/v1/namespaces/app/widgets/api"
			case "subresource":
				a.Path += "/scale"
			case "name":
				a.Ref.Name = "other"
			case "namespace":
				a.Ref.Namespace = "other"
			case "version":
				a.Ref.APIVersion = "apps/v2"
			}
			err := (program.Plan{Actions: []program.Action{a}}).Validate()
			if mode == "known" || mode == "inferred" {
				if err != nil {
					t.Fatal(err)
				}
			} else {
				var schemaError *program.RawMergeSchemaError
				if !errors.As(err, &schemaError) {
					t.Fatalf("want typed schema refusal, got %v", err)
				}
			}
		})
	}
}

func TestRawWebhookMergePatchUsesBuiltInSchema(t *testing.T) {
	for _, kind := range []string{"validating", "mutating"} {
		t.Run(kind, func(t *testing.T) {
			request := program.KubernetesRequest{Method: "PATCH", Path: "/apis/admissionregistration.k8s.io/v1/" + kind + "webhookconfigurations/policy", ContentType: "application/merge-patch+json", Body: []byte(`{"metadata":{"labels":{"repaired":"true"}}}`)}
			if err := request.Validate(); err != nil {
				t.Fatal(err)
			}
			request.Body = []byte(`{"webhooks":[{"name":"policy.example"}]}`)
			var replacement *program.KeyedListReplacementError
			if err := request.Validate(); !errors.As(err, &replacement) || replacement.Path != "/webhooks" {
				t.Fatalf("webhook keyed-list replacement escaped validation: %v", err)
			}
		})
	}
}
