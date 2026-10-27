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
