package replay

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/planwire"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

type previewProgram struct {
	action program.Action
}

func (p previewProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "preview", Params: []program.ParamSpec{{Name: "n", ValueKind: program.IntValue, Default: int64(1)}, {Name: "command", ValueKind: program.StringValue, Default: "true"}}}
}

func (p previewProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{p.action}}, nil
}

func TestClosingPreviewOfWholeSourceRestore(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "app", Name: "database"}
	accepted := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": ref.APIVersion, "kind": ref.Kind,
		"metadata": map[string]any{"namespace": ref.Namespace, "name": ref.Name},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "db", "image": "db:v1"}},
		}}},
	}}
	opened := accepted.DeepCopy()
	if err := unstructured.SetNestedSlice(opened.Object, []any{map[string]any{
		"name": "db", "image": "db:v1",
		"volumeMounts": []any{map[string]any{"name": "data", "mountPath": "/data", "readOnly": true}},
	}}, "spec", "template", "spec", "containers"); err != nil {
		t.Fatal(err)
	}
	c := Case{
		Corpus:   sdktest.CorpusCase{Open: sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{*opened}}},
		Accepted: []*unstructured.Unstructured{accepted},
	}
	path := "spec.template.spec.containers[name=db].volumeMounts"
	field := program.SetField{Ref: ref, Path: path, Value: program.FromSource(ref, path)}
	if note := CheckMitigationPreview(previewProgram{field}, c); !strings.Contains(note, "action value path") {
		t.Fatalf("expected a value-free advisory for the source field that does not exist, got %q", note)
	}
	if note := CheckMitigationPreview(previewProgram{program.ReconcileFromSource{Ref: ref}}, c); note != "" {
		t.Fatalf("whole-object source restore preview: %s", note)
	}
}

func previewObject(ref sdk.ObjectRef, fields map[string]any) unstructured.Unstructured {
	fields["apiVersion"], fields["kind"] = ref.APIVersion, ref.Kind
	fields["metadata"] = map[string]any{"namespace": ref.Namespace, "name": ref.Name}
	return unstructured.Unstructured{Object: fields}
}

func TestEveryWireActionUsesTheSamePreview(t *testing.T) {
	cm := configMap("settings")
	deploy := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "workload"}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "pod"}
	service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "service"}
	secret := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "credential"}
	replicaSet := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "replicas"}
	command := program.Command{Pod: pod, Container: "app", Args: []string{"true"}}
	http := program.HTTPRequest{Pod: pod, Container: "app", Method: "GET", URL: "http://service"}
	store := program.StoreTarget{Component: "cache", Engine: "redis", Pod: pod, Container: "app"}
	scope := program.DataScope{Selector: "item"}
	item := storestate.Item{Kind: storestate.KindSetting, Name: "maxmemory", Value: "1"}
	object := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"namespace": "app", "name": "settings"}}
	wait := program.WaitFor{Ref: pod, Path: "/status/phase", Equals: "Succeeded"}
	actions := map[string]program.Action{
		"reconcile_from_source":  program.ReconcileFromSource{Ref: deploy},
		"rollback":               program.Rollback{Ref: deploy, ToRevision: "1"},
		"set_field":              program.SetField{Ref: deploy, Path: "spec.replicas", Value: program.FromParam("n")},
		"delete_pod":             program.DeletePod{Ref: pod},
		"scale_workload":         program.ScaleWorkload{Ref: deploy, Replicas: program.FromParam("n")},
		"exec":                   program.Exec{Pod: pod, Container: "app", Command: []program.Value{program.FromParam("command")}},
		"restart_rollout":        program.RestartRollout{Ref: deploy},
		"create_from_source":     program.CreateFromSource{Ref: cm, Reason: "restore config"},
		"delete_object":          program.DeleteObject{Ref: cm, Reason: "remove orphan"},
		"remove_service_port":    program.RemoveServicePort{Ref: service, Port: 8080},
		"ensure_database_user":   program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: "mongodb", Pod: pod, Container: "app", Database: "app"}, User: "reader", Roles: []program.DatabaseRole{{Database: "app", Name: "read"}}, Password: program.CredentialRef{Env: "DB_PASSWORD"}},
		"patch_object":           program.PatchObject{Ref: cm, Type: "merge", Patch: []byte(`{"data":{"state":"ready"}}`)},
		"apply_object":           program.ApplyObject{Ref: cm, Object: object, Mode: "create"},
		"kubernetes_request":     program.KubernetesRequest{Ref: cm, Method: "GET", Path: "/api/v1/namespaces/app/configmaps/settings"},
		"wait_for":               wait,
		"ensure_exec":            program.EnsureExec{Observe: command, Ensure: command},
		"ensure_http":            program.EnsureHTTP{Observe: http, Ensure: http},
		"run_job":                program.RunJob{Object: program.ApplyObject{Ref: pod, Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "app", "name": "pod"}}, Mode: "create"}, Completion: wait},
		"run_script":             program.RunScript{Script: "true"},
		"sync_webhook_ca":        program.SyncWebhookCA{Target: sdk.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "validator"}, Webhook: "validator.example", SecretNamespace: "app", SecretName: "cert", SecretKey: "ca.crt"}},
		"retry_admission":        program.RetryAdmission{Ref: replicaSet},
		"retire_old_revision":    program.RetireOldRevision{Ref: replicaSet},
		"escalate":               program.Escalate{Reason: "operator required"},
		"store_backup":           program.StoreBackup{ID: "before", Target: store, Scope: scope},
		"invalidate_derived":     program.InvalidateDerived{Target: store, Scope: scope},
		"repair_data":            program.RepairData{Target: store, Scope: scope, Statement: "DEL", ExpectMin: 1, ExpectMax: 1},
		"restore_backup":         program.RestoreBackup{Target: store, Backup: "before"},
		"restore_store_item":     program.RestoreStoreItem{Target: store, Change: storestate.Change{After: &item}},
		"check_store_credential": program.CheckStoreCredential{Target: store, Client: pod, Container: "app"},
		"propose_source_change":  program.ProposeSourceChange{Ref: deploy, Path: "spec.replicas", Value: program.FromParam("n"), Reason: "Users need one replica to serve requests reliably."},
	}
	oneof := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().ByName("kind")
	ctx := context.Background()
	for i := range oneof.Fields().Len() {
		field := oneof.Fields().Get(i)
		t.Run(string(field.Name()), func(t *testing.T) {
			action := actions[string(field.Name())]
			if action == nil {
				t.Fatal("missing preview fixture for wire action")
			}
			plan := program.Plan{Actions: []program.Action{action}}
			if err := plan.Validate(); err != nil {
				t.Fatal(err)
			}
			wire, err := planwire.ActionProto(action)
			if err != nil || !wire.ProtoReflect().Has(field) {
				t.Fatalf("wire action = %v, %v", wire, err)
			}
			decoded := planwire.ActionState(wire)
			if reflect.TypeOf(decoded) != reflect.TypeOf(action) {
				t.Fatalf("wire action decoded to %T, want %T", decoded, action)
			}
			roundTrip, err := planwire.ActionProto(decoded)
			if err != nil || !proto.Equal(wire, roundTrip) {
				t.Fatalf("%s changes across the mitigation wire round-trip: %v", field.Name(), err)
			}

			open := sdktest.Snapshot{NamespaceName: "app", Objects: []unstructured.Unstructured{
				previewObject(deploy, map[string]any{"spec": map[string]any{
					"replicas": int64(1),
					"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "app:v1"}}}},
				}}),
				previewObject(pod, map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "app:v1"}}}}),
				previewObject(service, map[string]any{"spec": map[string]any{"ports": []any{map[string]any{"port": int64(8080)}, map[string]any{"port": int64(8081)}}}}),
				previewObject(cm, map[string]any{"data": map[string]any{"state": "ready"}}),
			}}
			client, err := newCluster(open)
			if err != nil {
				t.Fatal(err)
			}
			executor := operator.PlanExecutor{Client: client, Source: &source{}, Values: program.ValueInputs{Spec: previewProgram{action}.Spec()}}
			incident := operator.IncidentRequest{Namespace: "app", SourceCommit: "offline-accepted", RepairPolicy: "recorded-actions"}
			_, pageErr := executor.Apply(ctx, incident, plan, true)
			_, previewErr := executor.PreviewMitigation(ctx, incident, plan)
			if (pageErr == nil) != (previewErr == nil) || pageErr != nil && pageErr.Error() != previewErr.Error() {
				t.Fatalf("dry-run disagreement: page = %v, preview = %v", pageErr, previewErr)
			}
			reason := CheckMitigationPreview(previewProgram{action}, Case{Corpus: sdktest.CorpusCase{Open: open}})
			if (reason == "") != (pageErr == nil) {
				t.Fatalf("closing preview %q disagrees with page dry-run: %v", reason, pageErr)
			}
			if reason != "" {
				t.Logf("closing preview unverified: %s (page dry-run: %v)", reason, pageErr)
			}
		})
	}
	t.Run("secret_reconcile_from_source", func(t *testing.T) {
		_, err := planwire.ActionProto(program.ReconcileFromSource{Ref: secret})
		if err != nil {
			if !strings.Contains(err.Error(), "unsupported general action") {
				t.Fatal(err)
			}
			t.Log("expected encoder gap pending #1931")
		}
	})
}
