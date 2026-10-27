package runtime

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Semantic examples complement generated shapes. They use synthetic identities;
// live resource names and desired values must come from incident evidence.
// The help-only contract fixture checks these through the ordinary plan codec.
var menuActionExamples = sync.OnceValue(func() map[string]map[string]any {
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "example"}
	service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "example"}
	deployment := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "example"}
	credential := program.CredentialRef{Env: "APP_PASSWORD"}
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: pod, Container: "app", Database: "app", Auth: &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "ADMIN_USER"}, Password: program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "bootstrap", Key: "init.sh", Assignment: "ADMIN_PASSWORD"}}}}
	command := program.Command{Pod: pod, Container: "app", Args: []string{"true"}}
	http := program.HTTPRequest{Pod: pod, Container: "app", Method: "GET", URL: "http://127.0.0.1:8080/health", HeaderEnv: map[string]program.CredentialRef{"Authorization": {Env: "HTTP_TOKEN"}}, Bearer: &program.CredentialRef{Env: "HTTP_TOKEN"}}
	object := program.ApplyObject{Ref: pod, Mode: "create", Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "app", "name": "example"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "example.invalid/app"}}}}}
	wait := program.WaitFor{Ref: pod, Path: "/status/phase", Equals: "Succeeded"}
	reason := "restore intended behavior"
	actions := []program.Action{
		program.SyncWebhookCA{Target: program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}, Webhook: "policy.example", SecretNamespace: "app", SecretName: "serving", SecretKey: "ca.crt"}, Reason: reason},
		program.ReconcileFromSource{Ref: deployment},
		program.Rollback{Ref: deployment, ToRevision: "1"},
		program.SetField{Ref: service, Path: "spec.ports[0].targetPort", Value: program.FromSource(service, "spec.ports[0].targetPort"), Reason: reason},
		program.RestartRollout{Ref: deployment, Reason: reason},
		program.CreateFromSource{Ref: service, Reason: reason},
		program.DeleteObject{Ref: service, Reason: reason},
		program.DeletePod{Ref: pod},
		program.ScaleWorkload{Ref: deployment, Replicas: program.FromSource(deployment, "spec.replicas")},
		program.Exec{Pod: pod, Container: "app", Command: []program.Value{program.FromSource(deployment, "spec.template.spec.containers[0].command[0]")}},
		program.RemoveServicePort{Ref: service, Name: "obsolete", Reason: reason},
		program.EnsureDatabaseUser{Target: target, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: credential, Reason: reason},
		program.PatchObject{Ref: pod, Edits: []program.ListEdit{{Path: "/spec/containers", Match: map[string]any{"name": "app"}, Op: "replace", Value: map[string]any{"name": "app", "image": "example.invalid/app"}}}, Reason: reason},
		object,
		program.KubernetesRequest{Ref: pod, Method: "GET", Path: "/api/v1/namespaces/app/pods/example", Query: map[string]string{"pretty": "false"}},
		wait,
		program.EnsureExec{Observe: command, Ensure: command},
		program.EnsureHTTP{Observe: http, Ensure: http},
		program.RunJob{Object: object, Completion: wait},
		program.RunScript{Script: ":", Env: map[string]program.Value{"POD": program.FromObserved(pod, "metadata.name")}},
	}
	result := map[string]map[string]any{}
	for _, action := range actions {
		envelope, err := planActionProto(action)
		if err != nil {
			panic(fmt.Sprintf("invalid built-in menu example: %v", err))
		}
		field := envelope.ProtoReflect().WhichOneof(envelope.ProtoReflect().Descriptor().Oneofs().Get(0))
		body := envelope.ProtoReflect().Get(field).Message().Interface()
		raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(body)
		if err != nil {
			panic(err)
		}
		var values map[string]any
		if err := json.Unmarshal(raw, &values); err != nil {
			panic(err)
		}
		for name, value := range values {
			f := field.Message().Fields().ByName(protoreflect.Name(name))
			if f.Kind() == protoreflect.BytesKind {
				decoded, err := base64.StdEncoding.DecodeString(value.(string))
				if err != nil {
					panic(err)
				}
				values[name] = string(decoded)
			}
		}
		result[string(field.Message().Name())] = values
	}
	return result
})
