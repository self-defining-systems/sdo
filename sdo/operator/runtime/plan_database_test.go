package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
)

type databaseFake struct {
	replaceOnRead bool
	*planFakeClient
	users                     []program.DatabaseUser
	fail                      bool
	ignore                    bool
	writes                    int
	credentialErrorAfterWrite *CredentialReferenceError
}

func (c *databaseFake) DatabaseUsers(context.Context, program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if c.credentialErrorAfterWrite != nil && c.writes > 0 {
		return nil, c.credentialErrorAfterWrite
	}
	if c.fail {
		return nil, fmt.Errorf("canary-password")
	}
	if c.replaceOnRead && c.writes > 0 {
		for _, object := range c.objects {
			object.SetUID("replacement")
		}
	}
	return program.NormalizeDatabaseUsers(c.users)
}
func (c *databaseFake) EnsureDatabaseUser(_ context.Context, a program.EnsureDatabaseUser) error {
	c.writes++
	if c.fail {
		return fmt.Errorf("canary-password")
	}
	if !c.ignore {
		c.users, _ = program.ApplyEnsureDatabaseUser(c.users, a)
	}
	return nil
}
func databaseAction() program.EnsureDatabaseUser {
	return program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "app"}, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
}
func TestEnsureDatabaseUserProductionContract(t *testing.T) {
	for _, mode := range []string{"missing", "revoked", "refused", "ignored", "replaced"} {
		t.Run(mode, func(t *testing.T) {
			a := databaseAction()
			ref := a.Target.Pod
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "app", "name": "database", "uid": "one", "resourceVersion": "1"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "mongo"}}}}}
			c := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, users: []program.DatabaseUser{{Name: "unrelated", Roles: []program.DatabaseRole{{Database: "admin", Name: "read"}}}}, fail: mode == "refused", ignore: mode == "ignored", replaceOnRead: mode == "replaced"}
			if mode == "revoked" {
				c.users = append(c.users, program.DatabaseUser{Name: "app"})
			}
			e := PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
			if mode == "refused" || mode == "ignored" || mode == "replaced" {
				if mode == "ignored" && (len(r.ActionReports) != 1 || r.ActionReports[0].Status != "unknown") {
					t.Fatalf("unconfirmed write lost unknown status: %+v", r.ActionReports)
				}
				if err == nil || r.VerificationPassed {
					t.Fatalf("unverified operation credited %+v", r)
				}
				raw, _ := json.Marshal(r)
				if strings.Contains(string(raw)+fmt.Sprint(err), "canary-password") {
					t.Fatal("credential leaked")
				}
				return
			}
			if err != nil || !r.Applied || !r.VerificationPassed {
				t.Fatalf("%+v %v", r, err)
			}
			if len(c.users) != 2 || len(r.Patches) != 1 || !r.Patches[0].Irreversible {
				t.Fatalf("%+v %+v", c.users, r)
			}
			if _, err = r.Proto(); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			var restored PlanApplicationResult
			if err = json.Unmarshal(raw, &restored); err != nil {
				t.Fatal(err)
			}
			if err = e.rollback(context.Background(), &r); err == nil {
				t.Fatal("irreversible user creation was undoable")
			}
		})
	}
}

func TestDatabasePlanCapturesOpenCatalogWithoutCredentials(t *testing.T) {
	a := databaseAction()
	ref := a.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID("one")
	object.SetResourceVersion("1")
	client := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}}
	calls := []sdktest.PrimitiveCall{}
	executor := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, RecordPrimitive: func(call sdktest.PrimitiveCall) error { calls = append(calls, call); return nil }}
	result, err := executor.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
	if err != nil || !result.VerificationPassed {
		t.Fatalf("repair failed: %v", err)
	}
	if len(calls) != 1 || calls[0].Method != "DatabaseUsers" || calls[0].Phase != "" || string(calls[0].Result) != "[]" {
		t.Fatalf("pre-repair catalog was not captured for reflection: %+v", calls)
	}
}

func TestDatabaseReadbackPreservesTypedCredentialFailure(t *testing.T) {
	action := databaseAction()
	ref := action.Target.Pod
	object := planObject(ref, map[string]any{"containers": []any{map[string]any{"name": "mongo"}}})
	object.SetUID("one")
	object.SetResourceVersion("1")
	credential := program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "init.sh", Assignment: "ROOT_PWD"}}
	client := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, credentialErrorAfterWrite: &CredentialReferenceError{Reference: credential, Reason: "key missing"}}
	result, err := (PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}).Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{action}}, false)
	var failure *CredentialReferenceError
	if !errors.As(err, &failure) || failure.Reference.ConfigMap.Name != "mongo-geo-script" || result.VerificationPassed || result.ActionReports[0].Status != "unknown" {
		t.Fatalf("readback lost actionable reference error or credited an unverified repair: %v", err)
	}
}
