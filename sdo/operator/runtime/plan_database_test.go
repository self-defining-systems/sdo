package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

type databaseFake struct {
	*planFakeClient
	users  []program.DatabaseUser
	fail   bool
	ignore bool
	writes int
}

func (c *databaseFake) DatabaseUsers(context.Context, program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if c.fail {
		return nil, fmt.Errorf("canary-password")
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
	for _, mode := range []string{"missing", "revoked", "refused", "ignored"} {
		t.Run(mode, func(t *testing.T) {
			a := databaseAction()
			ref := a.Target.Pod
			object := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"namespace": "app", "name": "database", "uid": "one", "resourceVersion": "1"}, "spec": map[string]any{"containers": []any{map[string]any{"name": "mongo"}}}}}
			c := &databaseFake{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: object}}, users: []program.DatabaseUser{{Name: "unrelated", Roles: []program.DatabaseRole{{Database: "admin", Name: "read"}}}}, fail: mode == "refused", ignore: mode == "ignored"}
			if mode == "revoked" {
				c.users = append(c.users, program.DatabaseUser{Name: "app"})
			}
			e := PlanExecutor{Client: c, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}}
			r, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app", RepairPolicy: "recorded-actions"}, program.Plan{Actions: []program.Action{a}}, false)
			if mode == "refused" || mode == "ignored" {
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
