package replay

import (
	"context"
	"encoding/json"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

type databaseProgram struct{ wrong bool }

func (databaseProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "database-user"} }
func dbAction() program.EnsureDatabaseUser {
	return program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "app"}, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
}
func (databaseProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	a := dbAction()
	users, err := v.DatabaseUsers(a.Target)
	if err != nil {
		return program.Verdict{}, err
	}
	for _, u := range users {
		if u.Name == a.User {
			for _, r := range u.Roles {
				if r == a.Roles[0] {
					return program.Verdict{Result: program.NoMatch}, nil
				}
			}
		}
	}
	return program.Verdict{Result: program.Match}, nil
}
func (p databaseProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	a := dbAction()
	if p.wrong {
		a.User = "someone-else"
	}
	return program.Plan{Actions: []program.Action{a}}, nil
}
func databaseCase() Case {
	a := dbAction()
	s := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "mongo"}}}}}}
	args, _ := json.Marshal([]any{a.Target})
	open, _ := json.Marshal([]program.DatabaseUser{{Name: "unrelated", Roles: []program.DatabaseRole{}}})
	healthy, _ := json.Marshal([]program.DatabaseUser{{Name: "app", Roles: a.Roles}, {Name: "unrelated", Roles: []program.DatabaseRole{}}})
	return Case{ID: "synthetic-user-catalog", RepairOnly: true, Corpus: sdktest.CorpusCase{Open: s, Healthy: s, Fixed: s, RootCauseProgramIDs: []string{"database-user"}, Recordings: []sdktest.PrimitiveCall{{Phase: "open", Method: "DatabaseUsers", Arguments: args, Result: open}, {Phase: "healthy", Method: "DatabaseUsers", Arguments: args, Result: healthy}, {Phase: "fixed", Method: "DatabaseUsers", Arguments: args, Result: healthy}}}}
}
func TestDatabaseUserClosedLoopUsesOpenCatalog(t *testing.T) {
	for _, mode := range []string{"missing", "revoked", "wrong-user", "no-open"} {
		t.Run(mode, func(t *testing.T) {
			c := databaseCase()
			if mode == "no-open" {
				c.Corpus.Recordings = c.Corpus.Recordings[1:]
			}
			if mode == "revoked" {
				raw, _ := json.Marshal([]program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{}}})
				c.Corpus.Recordings[0].Result = raw
			}
			p := databaseProgram{wrong: mode == "wrong-user"}
			r := Check(context.Background(), p, p, nil, c)
			want := mode == "missing" || mode == "revoked"
			if (r.Status == "replayed") != want {
				t.Fatalf("%+v", r)
			}
		})
	}
}
