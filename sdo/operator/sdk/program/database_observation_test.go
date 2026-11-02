package program_test

import (
	"context"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type ignoredDatabaseRead struct{}

func (ignoredDatabaseRead) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "ignored-db"} }
func (ignoredDatabaseRead) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, _ = v.DatabaseUsers(program.DatabaseTarget{})
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestUnavailableDatabaseCatalogCannotClearDiagnosis(t *testing.T) {
	v := program.RunDiagnose(context.Background(), ignoredDatabaseRead{}, program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), program.Bindings{}, time.Second)
	if v.Result != program.Unknown {
		t.Fatalf("%+v", v)
	}
}
func TestDatabaseCatalogProjectionPreservesUsersAndIsIdempotent(t *testing.T) {
	a := program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "application"}, User: "app", Roles: []program.DatabaseRole{{Database: "application", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
	users := []program.DatabaseUser{{Name: "unrelated", Roles: []program.DatabaseRole{{Database: "admin", Name: "read"}}}, {Name: "app", Roles: []program.DatabaseRole{}}}
	first, err := program.ApplyEnsureDatabaseUser(users, a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := program.ApplyEnsureDatabaseUser(first, a)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || len(users[1].Roles) != 0 || len(first) != 2 {
		t.Fatalf("not isolated/idempotent: %+v %+v", first, users)
	}
	for _, name := range []string{"attacker:27017", "mongodb://attacker/", "-host", "bad name"} {
		bad := a
		bad.Target.Database = name
		if err = bad.Validate(); err == nil {
			t.Fatalf("accepted unsafe database %q", name)
		}
	}
}
