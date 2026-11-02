package program_test

import (
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
)

func TestDatabaseUserRequiresReferenceOnlyCredentials(t *testing.T) {
	a := program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db"}, Container: "mongo", Database: "application"}, User: "app", Roles: []program.DatabaseRole{{Database: "application", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
	if err := (program.Plan{Actions: []program.Action{a}}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, env := range []string{"", "literal password", "$(cat /secret)", "A;echo"} {
		bad := a
		bad.Password.Env = env
		if err := (program.Plan{Actions: []program.Action{bad}}).Validate(); err == nil {
			t.Fatalf("accepted %q", env)
		}
	}
	bad := a
	bad.Target.Engine = "sql"
	if err := (program.Plan{Actions: []program.Action{bad}}).Validate(); err == nil {
		t.Fatal("accepted unsupported engine")
	}
}

func TestCredentialReferenceKindsAreExclusive(t *testing.T) {
	key := &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "k8s-geo-mongo.sh", Assignment: "ROOT_PASSWORD"}
	for _, tc := range []struct {
		ref   program.CredentialRef
		valid bool
	}{
		{program.CredentialRef{Env: "PASSWORD"}, true},
		{program.CredentialRef{ConfigMap: key}, true},
		{program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "db", Key: "password"}}, true},
		{program.CredentialRef{}, false},
		{program.CredentialRef{Env: "PASSWORD", ConfigMap: key}, false},
		{program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "db"}}, false},
		{program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: "db", Key: "script", Assignment: "$(cat secret)"}}, false},
	} {
		if err := tc.ref.Validate(); (err == nil) != tc.valid {
			t.Fatalf("ref %+v: %v", tc.ref, err)
		}
	}
}

func TestDatabaseUserTransitionIgnoresRoleOrderAndOtherUsers(t *testing.T) {
	a := program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db"}, Container: "mongo", Database: "application"}, User: "app", Roles: []program.DatabaseRole{{Database: "b", Name: "read"}, {Database: "a", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
	other := program.DatabaseUser{Name: "other", Roles: []program.DatabaseRole{{Database: "a", Name: "readWrite"}}}
	for name, tc := range map[string]struct {
		before []program.DatabaseUser
		want   program.DatabaseUserOutcome
	}{
		"absent":       {[]program.DatabaseUser{other}, program.DatabaseUserCreated},
		"same roles":   {[]program.DatabaseUser{other, {Name: "app", Roles: []program.DatabaseRole{{Database: "a", Name: "readWrite"}, {Database: "b", Name: "read"}}}}, program.DatabaseUserAlreadyCorrect},
		"fewer roles":  {[]program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "a", Name: "readWrite"}}}}, program.DatabaseUserUpdated},
		"revoked all":  {[]program.DatabaseUser{{Name: "app"}}, program.DatabaseUserUpdated},
		"extra a role": {[]program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "a", Name: "readWrite"}, {Database: "b", Name: "read"}, {Database: "c", Name: "read"}}}}, program.DatabaseUserUpdated},
	} {
		got, err := program.DatabaseUserTransition(tc.before, a)
		if err != nil || got != tc.want {
			t.Fatalf("%s: got %q, %v want %q", name, got, err, tc.want)
		}
	}
}
