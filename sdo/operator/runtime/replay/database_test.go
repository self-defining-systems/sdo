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

type databaseProgram struct {
	wrong      bool
	credential *program.ConfigMapCredentialRef
}

func (databaseProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "database-user"} }
func dbAction() program.EnsureDatabaseUser {
	return program.EnsureDatabaseUser{Target: program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "database"}, Container: "mongo", Database: "app"}, User: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}, Password: program.CredentialRef{Env: "APP_PASSWORD"}}
}
func (p databaseProgram) action() program.EnsureDatabaseUser {
	a := dbAction()
	if p.credential != nil {
		ref := program.CredentialRef{ConfigMap: p.credential}
		a.Password = ref
		a.Target.Auth = &program.DatabaseAuth{Database: "admin", Username: ref, Password: ref}
	}
	return a
}
func (p databaseProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	a := p.action()
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
	a := p.action()
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

func TestDatabaseReplayRefusesUnrecordedAuthenticationConfiguration(t *testing.T) {
	c := databaseCase()
	cluster, err := newCluster(c.Corpus.Open)
	if err != nil {
		t.Fatal(err)
	}
	if err = cluster.seedDatabases(c.Corpus.PhaseRecordings("open")); err != nil {
		t.Fatal(err)
	}
	a := dbAction()
	a.Target.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "UNRECORDED_USERNAME"}, Password: program.CredentialRef{Env: "UNRECORDED_PASSWORD"}}
	if _, err = cluster.DatabaseUsers(context.Background(), a.Target); err == nil {
		t.Fatal("invented authentication capability")
	}
	a = dbAction()
	a.Target.Shell = "mongosh"
	if err = cluster.EnsureDatabaseUser(context.Background(), a); err == nil {
		t.Fatal("invented shell capability")
	}
}

func TestDatabaseProjectedCatalogAlsoClearsIncidentDetector(t *testing.T) {
	p := databaseProgram{}
	detector := databaseCatalogDetector{}
	result := Check(context.Background(), p, p, []sdk.Detector{detector}, databaseCase())
	if result.Status != "replayed" {
		t.Fatalf("%+v", result)
	}
}

type databaseCatalogDetector struct{}

func (databaseCatalogDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "db-catalog", Class: sdk.DetectorClassIncident, Playbooks: []string{"database-user"}}
}
func (databaseCatalogDetector) Detect(_ context.Context, s sdk.DetectionContext) ([]sdk.Finding, error) {
	users, err := program.ReadFrom(s).DatabaseUsers(dbAction().Target)
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		if u.Name == "app" {
			for _, r := range u.Roles {
				if r == dbAction().Roles[0] {
					return nil, nil
				}
			}
		}
	}
	return []sdk.Finding{{DetectorID: "db-catalog", RuleID: "missing-user", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, Summary: "Application database user missing", Evidence: "recorded catalog lacks the required direct role", PrimaryResource: dbAction().Target.Pod, Playbooks: []string{"database-user"}}}, nil
}

func TestDatabaseConfigMapCredentialKindsReplayToNoMatch(t *testing.T) {
	for _, assignment := range []string{"", "ROOT_PASSWORD"} {
		t.Run("assignment="+assignment, func(t *testing.T) {
			p := databaseProgram{credential: &program.ConfigMapCredentialRef{Name: "mongo-geo-script", Key: "init.sh", Assignment: assignment}}
			c := databaseCase()
			args, _ := json.Marshal([]any{p.action().Target})
			for i := range c.Corpus.Recordings {
				c.Corpus.Recordings[i].Arguments = args
			}
			result := Check(context.Background(), p, p, nil, c)
			if result.Status != "replayed" {
				t.Fatalf("reference-only repair did not reach NoMatch: %+v", result)
			}
			// Changing a recorded reference does not invent authentication authority.
			cluster, err := newCluster(c.Corpus.Open)
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.seedDatabases(c.Corpus.PhaseRecordings("open")); err != nil {
				t.Fatal(err)
			}
			changed := p.action().Target
			changed.Auth.Password.ConfigMap = &program.ConfigMapCredentialRef{Name: "other-script", Key: "init.sh", Assignment: assignment}
			if _, err := cluster.DatabaseUsers(context.Background(), changed); err == nil {
				t.Fatal("unrecorded reference became replay authority")
			}
		})
	}
}
