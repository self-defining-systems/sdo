package mongoauth

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "mongo-auth-bootstrap", Params: []program.ParamSpec{{Name: "pod", Kind: "Pod", Required: true}}}
}

// Desired catalog identity is observed nonsecret configuration. Credentials stay
// environment references; neither diagnosis nor replay resolves their values.
func configuration(v program.ReadView, b program.Bindings) (program.DatabaseTarget, string, string, error) {
	ref := b.Objects["pod"]
	for _, pod := range v.Pods() {
		if pod.Namespace != ref.Namespace || pod.Name != ref.Name {
			continue
		}
		for _, container := range pod.Spec.Containers {
			values := map[string]string{}
			present := map[string]bool{}
			for _, env := range container.Env {
				values[env.Name], present[env.Name] = env.Value, true
			}
			if values["APP_DATABASE"] == "" || values["APP_USERNAME"] == "" || values["MONGO_AUTH_DATABASE"] == "" || !present["APP_PASSWORD"] || !present["MONGO_INITDB_ROOT_USERNAME"] || !present["MONGO_INITDB_ROOT_PASSWORD"] {
				continue
			}
			return program.DatabaseTarget{Engine: program.MongoDB, Pod: ref, Container: container.Name, Database: values["APP_DATABASE"], Auth: &program.DatabaseAuth{Database: values["MONGO_AUTH_DATABASE"], Username: program.CredentialRef{Env: "MONGO_INITDB_ROOT_USERNAME"}, Password: program.CredentialRef{Env: "MONGO_INITDB_ROOT_PASSWORD"}}}, values["APP_USERNAME"], "APP_PASSWORD", nil
		}
	}
	return program.DatabaseTarget{}, "", "", fmt.Errorf("database configuration or credential environment reference unavailable")
}

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	target, user, _, err := configuration(v, b)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, nil
	}
	users, err := v.DatabaseUsers(target)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, nil
	}
	for _, entry := range users {
		if entry.Name == user && len(entry.Roles) == 1 && entry.Roles[0] == (program.DatabaseRole{Database: target.Database, Name: "readWrite"}) {
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: target.Pod, Fact: "application catalog user and direct role", Observed: "absent or different", Expected: user + " readWrite on " + target.Database}}}, nil
}
