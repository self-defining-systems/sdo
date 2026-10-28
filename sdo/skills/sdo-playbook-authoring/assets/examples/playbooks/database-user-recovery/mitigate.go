package databaseuser

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	target, user, passwordEnv, err := configuration(v, b)
	if err != nil {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.EnsureDatabaseUser{Target: target, User: user, Roles: []program.DatabaseRole{{Database: target.Database, Name: "readWrite"}}, Password: program.CredentialRef{Env: passwordEnv}, Reason: "restore observed application catalog identity"}}}, nil
}
