package mongoauth

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"

	"sdo.dev/operator/sdk/program"
)

func (Program) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	target, user, password, err := configuration(v, b)
	if err != nil {
		return program.Plan{}, err
	}
	var application sdk.ObjectRef
	count := 0
	for _, deployment := range v.Deployments() {
		if deployment.Namespace == target.Pod.Namespace && deployment.Name == "geo" {
			application = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: deployment.Namespace, Name: deployment.Name}
			count++
		}
	}
	if count != 1 {
		return program.Plan{}, fmt.Errorf("dependent application deployment unavailable or ambiguous")
	}
	return program.Plan{Actions: []program.Action{program.EnsureDatabaseUser{Target: target, User: user, Roles: []program.DatabaseRole{{Database: target.Database, Name: "readWrite"}}, Password: password, Reason: "restore observed application catalog identity"}, program.RestartRollout{Ref: application, Reason: "reconnect the dependent application after catalog repair"}}}, nil
}
