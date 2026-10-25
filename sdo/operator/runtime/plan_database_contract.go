package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func databaseActionProto(a program.EnsureDatabaseUser) *contracts.Action {
	t := a.Target
	target := &contracts.DatabaseTarget{Engine: t.Engine, Pod: planRefProto(t.Pod), Container: t.Container, Database: t.Database, Shell: t.Shell}
	if t.Auth != nil {
		target.Auth = &contracts.DatabaseAuth{Database: t.Auth.Database, Username: &contracts.CredentialRef{Env: t.Auth.Username.Env}, Password: &contracts.CredentialRef{Env: t.Auth.Password.Env}}
	}
	out := &contracts.EnsureDatabaseUser{Target: target, User: a.User, Password: &contracts.CredentialRef{Env: a.Password.Env}, Reason: a.Reason}
	for _, r := range a.Roles {
		out.Roles = append(out.Roles, &contracts.DatabaseRole{Database: r.Database, Name: r.Name})
	}
	return &contracts.Action{Kind: &contracts.Action_EnsureDatabaseUser{EnsureDatabaseUser: out}}
}
func databaseActionState(a *contracts.EnsureDatabaseUser) program.Action {
	t := a.GetTarget()
	target := program.DatabaseTarget{Engine: t.GetEngine(), Pod: objectRefState(t.GetPod()), Container: t.GetContainer(), Database: t.GetDatabase(), Shell: t.GetShell()}
	if t.Auth != nil {
		target.Auth = &program.DatabaseAuth{Database: t.Auth.GetDatabase(), Username: program.CredentialRef{Env: t.Auth.GetUsername().GetEnv()}, Password: program.CredentialRef{Env: t.Auth.GetPassword().GetEnv()}}
	}
	out := program.EnsureDatabaseUser{Target: target, User: a.GetUser(), Password: program.CredentialRef{Env: a.GetPassword().GetEnv()}, Reason: a.GetReason()}
	for _, r := range a.GetRoles() {
		out.Roles = append(out.Roles, program.DatabaseRole{Database: r.GetDatabase(), Name: r.GetName()})
	}
	return out
}
