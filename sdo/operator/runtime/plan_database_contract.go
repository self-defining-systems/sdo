package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
)

func databaseActionProto(a program.EnsureDatabaseUser) *contracts.Action {
	t := a.Target
	target := &contracts.DatabaseTarget{Engine: t.Engine, Pod: planRefProto(t.Pod), Container: t.Container, Database: t.Database, Shell: t.Shell}
	if t.Auth != nil {
		target.Auth = &contracts.DatabaseAuth{Database: t.Auth.Database, Username: credentialRefProto(t.Auth.Username), Password: credentialRefProto(t.Auth.Password)}
	}
	out := &contracts.EnsureDatabaseUser{Target: target, User: a.User, Password: credentialRefProto(a.Password), Reason: a.Reason}
	for _, r := range a.Roles {
		out.Roles = append(out.Roles, &contracts.DatabaseRole{Database: r.Database, Name: r.Name})
	}
	return &contracts.Action{Kind: &contracts.Action_EnsureDatabaseUser{EnsureDatabaseUser: out}}
}
func databaseActionState(a *contracts.EnsureDatabaseUser) program.Action {
	t := a.GetTarget()
	target := program.DatabaseTarget{Engine: t.GetEngine(), Pod: objectRefState(t.GetPod()), Container: t.GetContainer(), Database: t.GetDatabase(), Shell: t.GetShell()}
	if t != nil && t.Auth != nil {
		target.Auth = &program.DatabaseAuth{Database: t.Auth.GetDatabase(), Username: credentialRefState(t.Auth.GetUsername()), Password: credentialRefState(t.Auth.GetPassword())}
	}
	out := program.EnsureDatabaseUser{Target: target, User: a.GetUser(), Password: credentialRefState(a.GetPassword()), Reason: a.GetReason()}
	for _, r := range a.GetRoles() {
		out.Roles = append(out.Roles, program.DatabaseRole{Database: r.GetDatabase(), Name: r.GetName()})
	}
	return out
}

func credentialRefProto(r program.CredentialRef) *contracts.CredentialRef {
	if r.ConfigMap != nil {
		k := r.ConfigMap
		return &contracts.CredentialRef{Kind: &contracts.CredentialRef_ConfigMap{ConfigMap: &contracts.ConfigMapCredentialRef{Name: k.Name, Key: k.Key, Assignment: k.Assignment}}}
	}
	return &contracts.CredentialRef{Kind: &contracts.CredentialRef_Env{Env: r.Env}}
}
func credentialRefState(r *contracts.CredentialRef) program.CredentialRef {
	if k := r.GetConfigMap(); k != nil {
		return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: k.GetName(), Key: k.GetKey(), Assignment: k.GetAssignment()}}
	}
	return program.CredentialRef{Env: r.GetEnv()}
}
