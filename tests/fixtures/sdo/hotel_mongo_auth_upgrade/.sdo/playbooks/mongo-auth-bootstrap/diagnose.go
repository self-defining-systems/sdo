package mongoauth

import (
	"context"
	"fmt"

	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "mongo-auth-bootstrap", Params: []program.ParamSpec{
		{Name: "pod", Kind: "Pod", Required: true},

		{Name: "database", ValueKind: program.StringValue, Default: "geo"},
		{Name: "user", ValueKind: program.StringValue, Default: "admin"},
	}}
}

// Accepted application source supplies nonsecret database/user defaults. The
// mounted init script supplies credential references; capture retains key names
// only. Diagnosis and replay never inspect or reconstruct credential contents.
func configuration(v program.ReadView, b program.Bindings) (program.DatabaseTarget, string, program.CredentialRef, error) {
	ref := b.Objects["pod"]
	database, user := "geo", "admin"
	if value, ok := b.Values["database"].(string); ok {
		database = value
	}
	if value, ok := b.Values["user"].(string); ok {
		user = value
	}
	for _, pod := range v.Pods() {
		if pod.Namespace != ref.Namespace || pod.Name != ref.Name {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.ConfigMap == nil {
				continue
			}
			for _, item := range volume.ConfigMap.Items {
				if item.Key != "k8s-geo-mongo.sh" {
					continue
				}
				for _, cm := range v.ConfigMaps() {
					if cm.Namespace != ref.Namespace || cm.Name != volume.ConfigMap.Name {
						continue
					}
					if _, ok := cm.Data[item.Key]; !ok {
						continue
					}
					for _, container := range pod.Spec.Containers {
						for _, mount := range container.VolumeMounts {
							if mount.Name != volume.Name {
								continue
							}
							credential := func(assignment string) program.CredentialRef {
								return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: cm.Name, Key: item.Key, Assignment: assignment}}
							}
							target := program.DatabaseTarget{Engine: program.MongoDB, Pod: ref, Container: container.Name, Database: database, Auth: &program.DatabaseAuth{Database: "admin", Username: credential("ROOT_USER"), Password: credential("ROOT_PASSWORD")}}
							return target, user, credential("APP_PASSWORD"), nil
						}
					}
				}
			}
		}
	}
	return program.DatabaseTarget{}, "", program.CredentialRef{}, fmt.Errorf("mounted database credential reference unavailable")
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
