package mongorelation

import (
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Config struct {
	Target program.DatabaseTarget
	User   string
	Inputs map[string]program.Value
}

// Configuration is the one app-fact access point. Current mount relations and
// structured configuration supply identities; no incident object name is baked in.
func Configuration(v program.ReadView, ref sdk.ObjectRef) (Config, error) {
	var found []Config
	for _, pod := range v.Pods() {
		if pod.Namespace != ref.Namespace || pod.Name != ref.Name {
			continue
		}
		for vi, volume := range pod.Spec.Volumes {
			if volume.ConfigMap == nil {
				continue
			}
			for _, cm := range v.ConfigMaps() {
				if cm.Namespace != pod.Namespace || cm.Name != volume.ConfigMap.Name {
					continue
				}
				facts := cm.Annotations
				database, user, authDB := facts["example.test/database"], facts["example.test/user"], facts["example.test/auth-database"]
				rootUser, rootPassword := facts["example.test/root-user-assignment"], facts["example.test/root-password-assignment"]
				if database == "" || user == "" || authDB == "" || rootUser == "" || rootPassword == "" {
					continue
				}
				for ii, item := range volume.ConfigMap.Items {
					if _, exists := cm.Data[item.Key]; !exists {
						continue
					}
					for ci, container := range pod.Spec.Containers {
						for mi, mount := range container.VolumeMounts {
							if mount.Name != volume.Name {
								continue
							}
							credential := func(assignment string) program.CredentialRef {
								return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: cm.Name, Key: item.Key, Assignment: assignment}}
							}
							target := program.DatabaseTarget{Engine: program.MongoDB, Pod: ref, Container: container.Name, Database: database, Auth: &program.DatabaseAuth{Database: authDB, Username: credential(rootUser), Password: credential(rootPassword)}}
							inputs := map[string]program.Value{"POD": program.FromObserved(ref, "metadata.name"), "NAMESPACE": program.FromObserved(ref, "metadata.namespace"), "CONTAINER": program.FromObserved(ref, fmt.Sprintf("spec.containers[%d].name", ci)), "MOUNT": program.FromObserved(ref, fmt.Sprintf("spec.containers[%d].volumeMounts[%d].mountPath", ci, mi)), "ITEM": program.FromObserved(ref, fmt.Sprintf("spec.volumes[%d].configMap.items[%d].path", vi, ii))}
							found = append(found, Config{Target: target, User: user, Inputs: inputs})
						}
					}
				}
			}
		}
	}
	if len(found) != 1 {
		return Config{}, fmt.Errorf("initialization relation unavailable or ambiguous")
	}
	return found[0], nil
}
func Diagnose(v program.ReadView, ref sdk.ObjectRef) (program.Verdict, error) {
	config, err := Configuration(v, ref)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	users, err := v.DatabaseUsers(config.Target)
	if err != nil {
		return program.Verdict{Result: program.Unknown}, err
	}
	for _, user := range users {
		if user.Name == config.User && len(user.Roles) == 1 && user.Roles[0] == (program.DatabaseRole{Database: config.Target.Database, Name: "readWrite"}) {
			return program.Verdict{Result: program.NoMatch}, nil
		}
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: ref, Fact: "application catalog identity", Observed: "absent or different", Expected: "current configured user and direct role"}}}, nil
}
