package detector

import (
	"context"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Detector struct{}

func New() sdk.Detector { return Detector{} }
func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "mongo-authentication", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Watches: []sdk.WatchKind{{APIVersion: "v1", Kind: "Pod"}}, Interval: 30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityWarning},
		Playbooks: []string{"mongo-auth-bootstrap"}, OriginatingIncident: "mongo-auth-upgrade", OriginatingCommit: "synthetic"}
}
func (Detector) Detect(_ context.Context, v sdk.DetectionContext) ([]sdk.Finding, error) {
	for _, pod := range v.Pods() {
		for _, volume := range pod.Spec.Volumes {
			if volume.ConfigMap == nil {
				continue
			}
			for _, item := range volume.ConfigMap.Items {
				if item.Key != "k8s-geo-mongo.sh" {
					continue
				}
				ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
				credential := func(assignment string) program.CredentialRef {
					return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: volume.ConfigMap.Name, Key: item.Key, Assignment: assignment}}
				}
				target := program.DatabaseTarget{Engine: program.MongoDB, Pod: ref, Container: "mongo", Database: "geo-db", Auth: &program.DatabaseAuth{Database: "admin", Username: credential("ROOT_USER"), Password: credential("ROOT_PWD")}}
				users, err := program.ReadFrom(v).DatabaseUsers(target)
				if err != nil {
					return nil, err
				}
				for _, user := range users {
					if user.Name == "admin" && len(user.Roles) == 1 && user.Roles[0] == (program.DatabaseRole{Database: "geo-db", Name: "readWrite"}) {
						return nil, nil
					}
				}
				return []sdk.Finding{{Status: sdk.FindingActive, Severity: sdk.SeverityWarning, RuleID: "mongo-authentication", PrimaryResource: ref, Summary: "Application MongoDB catalog user missing or roles differ", Evidence: "nonsecret catalog lacks required application user and direct role", Playbooks: []string{"mongo-auth-bootstrap"}}}, nil
			}
		}
	}
	return nil, nil
}
