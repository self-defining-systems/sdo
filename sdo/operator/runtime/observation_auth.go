package runtime

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk/program"
)

// DiscoverDatabaseObservationAuth selects references from mounted source, never
// credential values. Explicit target auth and standard root env declarations
// win. Only an unambiguous literal bootstrap pair is eligible; Secret volumes,
// envFrom contents, shell expansion and arbitrary credential guessing are excluded.
func DiscoverDatabaseObservationAuth(ctx context.Context, client TypedPlanClient, pod corev1.Pod, target program.DatabaseTarget) (program.DatabaseTarget, error) {
	if target.Auth != nil {
		return target, nil
	}
	mounted := map[string]bool{}
	for _, container := range pod.Spec.Containers {
		if container.Name == target.Container {
			for _, mount := range container.VolumeMounts {
				mounted[mount.Name] = true
			}
		}
	}
	type source struct {
		name string
		keys []string
	}
	var sources []source
	for _, volume := range pod.Spec.Volumes {
		if !mounted[volume.Name] {
			continue
		}
		if cm := volume.ConfigMap; cm != nil {
			s := source{name: cm.Name}
			for _, item := range cm.Items {
				s.keys = append(s.keys, item.Key)
			}
			sources = append(sources, s)
		}
		if projected := volume.Projected; projected != nil {
			for _, projection := range projected.Sources {
				if cm := projection.ConfigMap; cm != nil {
					s := source{name: cm.Name}
					for _, item := range cm.Items {
						s.keys = append(s.keys, item.Key)
					}
					sources = append(sources, s)
				}
			}
		}
	}
	// Root/bootstrap declarations are conventions of Mongo container inputs, not
	// application or benchmark identities. Do not try credentials against Mongo.
	pairs := [][2]string{{"MONGO_INITDB_ROOT_USERNAME", "MONGO_INITDB_ROOT_PASSWORD"}, {"MONGODB_ROOT_USER", "MONGODB_ROOT_PASSWORD"}, {"ROOT_USER", "ROOT_PWD"}, {"ROOT_USER", "ROOT_PASSWORD"}, {"ADMIN_USER", "ADMIN_PWD"}, {"ADMIN_USER", "ADMIN_PASSWORD"}}
	type candidate struct {
		auth *program.DatabaseAuth
		rank int
	}
	var candidates []candidate
	seen := map[string]bool{}
	for _, source := range sources {
		cm, err := client.credentialConfigMap(ctx, pod.Namespace, source.name)
		if err != nil {
			return target, &CredentialReferenceError{Reference: program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: source.name}}, Reason: "mounted authentication source unavailable"}
		}
		keys := source.keys
		if len(keys) == 0 {
			for key := range cm.Data {
				keys = append(keys, key)
			}
			sort.Strings(keys)
		}
		for _, key := range keys {
			content, ok := cm.Data[key]
			if !ok {
				continue
			}
			for rank, pair := range pairs {
				_, username := assignmentCredential(content, pair[0])
				_, password := assignmentCredential(content, pair[1])
				if !username || !password {
					continue
				}
				reference := func(name string) program.CredentialRef {
					return program.CredentialRef{ConfigMap: &program.ConfigMapCredentialRef{Name: source.name, Key: key, Assignment: name}}
				}
				identity := source.name + "/" + key + "/" + pair[0] + "/" + pair[1]
				if seen[identity] {
					continue
				}
				seen[identity] = true
				candidates = append(candidates, candidate{auth: &program.DatabaseAuth{Database: "admin", Username: reference(pair[0]), Password: reference(pair[1])}, rank: rank})
			}
		}
	}
	if len(candidates) == 0 {
		return target, nil
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].rank < candidates[j].rank })
	if len(candidates) > 1 && candidates[0].rank == candidates[1].rank {
		return target, &DatabaseOperationError{Step: DatabaseAuthentication, Reason: databaseAmbiguousAuth}
	}
	target.Auth = candidates[0].auth
	return target, nil
}
