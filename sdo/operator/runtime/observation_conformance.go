package runtime

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// DatabaseObservationClient cannot mutate catalogs. Conformance uses the same
// observation implementation as typed actions, never shell-based substitutes.
type DatabaseObservationClient interface {
	DatabaseUsers(context.Context, program.DatabaseTarget) ([]program.DatabaseUser, error)
}
type ObservationConformance struct {
	Passed       bool               `json:"passed"`
	Observations []ObservationCheck `json:"observations"`
}
type ObservationCheck struct {
	Primitive    string                 `json:"primitive"`
	Target       program.DatabaseTarget `json:"target"`
	Passed       bool                   `json:"passed"`
	FailureStage string                 `json:"failure_stage,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

// DatabaseObservationTargets discovers MongoDB containers by their declared
// image or executable. Credentials are references to declared root env names;
// values (including Secret-backed env) are never read by discovery. Explicit
// reference-only overrides support source-declared ConfigMap credentials.
func DatabaseObservationTargets(pods []corev1.Pod) []program.DatabaseTarget {
	var targets []program.DatabaseTarget
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, container := range pod.Spec.Containers {
			image := strings.Split(strings.Split(container.Image, "@")[0], ":")[0]
			name := path.Base(image)
			mongo := name == "mongo" || name == "mongodb"
			if len(container.Command) > 0 {
				mongo = mongo || path.Base(container.Command[0]) == "mongod"
			}
			if !mongo {
				continue
			}
			target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}, Container: container.Name, Database: "admin"}
			username, password := false, false
			for _, env := range container.Env {
				username = username || env.Name == "MONGO_INITDB_ROOT_USERNAME"
				password = password || env.Name == "MONGO_INITDB_ROOT_PASSWORD"
			}
			if username && password {
				target.Auth = &program.DatabaseAuth{Database: "admin", Username: program.CredentialRef{Env: "MONGO_INITDB_ROOT_USERNAME"}, Password: program.CredentialRef{Env: "MONGO_INITDB_ROOT_PASSWORD"}}
			}
			targets = append(targets, target)
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		a, b := targets[i], targets[j]
		return a.Pod.Namespace+"/"+a.Pod.Name+"/"+a.Container < b.Pod.Namespace+"/"+b.Pod.Name+"/"+b.Container
	})
	return targets
}

// CheckObservationConformance runs bounded model-free reads for every matching
// workload. An override changes inputs, never coverage. A failure remains named
// and value-free; absence of matching workloads is an explicit empty inventory.
func CheckObservationConformance(ctx context.Context, client DatabaseObservationClient, pods []corev1.Pod, overrides []program.DatabaseTarget) ObservationConformance {
	report := ObservationConformance{Passed: true, Observations: []ObservationCheck{}}
	targets := DatabaseObservationTargets(pods)
	for _, override := range overrides {
		matched := false
		for i, target := range targets {
			if target.Pod == override.Pod && target.Container == override.Container {
				targets[i] = override
				matched = true
			}
		}
		if !matched {
			targets = append(targets, override)
		}
	}
	for _, target := range targets {
		row := ObservationCheck{Primitive: "DatabaseUsers", Target: target}
		var err error
		if target.Validate() != nil {
			err = &DatabaseOperationError{Step: DatabaseContainerLookup, Reason: databaseRejected}
		} else if client == nil {
			err = databaseOperationFailure(DatabaseExecTransport, nil)
		} else {
			probe, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, err = client.DatabaseUsers(probe, target)
			cancel()
		}
		if err != nil {
			safe := databaseSafeError(err, "database observation: unavailable")
			row.Error = safe.Error()
			row.FailureStage = "observation"
			var operation *DatabaseOperationError
			var reference *CredentialReferenceError
			var container *DatabaseTargetError
			switch {
			case errors.As(safe, &operation):
				row.FailureStage = string(operation.Step)
			case errors.As(safe, &reference):
				row.FailureStage = "credential-reference"
			case errors.As(safe, &container):
				row.FailureStage = string(DatabaseContainerLookup)
			}
			report.Passed = false
		} else {
			row.Passed = true
		}
		report.Observations = append(report.Observations, row)
	}
	return report
}
