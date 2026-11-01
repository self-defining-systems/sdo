package runtime

import (
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"regexp"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
	"strings"
)

type nativePathBackend struct {
	applies  int
	observes int
	revision string
	image    string
}

func (b *nativePathBackend) BuildImage(_ context.Context, _ string, _ *contracts.ImageBuild) (string, error) {
	return "registry.example/demo@sha256:" + strings.Repeat("b", 64), nil
}

func (b *nativePathBackend) Apply(_ context.Context, manifest []byte) error {
	revision := regexp.MustCompile(`sdo.dev/source-tree: ([a-f0-9]{40})`).FindSubmatch(manifest)
	if len(revision) != 2 {
		return fmt.Errorf("trusted adapter did not stamp candidate tree digest")
	}
	b.image = "registry.example/demo@sha256:" + strings.Repeat("b", 64)
	if !strings.Contains(string(manifest), b.image) {
		return fmt.Errorf("trusted adapter did not use immutable image")
	}
	b.revision = string(revision[1])
	b.applies++
	return nil
}

func (b *nativePathBackend) Observe(_ context.Context, id string) (deployment.WorkloadObservation, error) {
	b.observes++
	if id != "Deployment/demo/demo" || b.revision == "" {
		return deployment.WorkloadObservation{}, fmt.Errorf("unobserved workload %q", id)
	}
	return deployment.WorkloadObservation{ResourceID: id, Revision: b.revision, Images: []string{b.image}, Ready: true}, nil
}

type nativePathProbe struct{}

func (nativePathProbe) Evaluate(context.Context, string, string, string) (string, error) {
	return "clear", nil
}

// The task types are whatever the built-in manifests declare for each request
// message; the test restates none of them.
func nativeSourceTask() string { return manifestTaskType(&contracts.SourceDeploymentRequest{}) }

func nativeHealthJudgeTask() string { return manifestTaskType(&contracts.HealthJudgeRequest{}) }

func manifestTaskType(request proto.Message) string {
	registry, err := worker_registry.LoadDirectory(registrytest.Directory())
	if err != nil {
		panic(err)
	}
	agent, err := registry.AgentFor(request)
	if err != nil {
		panic(err)
	}
	task, err := registry.TaskType(agent, request)
	if err != nil {
		panic(err)
	}
	return task
}
