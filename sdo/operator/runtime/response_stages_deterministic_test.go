package runtime

import (
	"context"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// observedDeploymentRollout plays the deployment controller for a client-go
// fake, which never populates workload status. A completed wait records the
// observed rollout in the cluster, so the executor and the controller's
// independent closure check see the same rollout state.
type observedDeploymentRollout struct {
	client kubernetes.Interface
	waits  *int
}

func (r observedDeploymentRollout) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	if r.waits != nil {
		*r.waits++
	}
	for _, ref := range refs {
		if ref.Kind != "Deployment" {
			continue
		}
		deployment, err := r.client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		deployment.Status = appsv1.DeploymentStatus{ObservedGeneration: deployment.Generation, Replicas: replicas, UpdatedReplicas: replicas, ReadyReplicas: replicas, AvailableReplicas: replicas}
		if _, err := r.client.AppsV1().Deployments(ref.Namespace).UpdateStatus(ctx, deployment, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

type pipelinePortProgram struct{ fastloopPortProgram }

func (pipelinePortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}

func (pipelinePortProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromSource(b.Objects["service"], "spec.ports[0].targetPort"), Reason: "restore accepted port"}}}, nil
}

type pipelineImageProgram struct{ fastloopImageProgram }

func (pipelineImageProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}}}
}

func (pipelineImageProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["deployment"], Path: "spec.template.spec.containers[0].image", Value: program.FromSource(b.Objects["deployment"], "spec.template.spec.containers[0].image"), Reason: "restore accepted image"}}}, nil
}

type pipelineAcceptedSource struct{ client TypedPlanClient }

func (s pipelineAcceptedSource) AcceptedManifest(ctx context.Context, _ string, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	object, err := s.client.Get(ctx, ref)
	if err != nil {
		return nil, err
	}
	switch ref.Kind {
	case "Service":
		services, err := s.client.Client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		services.APIVersion = "v1"
		services.Kind = "Service"
		services.Spec.Ports[0].TargetPort.IntVal = 8080
		object.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(services)
		return object, err
	case "Deployment":
		deployment, err := s.client.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		deployment.APIVersion = "apps/v1"
		deployment.Kind = "Deployment"
		deployment.Spec.Template.Spec.Containers[0].Image = "good:v1"
		object.Object, err = runtime.DefaultUnstructuredConverter.ToUnstructured(deployment)
		return object, err
	}
	return nil, fmt.Errorf("no synthetic accepted manifest for %v", ref)
}

func (s pipelineAcceptedSource) Revision(context.Context, sdk.ObjectRef, string) (*unstructured.Unstructured, error) {
	return nil, fmt.Errorf("no revision fixture")
}

type pipelineIneffectivePortProgram struct{ pipelinePortProgram }

func (pipelineIneffectivePortProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromObserved(b.Objects["service"], "spec.ports[0].targetPort"), Reason: "synthetic ineffective repair"}}}, nil
}

type pipelineDuplicateRegistry struct{ program.ProgramRegistry }

func (r pipelineDuplicateRegistry) List() []program.ProgramSpec {
	specs := r.ProgramRegistry.List()
	return append(specs, specs[0])
}

type pipelineDetector struct{ coverageThreeFaultDetector }

func (pipelineDetector) Spec() sdk.DetectorSpec {
	spec := (fastloopDetector{}).Spec()
	spec.Playbooks = []string{"image-pull-failure", "service-targetport-mismatch"}
	return spec
}

// The dependency is expressed entirely by live diagnose, never by IDs/order.
type pipelineDependentImageProgram struct {
	pipelineImageProgram
	blocked program.Result
}

func (p pipelineDependentImageProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	verdict, err := p.pipelineImageProgram.Diagnose(ctx, view, b)
	if err != nil || verdict.Result != program.Match {
		return verdict, err
	}
	for _, service := range view.Services() {
		if service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			return program.Verdict{Result: p.blocked}, nil
		}
	}
	return verdict, nil
}

func (p pipelineDependentImageProgram) Mitigate(ctx context.Context, view program.ReadView, b program.Bindings) (program.Plan, error) {
	verdict, err := p.Diagnose(ctx, view, b)
	if err != nil || verdict.Result != program.Match {
		return program.Plan{}, errors.New("precondition unmet at planning")
	}
	return p.pipelineImageProgram.Mitigate(ctx, view, b)
}

// A sibling's converged effect may invalidate an opening match. Such a plan
// must never reach the executor, even though opening coverage offered it.
type pipelinePortWhileImageBrokenProgram struct{ pipelinePortProgram }

func (p pipelinePortWhileImageBrokenProgram) Diagnose(ctx context.Context, view program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Spec.Template.Spec.Containers[0].Image == "missing:v1" {
			return p.pipelinePortProgram.Diagnose(ctx, view, b)
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
