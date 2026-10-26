package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/kubeclient"
	"sdo.dev/operator/sdk/program"
)

// KubernetesExecution is an explicit admitted-execution capability. It reuses the
// PlanExecutor action/effect corpus and compensation rather than a second journal.
// Construct it after admission; never pass it to a detector or plan builder.
type KubernetesExecution struct {
	Client   *kubeclient.ExecutionClient
	executor PlanExecutor
	incident IncidentRequest
	mu       sync.Mutex
	result   PlanApplicationResult
}

func (e PlanExecutor) NewKubernetesExecution(view program.ReadView, incident IncidentRequest) (*KubernetesExecution, error) {
	s := &KubernetesExecution{executor: e, incident: incident}
	client, err := program.KubernetesForExecution(view, s)
	if err != nil {
		return nil, err
	}
	s.Client = client
	return s, nil
}

// Result returns a detached receipt including every measured before/after image.
func (s *KubernetesExecution) Result() (PlanApplicationResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(s.result)
	if err != nil {
		return PlanApplicationResult{}, err
	}
	var copy PlanApplicationResult
	err = json.Unmarshal(raw, &copy)
	return copy, err
}
func (s *KubernetesExecution) KubernetesRequest(ctx context.Context, q program.KubernetesRequest) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := q.Validate(); err != nil {
		return nil, err
	}
	parts := strings.Split(strings.Trim(q.Path, "/"), "/")
	i := 2
	group := ""
	version := parts[1]
	if parts[0] == "apis" {
		group = parts[1]
		version = parts[2]
		i = 3
	}
	ns := ""
	if len(parts) > i+2 && parts[i] == "namespaces" {
		ns = parts[i+1]
		i += 2
	}
	if len(parts) <= i {
		return nil, fmt.Errorf("Kubernetes resource path required")
	}
	kind, err := s.Client.RESTMapper().KindFor(schema.GroupVersionResource{Group: group, Version: version, Resource: parts[i]})
	if err != nil {
		return nil, err
	}
	name := ""
	if len(parts) > i+1 {
		name = parts[i+1]
	} else if len(q.Body) > 0 {
		var object struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		_ = json.Unmarshal(q.Body, &object)
		name = object.Metadata.Name
	}
	q.Ref = sdk.ObjectRef{APIVersion: kind.GroupVersion().String(), Kind: kind.Kind, Namespace: ns, Name: name}
	step, err := s.executor.generalStep(ctx, s.incident, q, false)
	offset := len(s.result.Plan.Actions)
	s.result.Plan.Actions = append(s.result.Plan.Actions, q)
	for _, report := range step.ActionReports {
		report.ActionIndex = offset
		s.result.ActionReports = append(s.result.ActionReports, report)
	}
	s.result.Applied = s.result.Applied || step.Applied
	s.result.ActionsApplied = append(s.result.ActionsApplied, step.ActionsApplied...)
	s.result.Patches = append(s.result.Patches, step.Patches...)
	s.result.PreImages = append(s.result.PreImages, step.PreImages...)
	s.result.ObjectEffects = append(s.result.ObjectEffects, step.ObjectEffects...)
	if err != nil {
		return nil, err
	}
	if len(step.ActionReports) == 0 || step.ActionReports[0].KubernetesEffect == nil {
		return nil, fmt.Errorf("Kubernetes mutation response unavailable")
	}
	return step.ActionReports[0].KubernetesEffect.Response, nil
}
