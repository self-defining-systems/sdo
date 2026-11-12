package runtime

import (
	"bytes"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/worker_registry"
)

func incidentRequestToProto(request IncidentRequest) (*contracts.IncidentRequest, error) {
	return incidentRequestContract(request)
}

func incidentResultToProto(result IncidentResult) (*contracts.IncidentResult, error) {
	return incidentResultContract(result)
}

// encodeIncidentAgentTask builds the validated AgentTask for one incident. The
// task type is whatever the selected agent's registration declares for the
// incident request message.
func encodeIncidentAgentTask(request IncidentRequest, registry *worker_registry.Registry, agentID string, snapshots ...*contracts.OperationalMemorySnapshot) (*contracts.AgentTask, []byte, error) {
	typedRequest, err := incidentRequestToProto(request)
	if err != nil {
		return nil, nil, err
	}
	if len(snapshots) > 1 {
		return nil, nil, fmt.Errorf("incident task accepts one memory snapshot")
	}
	if len(snapshots) == 1 {
		if snapshots[0] == nil || snapshots[0].GetAcceptedCommit() != request.RepositoryBaseCommit {
			return nil, nil, fmt.Errorf("memory snapshot differs from broker proposal base commit")
		}
		if snapshots[0].GetCodePath().GetName() != "shell" {
			// Evidence can quote agent prose naming memory files; a native
			// worker sees it in logical form. The snapshot, attached after, is
			// SDO's own logical memory, so the guard below still refuses a leak there.
			memory.LogicalizeMessage(typedRequest)
		}
		typedRequest.MemorySnapshot = snapshots[0]
		if request.ExternalMessage != "" {
			typedRequest.MemorySnapshot = proto.Clone(snapshots[0]).(*contracts.OperationalMemorySnapshot)
			typedRequest.MemorySnapshot.Detectors = nil
		}
	}
	taskType, err := registry.TaskType(agentID, typedRequest)
	if err != nil {
		return nil, nil, err
	}
	build := func() (*contracts.AgentTask, error) {
		packed, err := anypb.New(typedRequest)
		if err != nil {
			return nil, fmt.Errorf("pack incident request: %w", err)
		}
		return &contracts.AgentTask{
			SchemaVersion: worker_registry.SchemaVersion,
			TaskId:        request.IncidentID,
			AgentId:       agentID,
			TaskType:      taskType,
			Request:       packed,
			Workspace: &contracts.ProposalWorkspace{
				ProposalId:   request.IncidentID,
				CheckoutPath: request.RepositoryWorktree,
				BaseCommit:   request.RepositoryBaseCommit,
			},
		}, nil
	}
	fits, steps, err := fitIncidentRequest(typedRequest, build)
	if err != nil {
		return nil, nil, err
	}
	task, err := build()
	if err != nil {
		return nil, nil, err
	}
	if !fits {
		size, _ := wireSize(task)()
		return nil, nil, &worker_registry.RequestTooLargeError{TaskID: task.GetTaskId(), Bytes: size, Limit: worker_registry.MaxRequestBytes, Components: append([]string{"after every reduction: " + describeFit(steps)}, worker_registry.RequestComponents(task)...)}
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return nil, nil, fmt.Errorf("encode agent task: %w", err)
	}
	if len(snapshots) == 1 && snapshots[0].GetCodePath().GetName() != "shell" && bytes.Contains(payload, []byte(layout.PhysicalPathMarker)) {
		return nil, nil, fmt.Errorf("physical operational memory path leaked into native incident task")
	}
	return task, payload, nil
}

// incidentResultFromAgent checks a worker's AgentResult against its task and
// unpacks the incident result it carries.
func incidentResultFromAgent(result *contracts.AgentResult, task *contracts.AgentTask, registry *worker_registry.Registry) (IncidentResult, error) {
	if err := registry.ValidateResult(task, result); err != nil {
		return IncidentResult{}, fmt.Errorf("validate agent result: %w", err)
	}
	if result.GetResponse() == nil {
		return IncidentResult{}, fmt.Errorf("agent task %q %s: %s", task.GetTaskId(), result.GetStatus(), result.GetErrorSummary())
	}
	typed := &contracts.IncidentResult{}
	if err := result.GetResponse().UnmarshalTo(typed); err != nil {
		return IncidentResult{}, fmt.Errorf("unpack incident result: %w", err)
	}
	if err := wire.Validate(typed); err != nil {
		return IncidentResult{}, err
	}
	return incidentResultState(withoutResponderOwners(typed)), nil
}

// withoutResponderOwners drops owner chains from worker-reported repair
// receipts. Cause attribution follows only owner references SDO observed
// itself (plan pre-images, the incident capture), never the agent's shell.
func withoutResponderOwners(result *contracts.IncidentResult) *contracts.IncidentResult {
	for _, action := range result.GetRepairActions() {
		action.Owners = nil
		action.ScriptDigest = ""
	}
	return result
}

// fitIncidentRequest degrades a responder request in a fixed order until the frozen task fits the transport
// limit, recording each lossy step on the request: prior-incident evidence, then memory the incident does not
// touch, then program verdict evidence text, then raw program runs and old detector evaluations. It reports false,
// with every step tried, when the request still does not fit.
func fitIncidentRequest(request *contracts.IncidentRequest, build func() (*contracts.AgentTask, error)) (bool, []fitStep, error) {
	size := func() (int, error) {
		task, err := build()
		if err != nil {
			return 0, err
		}
		return wireSize(task)()
	}
	mentions := func() []byte {
		stripped := proto.Clone(request).(*contracts.IncidentRequest)
		stripped.MemorySnapshot = nil
		text, _ := wire.Marshal(stripped)
		return text
	}
	steps, fits, err := degradeToFit(size, []transportRung{
		{"compact_prior_response_progress", func() bool {
			before := progressWireBytes(request.PriorResponseProgress)
			request.PriorResponseProgress = boundResponseProgress(request.PriorResponseProgress, 4<<10)
			return progressWireBytes(request.PriorResponseProgress) < before
		}},
		{"drop_prior_incident_history", func() bool {
			changed := len(request.RelevantOutcomes) > 0
			request.RelevantOutcomes = nil
			return changed
		}},
		{"trim_memory_to_touched", func() bool { return trimSnapshotToTouched(request.MemorySnapshot, mentions()) }},
		{"clip_memory_documents", func() bool { return clipSnapshotDocuments(request.MemorySnapshot) }},
		{"clip_program_evidence", func() bool { return clipProgramEvidence(request) }},
		{"cap_program_runs_and_detector_history", func() bool {
			runs, changed := capProgramRuns(request.InitialProgramRuns)
			request.InitialProgramRuns = runs
			if len(request.DetectorHistory) > keptDetectorRuns {
				request.DetectorHistory, changed = request.DetectorHistory[len(request.DetectorHistory)-keptDetectorRuns:], true
			}
			return changed
		}},
	})
	request.Compaction = append(request.Compaction, appliedSteps(steps)...)
	return fits, steps, err
}
