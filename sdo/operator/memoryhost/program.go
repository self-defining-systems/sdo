package memoryhost

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// WithPrograms installs registrations before serving. Constructors execute in
// the child, never in the controller or agent pod.
func (s *Server) WithPrograms(registry program.ProgramRegistry) *Server {
	s.programs = registry
	if registry != nil {
		for _, spec := range registry.List() {
			out := &pb.ProgramSpec{Id: spec.ID}
			for _, param := range spec.Params {
				out.Params = append(out.Params, &pb.ParamSpec{Name: param.Name, Kind: string(param.Kind), Required: param.Required})
			}
			s.identity.Programs = append(s.identity.Programs, out)
			if _, ok := registry.Mitigator(spec.ID); ok {
				s.identity.MitigatorIds = append(s.identity.MitigatorIds, spec.ID)
			}
		}
	}
	return s
}
func (s *Server) programView(ctx context.Context, input *pb.MemoryProgramInput) (*sdktest.RecordingReadView, program.Bindings, error) {
	if input == nil {
		return nil, nil, errors.New("program input required")
	}
	snapshot, err := s.snapshot(input.MirrorSequence)
	if err != nil {
		return nil, nil, err
	}
	if err = observe(&snapshot, input.Observation); err != nil {
		return nil, nil, err
	}
	bindings := program.Bindings{}
	for _, b := range input.Bindings {
		if b.Object == nil {
			return nil, nil, errors.New("binding object required")
		}
		if _, exists := bindings[b.Name]; exists {
			return nil, nil, errors.New("duplicate binding")
		}
		bindings[b.Name] = sdk.ObjectRef{APIVersion: b.Object.ApiVersion, Kind: b.Object.Kind, Namespace: b.Object.Namespace, Name: b.Object.Name}
	}
	var view program.ReadView
	if s.readFactory != nil {
		view, err = s.readFactory(ctx, snapshot)
		if err != nil {
			return nil, nil, err
		}
	} else {
		calls, err := DecodeReceipts(input.Reads)
		if err != nil {
			return nil, nil, err
		}
		view = &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
	}
	if len(input.Reads) > 0 && s.readFactory != nil {
		calls, err := DecodeReceipts(input.Reads)
		if err != nil {
			return nil, nil, err
		}
		view = diffReadView{ReadView: view, diffs: &sdktest.ReplayReadView{Calls: calls}}
	}
	recorder := &sdktest.RecordingReadView{ReadView: view}

	return recorder, bindings, nil
}
func (s *Server) Diagnose(ctx context.Context, request *pb.DiagnoseRequest) (response *pb.DiagnoseResponse, err error) {
	response = &pb.DiagnoseResponse{}
	defer func() {
		if value := recover(); value != nil {
			response.Verdict = nil
			response.Error = fmt.Sprintf("program panic: %v", value)
		}
	}()
	view, bindings, err := s.programView(ctx, request.GetInput())
	if err != nil {
		return nil, err
	}
	if s.programs == nil {
		return nil, errors.New("program registry unavailable")
	}
	p, ok := s.programs.Diagnoser(request.Input.ProgramId)
	if !ok {
		return nil, errors.New("diagnoser not registered")
	}
	if err = program.ValidateBindings(p.Spec(), bindings); err != nil {
		return nil, err
	}
	defer func() {
		if response != nil {
			response.Reads, err = EncodeReceipts(view.SnapshotCalls())
		}
	}()
	verdict := program.RunDiagnose(ctx, p, view, bindings, time.Hour)
	result := pb.Verdict_RESULT_UNKNOWN
	switch verdict.Result {
	case program.Match:
		result = pb.Verdict_RESULT_MATCH
	case program.NoMatch:
		result = pb.Verdict_RESULT_NO_MATCH
	case program.NeedsJudgement:
		result = pb.Verdict_RESULT_NEEDS_JUDGEMENT
	}
	response.Verdict = &pb.Verdict{Result: result, Question: verdict.Question}
	for _, e := range verdict.Evidence {
		response.Verdict.Evidence = append(response.Verdict.Evidence, &pb.Evidence{Resource: ref(e.Resource), Fact: e.Fact, Observed: e.Observed, Expected: e.Expected})
	}
	return response, nil
}
func (s *Server) Mitigate(ctx context.Context, request *pb.MitigateRequest) (response *pb.MitigateResponse, err error) {
	response = &pb.MitigateResponse{}
	defer func() {
		if value := recover(); value != nil {
			response.Plan = nil
			response.Error = fmt.Sprintf("program panic: %v", value)
		}
	}()
	view, bindings, err := s.programView(ctx, request.GetInput())
	if err != nil {
		return nil, err
	}
	if s.programs == nil {
		return nil, errors.New("program registry unavailable")
	}
	p, ok := s.programs.Mitigator(request.Input.ProgramId)
	if !ok {
		return nil, errors.New("mitigator not registered")
	}
	if err = program.ValidateBindings(p.Spec(), bindings); err != nil {
		return nil, err
	}
	defer func() {
		if response != nil {
			reads, encodeErr := EncodeReceipts(view.SnapshotCalls())
			response.Reads = reads
			if encodeErr != nil {
				err = encodeErr
			}
		}
	}()
	checked := &checkedView{ReadView: view}
	plan, err := p.Mitigate(ctx, checked, bindings)
	if readErr := checked.failure(); readErr != nil {
		err = readErr
	}
	if err == nil {
		err = plan.Validate()
	}
	if err != nil {
		response.Error = err.Error()
		return response, nil
	}
	response.Plan = &pb.Plan{Rationale: plan.Rationale}
	for _, action := range plan.Actions {
		out, err := actionProto(action)
		if err != nil {
			return &pb.MitigateResponse{Error: err.Error()}, nil
		}
		response.Plan.Actions = append(response.Plan.Actions, out)
	}
	return response, nil
}
func actionProto(action program.Action) (*pb.Action, error) {
	switch a := action.(type) {
	case program.ReconcileFromSource:
		return &pb.Action{Kind: &pb.Action_ReconcileFromSource{ReconcileFromSource: &pb.ReconcileFromSource{Ref: ref(a.Ref)}}}, nil
	case program.Rollback:
		return &pb.Action{Kind: &pb.Action_Rollback{Rollback: &pb.Rollback{Ref: ref(a.Ref), ToRevision: a.ToRevision}}}, nil
	case program.RestartRollout:
		return &pb.Action{Kind: &pb.Action_RestartRollout{RestartRollout: &pb.RestartRollout{Ref: ref(a.Ref), Reason: a.Reason}}}, nil
	case program.SetField:
		value, err := structpb.NewValue(a.Value)
		if err != nil {
			return nil, err
		}
		return &pb.Action{Kind: &pb.Action_SetField{SetField: &pb.SetField{Ref: ref(a.Ref), Path: a.Path, Value: value, Reason: a.Reason}}}, nil
	case *program.ReconcileFromSource:
		if a != nil {
			return actionProto(*a)
		}
	case *program.Rollback:
		if a != nil {
			return actionProto(*a)
		}
	case *program.RestartRollout:
		if a != nil {
			return actionProto(*a)
		}
	case *program.SetField:
		if a != nil {
			return actionProto(*a)
		}
	}
	return nil, fmt.Errorf("unsupported plan action %T", action)
}

// WithReadFactory installs host-owned readers before serving; no writer is exposed.
func (s *Server) WithReadFactory(factory func(context.Context, sdktest.Snapshot) (program.ReadView, error)) *Server {
	s.readFactory = factory
	return s
}
