package memoryhost

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/planwire"
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
				wire := &pb.ParamSpec{Name: param.Name, Kind: string(param.Kind), Required: param.Required, ValueKind: string(param.ValueKind)}
				if param.Default != nil {
					if value, err := structpb.NewValue(param.Default); err == nil {
						wire.Default = value
					}
				}
				out.Params = append(out.Params, wire)
			}
			s.identity.Programs = append(s.identity.Programs, out)
			if _, ok := registry.Mitigator(spec.ID); ok {
				s.identity.MitigatorIds = append(s.identity.MitigatorIds, spec.ID)
			}
		}
	}
	return s
}

// WithVerifiedPlans attaches IDs proven by the captured-plan check for this binary.
func (s *Server) WithVerifiedPlans(ids []string) *Server {
	s.identity.VerifiedPlanIds = append([]string(nil), ids...)
	return s
}

// WithTelemetrySources records the accepted revision's telemetry sources file
// (empty when there is none) so Ready carries it to the controller.
func (s *Server) WithTelemetrySources(sources []byte) *Server {
	s.identity.TelemetrySources = append([]byte(nil), sources...)
	return s
}

func programBindings(invocation *pb.MemoryProgramInvocation) (program.Bindings, error) {
	bindings := program.Bindings{Objects: program.ObjectBindings{}}
	for _, b := range invocation.GetBindings() {
		if b == nil {
			return program.Bindings{}, errors.New("nil binding")
		}
		if _, exists := bindings.Objects[b.Name]; exists {
			return program.Bindings{}, errors.New("duplicate binding")
		}
		if _, exists := bindings.Values[b.Name]; exists {
			return program.Bindings{}, errors.New("duplicate binding")
		}
		switch {
		case b.Object != nil && b.Value == nil:
			bindings.Objects[b.Name] = sdk.ObjectRef{APIVersion: b.Object.ApiVersion, Kind: b.Object.Kind, Namespace: b.Object.Namespace, Name: b.Object.Name}
		case b.Value != nil && b.Object == nil:
			if bindings.Values == nil {
				bindings.Values = map[string]any{}
			}
			bindings.Values[b.Name] = b.Value.AsInterface()
		default:
			return program.Bindings{}, errors.New("binding requires exactly one of object or value")
		}
	}
	return bindings, nil
}

// ProgramRPC is the transport-neutral playbook entry point on a leased host.
type ProgramRPC interface {
	ExecuteProgram(context.Context, *pb.MemoryProgramInvocation, func(programread.Message) programread.Message) (*pb.RunProgramResponse, error)
}

func (s *Server) ExecuteProgram(ctx context.Context, invocation *pb.MemoryProgramInvocation, read func(programread.Message) programread.Message) (*pb.RunProgramResponse, error) {
	return s.executeProgram(ctx, invocation, func(call programread.Message) (programread.Message, error) {
		answer := read(call)
		bounded, _ := boundProgramReadReply(answer)
		recordProgramRead(answer, bounded)
		return bounded, nil
	})
}

func (s *Server) executeProgram(ctx context.Context, invocation *pb.MemoryProgramInvocation, exchange func(programread.Message) (programread.Message, error)) (response *pb.RunProgramResponse, err error) {
	if invocation == nil || s.programs == nil {
		return programError(errors.New("program invocation and registry required")), nil
	}
	bindings, err := programBindings(invocation)
	if err != nil {
		return programError(err), nil
	}
	trace := &programPanicTrace{}
	view := &programread.View{Exchange: func(call programread.Message) (programread.Message, error) {
		answer, err := exchange(call)
		if err == nil {
			trace.add(call, answer)
		}
		return answer, err
	}}
	record := func(value any) {
		if value != sdk.ErrReadReplyLimit {
			s.recordProgramPanic(invocation, trace, value)
		}
	}
	defer func() {
		if value := recover(); value != nil {
			if value == sdk.ErrReadReplyLimit {
				response, err = programError(sdk.ErrReadReplyLimit), nil
				return
			}
			record(value)
			response, err = programError(fmt.Errorf("program panic: %v", value)), nil
		}
	}()

	switch invocation.Operation {
	case pb.MemoryProgramInvocation_OPERATION_DIAGNOSE:
		p, ok := s.programs.Diagnoser(invocation.ProgramId)
		if !ok {
			return programError(errors.New("diagnoser not registered")), nil
		}
		verdict := program.RunDiagnose(ctx, recordingDiagnoser{Diagnoser: p, record: record}, view, bindings, time.Hour)
		return &pb.RunProgramResponse{Frame: &pb.RunProgramResponse_Verdict{Verdict: verdictProto(verdict)}}, nil
	case pb.MemoryProgramInvocation_OPERATION_MITIGATE:
		p, ok := s.programs.Mitigator(invocation.ProgramId)
		if !ok {
			return programError(errors.New("mitigator not registered")), nil
		}
		if err := program.ValidateBindings(p.Spec(), bindings); err != nil {
			return programError(err), nil
		}
		plan, err := BuildProgramPlan(ctx, p, view, bindings)
		if aborted := view.ReadAbortError(); aborted != nil {
			err = aborted
		}
		if err != nil {
			return programError(err), nil
		}
		return &pb.RunProgramResponse{Frame: &pb.RunProgramResponse_Plan{Plan: plan}}, nil
	default:
		return programError(errors.New("invalid program operation")), nil
	}
}

func programError(err error) *pb.RunProgramResponse {
	return &pb.RunProgramResponse{Frame: &pb.RunProgramResponse_Error{Error: err.Error()}}
}

func verdictProto(verdict program.Verdict) *pb.Verdict {
	result := pb.Verdict_RESULT_UNKNOWN
	switch verdict.Result {
	case program.Match:
		result = pb.Verdict_RESULT_MATCH
	case program.NoMatch:
		result = pb.Verdict_RESULT_NO_MATCH
	case program.NeedsJudgement:
		result = pb.Verdict_RESULT_NEEDS_JUDGEMENT
	}
	out := &pb.Verdict{Result: result, Question: verdict.Question, CauseClass: verdict.CauseClass, Reach: verdict.Reach}
	for _, e := range verdict.Evidence {
		out.Evidence = append(out.Evidence, &pb.Evidence{Resource: ref(e.Resource), Fact: e.Fact, Observed: e.Observed, Expected: e.Expected})
	}
	return out
}

// BuildProgramPlan validates and encodes the actual program output at the
// host boundary. Captured-plan validation uses this same boundary.
func BuildProgramPlan(ctx context.Context, p program.Mitigator, view program.ReadView, bindings program.Bindings) (*pb.Plan, error) {
	checked := &checkedView{ReadView: view}
	// Programs that honor their context end before the transport gives up, so a
	// slow program is a plan error and never a failed host.
	budget, cancel := withinTransportBudget(ctx)
	defer cancel()
	plan, err := p.Mitigate(budget, checked, bindings)
	if readErr := checked.failure(); readErr != nil {
		err = readErr
	}
	if err == nil {
		err = plan.Validate()
	}
	if err != nil {
		return nil, err
	}
	encoded := &pb.Plan{Rationale: plan.Rationale}
	for _, action := range plan.Actions {
		out, err := planwire.ActionProto(action)
		if err != nil {
			return nil, err
		}
		encoded.Actions = append(encoded.Actions, out)
	}
	return encoded, nil
}

// WithReadFactory installs host-owned readers before serving; no writer is exposed.
func (s *Server) WithReadFactory(factory func(context.Context, sdktest.Snapshot) (program.ReadView, error)) *Server {
	s.readFactory = factory
	return s
}
