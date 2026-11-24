package memoryhost

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk/program"
)

type refusalFixture struct {
	mitigationError error
}

func (refusalFixture) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "refusal-fixture"} }
func (refusalFixture) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}

func (p refusalFixture) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RestartRollout{}}}, p.mitigationError
}

func TestHostDistinguishesPlanRefusalFromUnavailableRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		err     error
		refused bool
		message string
	}{
		{name: "invalid", refused: true, message: "RestartRollout requires"},
		{name: "unavailable", err: errors.New("live read unavailable"), message: "live read unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, err := NewServer("accepted", "binary", nil)
			if err != nil {
				t.Fatal(err)
			}
			fixture := refusalFixture{mitigationError: tc.err}
			registry, err := program.NewRegistry([]program.Diagnoser{fixture}, []program.Mitigator{fixture})
			if err != nil {
				t.Fatal(err)
			}
			server.WithPrograms(registry)
			reply, err := server.ExecuteProgram(t.Context(), &pb.MemoryProgramInvocation{
				ProgramId: fixture.Spec().ID, Operation: pb.MemoryProgramInvocation_OPERATION_MITIGATE,
			}, func(programread.Message) programread.Message { return programread.Message{} })
			if err != nil || !strings.Contains(reply.GetError(), tc.message) || reply.GetPlan() != nil {
				t.Fatalf("response = %v, %v", reply, err)
			}
			if refused := reply.GetPlanRefused(); refused != tc.refused {
				t.Fatalf("refused = %v, want %v, response = %v", refused, tc.refused, reply)
			}
		})
	}
}
