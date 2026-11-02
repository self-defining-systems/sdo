package runtime

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/program"
)

type scriptVerifyProgram struct {
	fastloopPortProgram
	remains bool
}

func (p scriptVerifyProgram) Diagnose(ctx context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	if p.remains {
		return program.Verdict{Result: program.Match}, nil
	}
	return p.fastloopPortProgram.Diagnose(ctx, v, b)
}

func (p scriptVerifyProgram) Mitigate(context.Context, program.ReadView, program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.RunScript{Script: `kubectl patch service "$SERVICE" --type merge -p "$PATCH"`, Env: map[string]program.Value{"SERVICE": program.FromParam("name"), "PATCH": program.FromParam("patch")}}}}, nil
}

func (p scriptVerifyProgram) Spec() program.ProgramSpec {
	s := p.fastloopPortProgram.Spec()
	s.Params = append(s.Params, program.ParamSpec{Name: "name", ValueKind: program.StringValue, Default: "first"}, program.ParamSpec{Name: "patch", ValueKind: program.StringValue, Default: `{"spec":{"ports":[{"port":80,"targetPort":8080}]}}`})
	return s
}

type verifyingScriptClient struct{ TypedPlanClient }

func (c verifyingScriptClient) RunScript(ctx context.Context, r ScriptExecutionRequest) (ScriptExecutionResult, error) {
	service, err := c.Client.CoreV1().Services("app").Get(ctx, r.Env["SERVICE"], metav1.GetOptions{})
	if err != nil {
		return ScriptExecutionResult{}, err
	}
	service.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	_, err = c.Client.CoreV1().Services("app").Update(ctx, service, metav1.UpdateOptions{})
	code := int32(0)
	return ScriptExecutionResult{ApplicationID: r.ApplicationID, JobName: "script-job", JobUID: "script-uid", ScriptDigest: program.ScriptDigest(r.Script), ExitCode: &code}, err
}

