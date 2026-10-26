package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type clientGoServiceView struct {
	program.ReadView
	client kubernetes.Interface
}

func (v clientGoServiceView) KubernetesRead(q program.KubernetesRequest) (json.RawMessage, error) {
	parts := strings.Split(q.Path, "/")
	object, err := v.client.CoreV1().Services(parts[4]).Get(context.Background(), parts[6], metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return json.Marshal(object)
}

func TestWorkingProgramClientGoReadsUseRecordedJITBridge(t *testing.T) {
	loop, bindings, write := workingProgramLoop(t)
	baseView := loop.ReadView
	client := loop.Executor.Client.(TypedPlanClient).Client
	loop.ReadView = func(snapshot sdk.DetectionContext) program.ReadView {
		return clientGoServiceView{ReadView: baseView(snapshot), client: client}
	}
	const source = `package repair
import("context";metav1 "k8s.io/apimachinery/pkg/apis/meta/v1";"sdo.dev/operator/sdk/program")
type Program struct{}
func(Program)Spec()program.ProgramSpec{return program.ProgramSpec{ID:"service-targetport-mismatch",Params:[]program.ParamSpec{{Name:"service",Kind:"Service",Required:true}}}}
func(Program)Diagnose(ctx context.Context,v program.ReadView,b program.Bindings)(program.Verdict,error){
 client,err:=program.Kubernetes(v);if err!=nil{return program.Verdict{},err}
 ref:=b.Objects["service"]
 service,err:=client.CoreV1().Services(ref.Namespace).Get(ctx,ref.Name,metav1.GetOptions{});if err!=nil{return program.Verdict{},err}
 if service.Spec.Ports[0].TargetPort.IntVal==8080{return program.Verdict{Result:program.NoMatch},nil}
 return program.Verdict{Result:program.Match},nil
}
`
	write("service-targetport-mismatch", "diagnose.go", source)
	response := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if response.Error != "" || len(response.ProgramRuns) != 1 || response.ProgramRuns[0].Verdict.Result != program.Match {
		t.Fatalf("typed working program: %+v", response)
	}
	// A removed upstream field fails independent compilation before execution.
	write("service-targetport-mismatch", "diagnose.go", strings.ReplaceAll(source, "service.Spec.Ports", "service.Spec.RemovedPorts"))
	broken := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if broken.Error == "" {
		t.Fatalf("stale typed API field compiled: %+v", broken)
	}
	// Local client validation errors must also survive the compiled process boundary.
	ignored := strings.Replace(source, "service,err:=client", `_,_=client.CoreV1().Secrets(ref.Namespace).Get(ctx,"private",metav1.GetOptions{})
 service,err:=client`, 1)
	write("service-targetport-mismatch", "diagnose.go", ignored)
	refused := loop.Handle(context.Background(), ToolRequest{Command: "diagnose", ProgramID: "service-targetport-mismatch", Bindings: bindings})
	if refused.Error != "" || len(refused.ProgramRuns) != 1 || refused.ProgramRuns[0].Verdict.Result != program.Unknown {
		t.Fatalf("compiled program ignored local client failure: %+v", refused)
	}

}
