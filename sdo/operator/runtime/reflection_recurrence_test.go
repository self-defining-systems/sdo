package runtime

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"sdo.dev/operator/builder"
)

// Compile the accepted programs and detector into the recurrence driver. This
// prevents a hand-written test double from standing in for the learned code.
func runCompiledLearningRecurrence(t *testing.T, ctx context.Context, built *builder.Workspace, accepted string, request IncidentRequest) {
	t.Helper()
	request.IncidentID = "identical-recurrence"
	request.SourceCommit = nativeGit(t, accepted, "rev-parse", "HEAD")
	request.RepositoryBaseCommit = request.SourceCommit
	request.DeployedCommit = request.SourceCommit
	request.RepositoryWorktree = accepted
	request.PreparedAgentTask = ""
	request.InitialProgramRuns = nil
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(input, raw, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(built.Directory, "generated/recurrence_test.go")
	if err := os.WriteFile(path, []byte(compiledLearningRecurrence), 0600); err != nil {
		t.Fatal(err)
	}
	registrations, err := filepath.Abs("../../agents/registrations")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "-count=1", "-run", "^TestCompiledIdenticalRecurrence$", "./generated")
	cmd.Dir = built.Directory
	cmd.Env = append(os.Environ(), "RL1_REQUEST="+input, "RL1_REGISTRATIONS="+registrations)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compiled detector/program recurrence: %v: %s", err, output)
	} else {
		t.Logf("compiled recurrence: %s", output)
	}
}

const compiledLearningRecurrence = `package generated
import(
 "context";"encoding/json";"os";"path/filepath";"testing";"time"
 metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

 "k8s.io/client-go/kubernetes/fake"

 contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
 "sdo.dev/operator/memory"
 operator "sdo.dev/operator/runtime"
 "sdo.dev/operator/sdk";"sdo.dev/operator/sdk/program";"sdo.dev/operator/sdk/sdktest"
 "sdo.dev/operator/worker_registry"
 "google.golang.org/protobuf/encoding/protojson"
)
type recurrenceProvider struct{client *fake.Clientset}
func(p recurrenceProvider) Snapshot(ctx context.Context)(sdk.DetectionContext,error){
 services,err:=p.client.CoreV1().Services("app").List(ctx,metav1.ListOptions{});if err!=nil{return nil,err}
 pods,err:=p.client.CoreV1().Pods("app").List(ctx,metav1.ListOptions{});if err!=nil{return nil,err}
 return sdktest.Snapshot{NamespaceName:"app",ServiceList:services.Items,PodList:pods.Items},nil
}
func TestCompiledIdenticalRecurrence(t *testing.T){
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
 raw,err:=os.ReadFile(os.Getenv("RL1_REQUEST"));if err!=nil{t.Fatal(err)}
 var request operator.IncidentRequest;if err:=json.Unmarshal(raw,&request);err!=nil{t.Fatal(err)}
 paths,err:=filepath.Glob(filepath.Join(request.RepositoryWorktree,".sdo/corpus/*/labels.json"));if err!=nil||len(paths)!=1{t.Fatalf("learned cases: %v %v",paths,err)}
 c,err:=sdktest.LoadCorpusCase(filepath.Dir(paths[0]));if err!=nil{t.Fatal(err)}
 client:=fake.NewSimpleClientset()
 for _,item:=range c.Open.ServiceList{if _,err:=client.CoreV1().Services("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
 for _,item:=range c.Open.PodList{if _,err:=client.CoreV1().Pods("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
 provider:=recurrenceProvider{client};snapshot,err:=provider.Snapshot(ctx);if err!=nil{t.Fatal(err)}
 allDetectors:=All()
 var detectors []sdk.Detector
 for _,d:=range allDetectors{if d.Spec().ID=="routing-relations"{detectors=append(detectors,d)}}
 if len(detectors)!=1{t.Fatal("learned detector not compiled")}
 request.Findings,err=detectors[0].Detect(ctx,snapshot);if err!=nil||len(request.Findings)!=1{t.Fatalf("identical fault not surfaced: %+v %v",request.Findings,err)}
 registry,err:=Programs();if err!=nil{t.Fatal(err)}
 workers,err:=worker_registry.LoadBuiltIn(os.Getenv("RL1_REGISTRATIONS"),"scripted:test");if err!=nil{t.Fatal(err)}
 agentID,err:=workers.AgentFor(&contracts.IncidentRequest{});if err!=nil{t.Fatal(err)}
 dispatcher:=operator.KubernetesJobDispatcher{Client:client,Namespace:"app",Registry:workers,AgentID:agentID,ServiceAccount:"scripted",RepositoryPVC:"scripted",RepositoryMountPath:filepath.Dir(request.RepositoryWorktree),Outcomes:memory.OpenReader(request.RepositoryWorktree)}
 var lastLoop *operator.ResponderLoop
 dispatcher.LoopFactory=func(r operator.IncidentRequest)*operator.ResponderLoop{
  loop:=operator.NewResponderLoop(r,registry,provider,allDetectors,operator.PlanExecutor{Client:operator.TypedPlanClient{Client:client},Rollouts:operator.KubernetesPlanRollouts{Client:client},Verifier:operator.PlanHealthVerifier{Detectors:allDetectors,Provider:provider}},1)
  loop.ReadView=func(s sdk.DetectionContext) program.ReadView{return &sdktest.ReplayReadView{Snapshot:s.(sdktest.Snapshot)}}
  loop.MenuConfig=&operator.ResponderMenuConfig{};lastLoop=loop;return loop
 }
 prepared,err:=dispatcher.PrepareDispatch(ctx,request);if err!=nil{t.Fatal(err)}
 task:=new(contracts.AgentTask);if err:=protojson.Unmarshal([]byte(prepared.PreparedAgentTask),task);err!=nil{t.Fatal(err)}
 typed:=new(contracts.IncidentRequest);if err:=task.GetRequest().UnmarshalTo(typed);err!=nil{t.Fatal(err)}
 if len(typed.GetPlaybookCoverage().GetMatches())!=1{t.Fatalf("recurrence coverage: %v",typed.GetPlaybookCoverage())}
 if len(lastLoop.PlanApplications())!=0{t.Fatal("controller applied before agent")}
 menu:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"menu"})
 for _,option:=range menu.Menu.Options{if option.ProgramId==typed.PlaybookCoverage.Matches[0].PlaybookId{
  applied:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"mitigate",Selection:&contracts.MitigationSelection{OptionId:option.OptionId,RoundLimit:1}});if applied.Error!=""{t.Fatal(applied.Error)}
 }}
 verified:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"verify"});if !verified.Healthy{t.Fatalf("agent repair did not verify: %+v",verified)}
 service,err:=client.CoreV1().Services("app").Get(ctx,"api",metav1.GetOptions{});if err!=nil||service.Spec.Ports[0].TargetPort.IntVal!=8080{t.Fatalf("identical recurrence not repaired: %+v %v",service,err)}
 t.Log("incident 1 committed programs and detector; identical incident 2 exposes compiled program coverage and is repaired by scripted agent selection")
}
`
