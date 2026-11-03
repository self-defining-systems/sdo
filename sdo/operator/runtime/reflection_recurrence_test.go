package runtime

import ()

const compiledLearningRecurrence = `package generated
import(
 "context";"encoding/json";"os";"path/filepath";"testing";"time"
 metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
 "k8s.io/apimachinery/pkg/util/intstr"
 "k8s.io/apimachinery/pkg/types"

 "k8s.io/client-go/kubernetes/fake"

 contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
 "sdo.dev/operator/memory"
 operator "sdo.dev/operator/runtime"
 "fmt";"sdo.dev/operator/sdk";"sdo.dev/operator/sdk/program";"sdo.dev/operator/sdk/sdktest"
 "sdo.dev/operator/worker_registry"
 "google.golang.org/protobuf/encoding/protojson"
)
type recurrenceProvider struct{client *fake.Clientset}
func(p recurrenceProvider) Snapshot(ctx context.Context)(sdk.DetectionContext,error){
 services,err:=p.client.CoreV1().Services("app").List(ctx,metav1.ListOptions{});if err!=nil{return nil,err}
 pods,err:=p.client.CoreV1().Pods("app").List(ctx,metav1.ListOptions{});if err!=nil{return nil,err}
 return sdktest.Snapshot{NamespaceName:"app",ServiceList:services.Items,PodList:pods.Items},nil
}
// The fake API has no REST normalizer; compare the synthetic Service's declared
// targetPort directly, retaining real Git source reads and program execution.
type recurrenceView struct{*sdktest.ReplayReadView;source operator.GitPlanSource;commit string}
func(v recurrenceView) DiffAgainstSource(ref sdk.ObjectRef)(program.Diff,error){
 accepted,err:=v.source.AcceptedManifest(context.Background(),v.commit,ref);if err!=nil{return program.Diff{},err}
 spec:=accepted.Object["spec"].(map[string]any);ports:=spec["ports"].([]any)
 target:=fmt.Sprint(ports[0].(map[string]any)["targetPort"])
 for _,service:=range v.Services(){if service.Name==ref.Name&&service.Namespace==ref.Namespace{return program.Diff{Changed:service.Spec.Ports[0].TargetPort.String()!=target},nil}}
 return program.Diff{},fmt.Errorf("Service %s absent",ref.Name)
}
type staleProvider struct{recurrenceProvider}
func(p staleProvider) Snapshot(ctx context.Context)(sdk.DetectionContext,error){
 s,err:=p.recurrenceProvider.Snapshot(ctx);if err!=nil{return nil,err}
 snap:=s.(sdktest.Snapshot)
 for i:=range snap.ServiceList{if snap.ServiceList[i].Name=="api"{snap.ServiceList[i].Spec.Ports[0].TargetPort=intstr.FromInt32(9999)}}
 return snap,nil
}
// The repeat applies the saved step, but the fault is not the one the step
// fixes: the live fault reasserts. The real verify must report the finding still
// broken, give the step no credit, and never report health.
func TestCompiledStaleStepStaysBroken(t *testing.T){
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
 raw,err:=os.ReadFile(os.Getenv("RL1_REQUEST"));if err!=nil{t.Fatal(err)}
 var request operator.IncidentRequest;if err:=json.Unmarshal(raw,&request);err!=nil{t.Fatal(err)}
 paths,err:=filepath.Glob(filepath.Join(request.RepositoryWorktree,".sdo/corpus/*/labels.json"));if err!=nil||len(paths)!=1{t.Fatalf("learned cases: %v %v",paths,err)}
 c,err:=sdktest.LoadCorpusCase(filepath.Dir(paths[0]));if err!=nil{t.Fatal(err)}
 client:=fake.NewSimpleClientset()
 for _,item:=range c.Open.ServiceList{if item.UID==""{item.UID=types.UID("synthetic-service-"+item.Name)};if item.ResourceVersion==""{item.ResourceVersion="1"};if _,err:=client.CoreV1().Services("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
 for _,item:=range c.Open.PodList{if item.UID==""{item.UID=types.UID("synthetic-pod-"+item.Name)};if item.ResourceVersion==""{item.ResourceVersion="1"};if _,err:=client.CoreV1().Pods("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
 provider:=staleProvider{recurrenceProvider{client}};snapshot,err:=provider.Snapshot(ctx);if err!=nil{t.Fatal(err)}
 allDetectors:=All()
 var detectors []sdk.Detector
 for _,d:=range allDetectors{if d.Spec().ID=="routing-relations"{detectors=append(detectors,d)}}
 if len(detectors)!=1{t.Fatal("learned detector not compiled")}
 request.Findings,err=detectors[0].Detect(ctx,snapshot);if err!=nil||len(request.Findings)!=1{t.Fatalf("fault not surfaced: %+v %v",request.Findings,err)}
 registry,err:=Programs();if err!=nil{t.Fatal(err)}
 loop:=operator.NewResponderLoop(request,registry,provider,allDetectors,operator.PlanExecutor{Client:operator.TypedPlanClient{Client:client},Source:operator.GitPlanSource{Repository:request.RepositoryWorktree,ManifestPaths:[]string{"api.yaml","peer.yaml"}},Rollouts:operator.KubernetesPlanRollouts{Client:client},Verifier:operator.PlanHealthVerifier{Detectors:allDetectors,Provider:provider}},1)
 loop.ReadView=func(s sdk.DetectionContext) program.ReadView{return recurrenceView{ReplayReadView:&sdktest.ReplayReadView{Snapshot:s.(sdktest.Snapshot)},source:operator.GitPlanSource{Repository:request.RepositoryWorktree,ManifestPaths:[]string{"api.yaml","peer.yaml"}},commit:request.SourceCommit}}
 loop.MenuConfig=&operator.ResponderMenuConfig{}
 menu:=loop.Handle(ctx,operator.ToolRequest{Command:"menu"})
 applied:=false
 for _,option:=range menu.Menu.Options{if option.ProgramId=="service-targetport-mismatch"{
  loop.Handle(ctx,operator.ToolRequest{Command:"mitigate",Selection:&contracts.MitigationSelection{OptionId:option.OptionId,RoundLimit:1}});applied=true
 }}
 if !applied{t.Fatal("saved step not offered on the repeat")}
 verified:=loop.Handle(ctx,operator.ToolRequest{Command:"verify"})
 if verified.Healthy||verified.TargetedHealthy{t.Fatalf("stale step reported health: %+v",verified)}
 if len(verified.Findings)!=1||verified.Findings[0].Fingerprint!=request.Findings[0].Fingerprint{t.Fatalf("verify did not report exactly the still-broken finding: %+v",verified.Findings)}
 for _,application:=range loop.PlanApplications(){if application.Result.VerificationPassed{t.Fatalf("stale step earned credit: %+v",application.Result)}}
}
func TestCompiledIdenticalRecurrence(t *testing.T){
 ctx,cancel:=context.WithTimeout(context.Background(),15*time.Second);defer cancel()
 raw,err:=os.ReadFile(os.Getenv("RL1_REQUEST"));if err!=nil{t.Fatal(err)}
 var request operator.IncidentRequest;if err:=json.Unmarshal(raw,&request);err!=nil{t.Fatal(err)}
 paths,err:=filepath.Glob(filepath.Join(request.RepositoryWorktree,".sdo/corpus/*/labels.json"));if err!=nil||len(paths)!=1{t.Fatalf("learned cases: %v %v",paths,err)}
 c,err:=sdktest.LoadCorpusCase(filepath.Dir(paths[0]));if err!=nil{t.Fatal(err)}
 client:=fake.NewSimpleClientset()
 for _,item:=range c.Open.ServiceList{if item.UID==""{item.UID=types.UID("synthetic-service-"+item.Name)};if item.ResourceVersion==""{item.ResourceVersion="1"};if _,err:=client.CoreV1().Services("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
 for _,item:=range c.Open.PodList{if item.UID==""{item.UID=types.UID("synthetic-pod-"+item.Name)};if item.ResourceVersion==""{item.ResourceVersion="1"};if _,err:=client.CoreV1().Pods("app").Create(ctx,item.DeepCopy(),metav1.CreateOptions{});err!=nil{t.Fatal(err)}}
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
  loop:=operator.NewResponderLoop(r,registry,provider,allDetectors,operator.PlanExecutor{Client:operator.TypedPlanClient{Client:client},Source:operator.GitPlanSource{Repository:request.RepositoryWorktree,ManifestPaths:[]string{"api.yaml","peer.yaml"}},Rollouts:operator.KubernetesPlanRollouts{Client:client},Verifier:operator.PlanHealthVerifier{Detectors:allDetectors,Provider:provider}},1)
  loop.ReadView=func(s sdk.DetectionContext) program.ReadView{return recurrenceView{ReplayReadView:&sdktest.ReplayReadView{Snapshot:s.(sdktest.Snapshot)},source:operator.GitPlanSource{Repository:request.RepositoryWorktree,ManifestPaths:[]string{"api.yaml","peer.yaml"}},commit:request.SourceCommit}}
  loop.MenuConfig=&operator.ResponderMenuConfig{};lastLoop=loop;return loop
 }
 prepared,err:=dispatcher.PrepareDispatch(ctx,request);if err!=nil{t.Fatal(err)}
 task:=new(contracts.AgentTask);if err:=protojson.Unmarshal([]byte(prepared.PreparedAgentTask),task);err!=nil{t.Fatal(err)}
 typed:=new(contracts.IncidentRequest);if err:=task.GetRequest().UnmarshalTo(typed);err!=nil{t.Fatal(err)}
 if len(typed.GetPlaybookCoverage().GetMatches())!=1{t.Fatalf("recurrence coverage: %v; runs=%v",typed.GetPlaybookCoverage(),typed.GetInitialProgramRuns())}
 if len(lastLoop.PlanApplications())!=0{t.Fatal("controller applied before agent")}
 menu:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"menu"})
 for _,option:=range menu.Menu.Options{if option.ProgramId==typed.PlaybookCoverage.Matches[0].PlaybookId{
  applied:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"mitigate",Selection:&contracts.MitigationSelection{OptionId:option.OptionId,RoundLimit:1}});if applied.Error!=""{t.Fatal(applied.Error)}
 }}
 verified:=lastLoop.Handle(ctx,operator.ToolRequest{Command:"verify"});if !verified.Healthy{t.Fatalf("agent repair did not verify: %+v; applications=%+v",verified,lastLoop.PlanApplications())}
 service,err:=client.CoreV1().Services("app").Get(ctx,"api",metav1.GetOptions{});if err!=nil||service.Spec.Ports[0].TargetPort.IntVal!=8080{t.Fatalf("identical recurrence not repaired: %+v %v",service,err)}
 t.Log("incident 1 committed programs and detector; identical incident 2 exposes compiled program coverage and is repaired by scripted agent selection")
}
`
