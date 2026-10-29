package runtime

import (
 "context"
 "testing"
 "time"

 appsv1 "k8s.io/api/apps/v1"
 corev1 "k8s.io/api/core/v1"
 metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
 "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
 kruntime "k8s.io/apimachinery/pkg/runtime"
 "sdo.dev/operator/sdk"
 "sdo.dev/operator/sdk/program"
 "sdo.dev/operator/sdk/sdktest"
)

func recoveryEffectFixture(t *testing.T)(*ResponderLoop,*IncidentState,sdktest.Snapshot,sdk.Finding){
 t.Helper()
 loop,state:=pipelineTestLoop(t,false)
 if err:=RunResponseStages(context.Background(),state,[]ResponseStage{&DeterministicResponseStage{NewLoop:func(IncidentRequest)*ResponderLoop{return loop}}});err!=nil{t.Fatal(err)}
 snapshot,err:=loop.Provider.Snapshot(context.Background());if err!=nil{t.Fatal(err)}
 s:=snapshot.(sdktest.Snapshot)
 d:=s.DeploymentList[0]
 yes:=true;zero:=int32(0)
 rs:=appsv1.ReplicaSet{TypeMeta:metav1.TypeMeta{APIVersion:"apps/v1",Kind:"ReplicaSet"},ObjectMeta:metav1.ObjectMeta{Name:"old",Namespace:d.Namespace,UID:"old-set",OwnerReferences:[]metav1.OwnerReference{{APIVersion:"apps/v1",Kind:"Deployment",Name:d.Name,UID:d.UID,Controller:&yes}}},Spec:appsv1.ReplicaSetSpec{Replicas:&zero,Template:*d.Spec.Template.DeepCopy()}}
 rs.Spec.Template.Spec.Containers[0].Image="old:v1"
 at:=metav1.NewTime(time.Now())
 p:=corev1.Pod{TypeMeta:metav1.TypeMeta{APIVersion:"v1",Kind:"Pod"},ObjectMeta:metav1.ObjectMeta{Name:"draining",Namespace:d.Namespace,UID:"old-pod",DeletionTimestamp:&at,OwnerReferences:[]metav1.OwnerReference{{APIVersion:"apps/v1",Kind:"ReplicaSet",Name:rs.Name,UID:rs.UID,Controller:&yes}}}}
 s.ReplicaSetList=append(s.ReplicaSetList,rs);s.PodList=append(s.PodList,p)
 raw,err:=kruntime.DefaultUnstructuredConverter.ToUnstructured(&p);if err!=nil{t.Fatal(err)}
 ref:=sdk.ObjectRef{APIVersion:"v1",Kind:"Pod",Namespace:p.Namespace,Name:p.Name}
 event:=sdk.DriftEvent{Ref:ref,Source:"absent from accepted source",Fields:inventoryFields(&unstructured.Unstructured{Object:raw})}
 f:=sdk.Finding{DetectorID:"drift.unexplained",RuleID:"unexplained",PrimaryResource:ref,Status:sdk.FindingActive,Metadata:map[string]any{"source_inventory":true,"drift_event":event}}
 return loop,state,s,f
}

// The actual executor's UID-bound write receipts are the producer. Sweep
// identity/ownership/failure variants so the next stale-child class is covered.
func TestRecoveryEffectsRequireCompleteOwnedRepairAndDisappearance(t *testing.T){
 for _,kind:=range []string{"owned","foreign-owner","reused-workload-uid","reused-pod-uid","pending-write","failed-write","real-source-drift"}{
  t.Run(kind,func(t *testing.T){
   loop,_,snapshot,f:=recoveryEffectFixture(t)
   switch kind {
   case "foreign-owner":snapshot.ReplicaSetList[len(snapshot.ReplicaSetList)-1].OwnerReferences[0].UID="foreign"
   case "reused-workload-uid":snapshot.DeploymentList[0].UID="replacement"
   case "reused-pod-uid":snapshot.PodList[len(snapshot.PodList)-1].UID="replacement"
   case "pending-write":for i:=range loop.applicationsLog{loop.applicationsLog[i].Pending=true}
   case "failed-write":for i:=range loop.applicationsLog{loop.applicationsLog[i].Error="failed"}
   case "real-source-drift":event:=inventoryEvent(f);event.Source="deletion pending relative to accepted source";f.Metadata["drift_event"]=event
   }
   effects:=repairedLifecycleEffects(snapshot,loop.applicationsLog,[]sdk.Finding{f})
   if (len(effects)==1)!=(kind=="owned"){t.Fatalf("waiting authority for %s: %v",kind,effects)}
   if len(effects.observe(snapshot,[]sdk.Finding{f}))!=1 {t.Fatal("live child retired")}
   snapshot.PodList[len(snapshot.PodList)-1].UID="name-reused"
   if len(effects.observe(snapshot,[]sdk.Finding{f}))!=1 {t.Fatal("name reuse retired old evidence")}
   snapshot.PodList=snapshot.PodList[:len(snapshot.PodList)-1]
   if got:=len(effects.observe(snapshot,[]sdk.Finding{f}));(got==0)!=(kind=="owned"){t.Fatalf("disappearance consumed unrelated evidence: %d",got)}
  })
 }
}

type recoveryEffectProvider struct{ snapshot sdktest.Snapshot; samples int }
func(p *recoveryEffectProvider)Snapshot(context.Context)(sdk.DetectionContext,error){p.samples++;s:=p.snapshot;if p.samples>2{s.PodList=s.PodList[:len(s.PodList)-1]};return s,nil}

func TestRecoveryEffectsStageWaitConsumesLiveDisappearance(t *testing.T){
 loop,state,snapshot,f:=recoveryEffectFixture(t)
 p:=&recoveryEffectProvider{snapshot:snapshot};loop.Provider=p
 loop.ReadView=func(s sdk.DetectionContext)program.ReadView{return &sdktest.ReplayReadView{Snapshot:s.(sdktest.Snapshot)}}
 state.Findings=[]sdk.Finding{f}
 stage:=&DeterministicResponseStage{ObservationCeiling:100*time.Millisecond,ObservationInterval:time.Millisecond}
 stage.awaitRepairedHealth(context.Background(),loop,state,state.PriorStages[0],time.Second)
 if len(state.Findings)!=0||p.samples<3 {t.Fatalf("own old Pod forced responder: findings=%v samples=%d",state.Findings,p.samples)}
}
