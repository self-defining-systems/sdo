package runtime

import (
	"context"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"strings"
	"testing"
	"time"
)

type fastloopObjects struct{ client *fake.Clientset }

func (f fastloopObjects) Snapshot(ctx context.Context) (sdk.DetectionContext, error) {
	services, err := f.client.CoreV1().Services("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods, err := f.client.CoreV1().Pods("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	deployments, err := f.client.AppsV1().Deployments("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	sets, err := f.client.AppsV1().ReplicaSets("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	events, err := f.client.CoreV1().Events("app").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: services.Items, PodList: pods.Items, DeploymentList: deployments.Items, ReplicaSetList: sets.Items, EventList: events.Items}, nil
}

type fastloopDetector struct{}

func (fastloopDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "availability", Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge,
		Watches:  []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "apps/v1", Kind: "Deployment"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}, Playbooks: []string{"base"}, OriginatingCommit: "seed"}
}

func (fastloopDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	findings := []sdk.Finding{}
	for _, service := range view.Services() {
		if service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			findings = append(findings, sdk.Finding{DetectorID: "availability", RuleID: "wrong-port", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Service", "v1", &service), Summary: "no ready endpoint", Evidence: "Service targetPort is wrong", Playbooks: []string{"base"}})
		}
	}
	for _, deployment := range view.Deployments() {
		if deployment.Spec.Template.Spec.Containers[0].Image == "missing:v1" {
			findings = append(findings, sdk.Finding{DetectorID: "availability", RuleID: "image-pull", Status: sdk.FindingActive, Severity: sdk.SeverityCritical, PrimaryResource: sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment), Summary: "image pull fails", Evidence: "pod waits for image"})
		}
	}
	return findings, nil
}

type fastloopPortProgram struct{}

func (fastloopPortProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "service-targetport-mismatch", Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: 8080}}}
}

func (fastloopPortProgram) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, service := range view.Services() {
		if service.Name == bindings.Objects["service"].Name && service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (fastloopPortProgram) Mitigate(_ context.Context, _ program.ReadView, bindings program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: bindings.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port"), Reason: "restore declared port"}}}, nil
}

type fastloopImageProgram struct{}

func (fastloopImageProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "image-pull-failure", Params: []program.ParamSpec{{Name: "deployment", Kind: "Deployment", Required: true}, {Name: "image", ValueKind: program.StringValue, Default: "good:v1"}}}
}

func (fastloopImageProgram) Diagnose(_ context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	for _, deployment := range view.Deployments() {
		if deployment.Name == bindings.Objects["deployment"].Name && deployment.Spec.Template.Spec.Containers[0].Image == "missing:v1" {
			for _, pod := range view.Pods() {
				if pod.Labels["app"] != deployment.Name || len(pod.Status.ContainerStatuses) == 0 {
					continue
				}
				if waiting := pod.Status.ContainerStatuses[0].State.Waiting; waiting != nil && (waiting.Reason == "ImagePullBackOff" || waiting.Reason == "ErrImagePull") {
					return program.Verdict{Result: program.Match}, nil
				}
			}
			return program.Verdict{Result: program.Unknown}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (fastloopImageProgram) Mitigate(_ context.Context, _ program.ReadView, bindings program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: bindings.Objects["deployment"], Path: "spec.template.spec.containers[0].image", Value: program.FromParam("image"), Reason: "restore image"}}}, nil
}

func fastloopClient(loadedImages ...string) *fake.Clientset {
	client := fake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "missing:v1", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "fixture", Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "missing:v1", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}}}, Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "api", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}}},
	)
	limit := func(action ktesting.Action) (bool, kruntime.Object, error) {
		var object kruntime.Object
		switch typed := action.(type) {
		case ktesting.CreateAction:
			object = typed.GetObject()
		case ktesting.UpdateAction:
			object = typed.GetObject()
		}
		configMap, ok := object.(*corev1.ConfigMap)
		if !ok {
			return false, nil, nil
		}
		size := 0
		for _, value := range configMap.Data {
			size += len(value)
		}
		if size > 1<<20 {
			return true, nil, fmt.Errorf("ConfigMap exceeds 1 MiB")
		}
		return false, nil, nil
	}
	client.Fake.PrependReactor("create", "configmaps", limit)
	client.Fake.PrependReactor("update", "configmaps", limit)
	if len(loadedImages) > 0 {
		loaded := map[string]bool{}
		for _, image := range loadedImages {
			loaded[image] = true
		}
		mark := func(pod *corev1.Pod) {
			for _, container := range pod.Spec.Containers {
				reason := "ContainerCreating"
				if !loaded[container.Image] {
					reason = "ImagePullBackOff"
				}
				pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container.Name, Image: container.Image, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}})
			}
		}
		client.Fake.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
			mark(action.(ktesting.CreateAction).GetObject().(*corev1.Pod))
			return false, nil, nil
		})
		client.Fake.PrependReactor("update", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
			pod := action.(ktesting.UpdateAction).GetObject().(*corev1.Pod)
			pod.Status.ContainerStatuses = nil
			mark(pod)
			return false, nil, nil
		})
		pod, err := client.CoreV1().Pods("app").Get(context.Background(), "api-pod", metav1.GetOptions{})
		if err != nil {
			panic(err)
		}
		pod.Status.ContainerStatuses = nil
		if _, err := client.CoreV1().Pods("app").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
			panic(err)
		}
	}
	return client
}

func TestHermeticFakePodReportsUnloadedImage(t *testing.T) {
	client := fastloopClient("good:v1")
	pod, err := client.CoreV1().Pods("app").Get(context.Background(), "api-pod", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := pod.Status.ContainerStatuses[0].State.Waiting.Reason; got != "ImagePullBackOff" {
		t.Fatalf("unloaded image state: %s", got)
	}
}

func TestHermeticFakeConfigMapEnforcesProductionLimit(t *testing.T) {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "oversized"}, Data: map[string]string{"state": strings.Repeat("x", 1<<20+1)}}
	_, err := fastloopClient().CoreV1().ConfigMaps("app").Create(context.Background(), configMap, metav1.CreateOptions{})
	if err == nil || !strings.Contains(err.Error(), "1 MiB") {
		t.Fatalf("oversized ConfigMap passed the fake API: %v", err)
	}
}

func fastloopPodWaiting(t *testing.T, ctx context.Context, client *fake.Clientset, name, reason string) {
	t.Helper()
	pod, err := client.CoreV1().Pods("app").Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.ContainerStatuses[0].State.Waiting.Reason = reason
	if _, err := client.CoreV1().Pods("app").Update(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func TestImageProgramsRecognizeBothPullFailurePhases(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
	for _, reason := range []string{"ImagePullBackOff", "ErrImagePull"} {
		t.Run(reason, func(t *testing.T) {
			snapshot := sdktest.Snapshot{NamespaceName: "app", DeploymentList: []appsv1.Deployment{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Image: "missing:v1"}}}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}}}}}}}
			for _, p := range []program.Diagnoser{fastloopImageProgram{}, transientImageProgram{}} {
				verdict := program.RunDiagnose(context.Background(), p, &sdktest.ReplayReadView{Snapshot: snapshot}, program.Bindings{Objects: program.ObjectBindings{"deployment": ref}}, time.Second)
				if verdict.Result != program.Match {
					t.Errorf("%s missed %s: %+v", p.Spec().ID, reason, verdict)
				}
			}
		})
	}
}

// The scripted client follows the production file contract instead of treating
// compact display metadata as the completed execution result.
func fullCommandReportPayload(t *testing.T, encoded []byte) []byte {
	t.Helper()
	var report struct {
		Actions []json.RawMessage `json:"actions"`
	}
	if err := json.Unmarshal(encoded, &report); err != nil {
		t.Fatal(err)
	}
	if report.Actions == nil {
		return encoded
	}
	if len(report.Actions) != 1 {
		t.Fatalf("single command has %d full action results", len(report.Actions))
	}
	return report.Actions[0]
}
