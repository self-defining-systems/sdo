package sdk_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
)

func submittedPod() corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "api"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "api", Image: "registry.example/api:1",
			Ports: []corev1.ContainerPort{{ContainerPort: 8080}},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0.5")},
			},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/ready"}}},
		}}},
	}
}

// defaulted is what the API server returns for submittedPod with no mutating
// admission: defaults only, quantities canonicalized.
func defaulted() corev1.Pod {
	pod := submittedPod()
	grace := int64(30)
	links := true
	priority := int32(0)
	never := corev1.PreemptLowerPriority
	seconds := int64(300)
	pod.Spec.DNSPolicy = corev1.DNSClusterFirst
	pod.Spec.RestartPolicy = corev1.RestartPolicyAlways
	pod.Spec.SchedulerName = "default-scheduler"
	pod.Spec.TerminationGracePeriodSeconds = &grace
	pod.Spec.ServiceAccountName = "default"
	pod.Spec.EnableServiceLinks = &links
	pod.Spec.Priority = &priority
	pod.Spec.PreemptionPolicy = &never
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	pod.Spec.Tolerations = []corev1.Toleration{
		{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
		{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: &seconds},
	}
	pod.Spec.Volumes = []corev1.Volume{{Name: "kube-api-access-x7k2p", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}}}
	c := &pod.Spec.Containers[0]
	c.TerminationMessagePath = "/dev/termination-log"
	c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	c.ImagePullPolicy = corev1.PullIfNotPresent
	c.Ports[0].Protocol = corev1.ProtocolTCP
	c.Resources.Requests[corev1.ResourceCPU] = resource.MustParse("500m")
	c.ReadinessProbe.TimeoutSeconds, c.ReadinessProbe.PeriodSeconds = 1, 10
	c.ReadinessProbe.SuccessThreshold, c.ReadinessProbe.FailureThreshold = 1, 3
	c.ReadinessProbe.HTTPGet.Scheme = corev1.URISchemeHTTP
	c.VolumeMounts = []corev1.VolumeMount{{Name: "kube-api-access-x7k2p", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}}
	return pod
}

func TestAdmissionMutationsIgnoreServerDefaults(t *testing.T) {
	if got := sdk.AdmissionMutations(submittedPod(), defaulted()); len(got) != 0 {
		t.Fatalf("defaulting reported as mutation: %+v", got)
	}
}

func TestAdmissionMutationsNameTheMutatedField(t *testing.T) {
	admitted := defaulted()
	admitted.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("8Mi")}
	admitted.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = resource.MustParse("8Mi")
	admitted.Spec.Containers[0].Image = "registry.example/api:2"
	admitted.Spec.Containers = append(admitted.Spec.Containers, corev1.Container{Name: "proxy", Image: "registry.example/proxy:1"})
	admitted.Labels["injected"] = "true"
	got := sdk.AdmissionMutations(submittedPod(), admitted)
	want := []sdk.FieldChange{
		{Path: "metadata.labels.injected", After: `"true"`},
		{Path: "spec.containers[api].image", Before: `"registry.example/api:1"`, After: `"registry.example/api:2"`},
		{Path: "spec.containers[api].resources.limits.memory", After: `"8Mi"`},
		{Path: "spec.containers[api].resources.requests.memory", After: `"8Mi"`},
		{Path: "spec.containers[proxy]", After: `{"image":"registry.example/proxy:1","name":"proxy","resources":{}}`},
	}
	if len(got) != len(want) {
		t.Fatalf("mutations = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("mutation %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestAdmissionMutationsReportAChangedDefault(t *testing.T) {
	admitted := defaulted()
	admitted.Spec.SchedulerName = "custom"
	submitted := submittedPod()
	submitted.Spec.SchedulerName = "default-scheduler"
	got := sdk.AdmissionMutations(submitted, admitted)
	if len(got) != 1 || got[0].Path != "spec.schedulerName" {
		t.Fatalf("a changed declared value must be a mutation: %+v", got)
	}
}
