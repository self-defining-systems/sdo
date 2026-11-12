package install

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func hostPathSink(t *testing.T) string {
	t.Helper()
	sink := EvidenceSink{RunID: "run-1", Volume: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/var/sdo-evidence"}}}
	encoded, err := sink.Format()
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func evidenceSidecar(pod corev1.PodSpec) (*corev1.Container, *corev1.Volume) {
	var container *corev1.Container
	var volume *corev1.Volume
	for index := range pod.InitContainers {
		if pod.InitContainers[index].Name == "evidence-sink" {
			container = &pod.InitContainers[index]
		}
	}
	for index := range pod.Volumes {
		if pod.Volumes[index].Name == evidenceSinkVolume {
			volume = &pod.Volumes[index]
		}
	}
	return container, volume
}

func TestEvidenceSinkRunsBesideControllerAndOperate(t *testing.T) {
	cfg := policyConfig()
	cfg.EvidenceSink = hostPathSink(t)
	operate, err := OperateJob(cfg, "stay healthy", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := argValue(operate.Spec.Template.Spec.Containers[0].Args, "--evidence-sink"); !ok || got != cfg.EvidenceSink {
		t.Fatalf("operate args lack the sink: %v", operate.Spec.Template.Spec.Containers[0].Args)
	}
	for name, pod := range map[string]corev1.PodSpec{"operate": operate.Spec.Template.Spec, "controller": controllerJob(cfg).Spec.Template.Spec} {
		container, volume := evidenceSidecar(pod)
		if container == nil || volume == nil || volume.HostPath == nil || volume.HostPath.Path != "/var/sdo-evidence" {
			t.Fatalf("%s pod lacks the evidence sidecar or its volume", name)
		}
		if container.RestartPolicy == nil || *container.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			t.Errorf("%s evidence sidecar is not a native sidecar", name)
		}
		if got, ok := argValue(container.Args, "--run-id"); !ok || got != "run-1" {
			t.Errorf("%s evidence sidecar args = %v", name, container.Args)
		}
		for _, mount := range container.VolumeMounts {
			if mount.Name == "repository" && !mount.ReadOnly {
				t.Errorf("%s evidence sidecar can write the repository volume", name)
			}
		}
	}
}

func TestNoEvidenceSinkKeepsTodaysPods(t *testing.T) {
	cfg := policyConfig()
	operate, err := OperateJob(cfg, "stay healthy", "op-1")
	if err != nil {
		t.Fatal(err)
	}
	for name, pod := range map[string]corev1.PodSpec{"operate": operate.Spec.Template.Spec, "controller": controllerJob(cfg).Spec.Template.Spec} {
		if container, volume := evidenceSidecar(pod); container != nil || volume != nil {
			t.Errorf("%s pod has an evidence sidecar without a sink", name)
		}
	}
	if _, ok := argValue(operate.Spec.Template.Spec.Containers[0].Args, "--evidence-sink"); ok {
		t.Error("operate passes --evidence-sink without a sink")
	}
}

func TestEvidenceSinkMustOutliveTheNamespace(t *testing.T) {
	for name, volume := range map[string]corev1.VolumeSource{
		"none":      {},
		"emptyDir":  {EmptyDir: &corev1.EmptyDirVolumeSource{}},
		"secret":    {Secret: &corev1.SecretVolumeSource{SecretName: "x"}},
		"configMap": {ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "x"}}},
		"relative":  {HostPath: &corev1.HostPathVolumeSource{Path: "evidence"}},
		"two":       {HostPath: &corev1.HostPathVolumeSource{Path: "/e"}, NFS: &corev1.NFSVolumeSource{Server: "s", Path: "/e"}},
	} {
		if err := (EvidenceSink{RunID: "run-1", Volume: volume}).Validate(); err == nil {
			t.Errorf("%s volume accepted as an evidence sink", name)
		}
	}
	if err := (EvidenceSink{RunID: "../x", Volume: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/e"}}}).Validate(); err == nil {
		t.Error("unsafe run ID accepted")
	}
	cfg := policyConfig()
	cfg.EvidenceSink = `{"run_id":"run-1","volume":{"emptyDir":{}}}`
	if err := cfg.validate(); err == nil {
		t.Fatal("an emptyDir sink was installed")
	}
}
