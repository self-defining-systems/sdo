package buildkit

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Every build of an install runs in one long-lived BuildKit daemon, so the
// layers a failed build already made (and every pulled base image) are local
// for the next build instead of recompiled or re-downloaded from the registry.
// The build Job is only the client that uploads the committed tree.
func TestBuildsShareOneLongLivedDaemon(t *testing.T) {
	root, checkout := proposalCheckout(t)
	b, client := completedBuild(root)
	n := 0
	b.JobSuffix = func() string { n++; return fmt.Sprint(n) }
	for range 2 {
		if _, err := b.buildImage(context.Background(), checkout, testBuild(checkout)); err != nil {
			t.Fatal(err)
		}
	}
	pods, _ := client.CoreV1().Pods("sdo-control").List(context.Background(), metav1.ListOptions{})
	if len(pods.Items) != 1 || pods.Items[0].Name != DaemonName {
		t.Fatalf("want one daemon pod %s, got %v", DaemonName, pods.Items)
	}
	daemon := pods.Items[0]
	if daemon.Labels["sdo.dev/component"] != "image-build" || daemon.Spec.Containers[0].Image != b.BuildKitImage {
		t.Fatalf("daemon pod %+v", daemon.ObjectMeta)
	}
	if _, err := client.CoreV1().Services("sdo-control").Get(context.Background(), DaemonName, metav1.GetOptions{}); err != nil {
		t.Fatalf("daemon service: %v", err)
	}
	_, script := jobEnvironment(t, b, "reg.example/app/api:1")
	if strings.Contains(script, "buildctl-daemonless") || !strings.Contains(script, `buildctl --addr "$SDO_BUILDKIT_ADDR" build`) {
		t.Fatalf("the Job must be a client of the shared daemon:\n%s", script)
	}
	environment, _ := jobEnvironment(t, b, "reg.example/app/api:1")
	if environment["SDO_BUILDKIT_ADDR"] != "tcp://"+DaemonName+".sdo-control.svc:1234" {
		t.Fatalf("daemon address %q", environment["SDO_BUILDKIT_ADDR"])
	}
}

// A daemon from another configuration (a new BuildKit image or registry
// setting) or one that has stopped is replaced; a current one is kept.
func TestStaleOrStoppedDaemonIsReplaced(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.Pod){
		"stale spec": func(p *corev1.Pod) { p.Annotations[daemonSpecAnnotation] = "old" },
		"stopped":    func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed },
	} {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			b := testBackend(t.TempDir(), client, time.Minute)
			stale := b.daemonPod()
			mutate(stale)
			stale.UID = "old"
			if _, err := client.CoreV1().Pods("sdo-control").Create(context.Background(), stale, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := b.ensureDaemon(context.Background()); err != nil {
				t.Fatal(err)
			}
			got, err := client.CoreV1().Pods("sdo-control").Get(context.Background(), DaemonName, metav1.GetOptions{})
			if err != nil || got.UID == "old" || got.Annotations[daemonSpecAnnotation] != b.daemonPod().Annotations[daemonSpecAnnotation] {
				t.Fatalf("daemon not replaced: %+v %v", got, err)
			}
		})
	}
	client := fake.NewSimpleClientset()
	b := testBackend(t.TempDir(), client, time.Minute)
	current := b.daemonPod()
	current.UID = "kept"
	if _, err := client.CoreV1().Pods("sdo-control").Create(context.Background(), current, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := b.ensureDaemon(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, _ := client.CoreV1().Pods("sdo-control").Get(context.Background(), DaemonName, metav1.GetOptions{}); got.UID != "kept" {
		t.Fatal("a current daemon must be kept with its cache")
	}
}

// An insecure lane registry is configured in the daemon, which resolves the
// cache and the push.
func TestDaemonReachesAnInsecureLaneRegistry(t *testing.T) {
	b := testBackend(t.TempDir(), fake.NewSimpleClientset(), time.Minute)
	b.RegistryInsecure = true
	pod := b.daemonPod()
	config := ""
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == "SDO_BUILDKITD_CONFIG" {
			config = env.Value
		}
	}
	if !strings.Contains(config, `[registry."reg.example"]`) || !strings.Contains(config, "http = true") || !strings.Contains(config, `reservedSpace = "20GB"`) {
		t.Fatalf("daemon config %q", config)
	}
	if secure := testBackend(t.TempDir(), fake.NewSimpleClientset(), time.Minute).daemonPod(); secure.Annotations[daemonSpecAnnotation] == pod.Annotations[daemonSpecAnnotation] {
		t.Fatal("the registry setting must be part of the daemon's spec identity")
	}
}

// A client that cannot reach the daemon, or loses it mid-build, ran no build
// step the plan could be blamed for.
func TestUnreachableDaemonIsAnInfrastructureOutcome(t *testing.T) {
	for _, log := range []string{
		"could not connect to sdo-buildkitd after 90 trials\n",
		"#5 [builder 2/7] RUN make\nerror: failed to solve: rpc error: code = Unavailable desc = error reading from server: EOF\n",
	} {
		if failure := ClassifyBuildLog("reg.example/app/api:1", "j", log); !failure.InfrastructureOutcome() {
			t.Fatalf("log %q classified %s", log, failure.Cause)
		}
	}
}
