package install

import (
	"context"
	"errors"
	"fmt"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"testing"
	"time"
)

func TestClaimRecoveryLifecycleCuts(t *testing.T) {
	for _, tc := range []struct {
		name, kind, uid      string
		age                  time.Duration
		present, wantBlocked bool
	}{
		{"crash-before-create", "Job", "", time.Minute, false, false},
		{"creation-window", "Job", "", time.Second, false, true},
		{"deleted-job", "Job", "gone", time.Second, false, false},
		{"pending-job", "Job", "live", time.Hour, true, true},
		{"deleted-sync", "Pod", "gone", time.Second, false, false},
		{"live-sync", "Pod", "live", time.Hour, true, true},
		{"legacy-sync-orphan", "", "", time.Minute, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			owner := "old"
			name := OperateJobName(owner)
			if tc.kind == "Pod" || tc.name == "legacy-sync-orphan" {
				owner = "sync-old"
				name = RepositorySyncPodName
			}
			claim := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace, UID: "claim-old", CreationTimestamp: metav1.NewTime(time.Now().Add(-tc.age))}, Data: map[string]string{"operation_id": owner, "owner_kind": tc.kind, "owner_name": name, "owner_uid": tc.uid}}
			client := fake.NewSimpleClientset(claim)
			if tc.present {
				if tc.kind == "Pod" {
					_, _ = client.CoreV1().Pods(cfg.ControlNamespace).Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "live"}}, metav1.CreateOptions{})
				} else {
					_, _ = client.BatchV1().Jobs(cfg.ControlNamespace).Create(context.Background(), &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, UID: "live"}}, metav1.CreateOptions{})
				}
			}
			_, err := (Installer{Client: client}).acquireOperateLock(context.Background(), cfg, "new", metav1.OwnerReference{Kind: "Job", Name: OperateJobName("new")})
			if (err != nil) != tc.wantBlocked {
				t.Fatalf("blocked=%v want=%v: %v", err != nil, tc.wantBlocked, err)
			}
		})
	}
}

func TestStopReleasesOrphanSyncClaim(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: "demo", UID: "claim", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute))}, Data: map[string]string{"operation_id": "sync-abandoned"}})
	if _, err := Stop(context.Background(), client, "demo"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().ConfigMaps("demo").Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("orphan claim survived supported stop: %v", err)
	}
}

// claimAPI enforces Kubernetes UID/resourceVersion preconditions, which the
// stock object tracker omits. The production clients still exercise every call.
func claimAPI(objects ...kruntime.Object) *fake.Clientset {
	client := fake.NewSimpleClientset(objects...)
	var serial int
	client.PrependReactor("*", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		resource := action.GetResource()
		ns := action.GetNamespace()
		switch action.GetVerb() {
		case "create":
			a := action.(ktesting.CreateAction)
			obj := a.GetObject().(*corev1.ConfigMap).DeepCopy()
			serial++
			obj.UID = types.UID(fmt.Sprintf("claim-%d", serial))
			obj.ResourceVersion = fmt.Sprint(serial)
			obj.CreationTimestamp = metav1.Now()
			err := client.Tracker().Create(resource, obj, ns)
			return true, obj, err
		case "update":
			a := action.(ktesting.UpdateAction)
			obj := a.GetObject().(*corev1.ConfigMap).DeepCopy()
			old, err := client.Tracker().Get(resource, ns, obj.Name)
			if err != nil {
				return true, nil, err
			}
			prior := old.(*corev1.ConfigMap)
			if obj.UID != prior.UID || obj.ResourceVersion != prior.ResourceVersion {
				return true, nil, apierrors.NewConflict(resource.GroupResource(), obj.Name, errors.New("reservation replaced or changed"))
			}
			serial++
			obj.ResourceVersion = fmt.Sprint(serial)
			err = client.Tracker().Update(resource, obj, ns)
			return true, obj, err
		case "delete":
			a := action.(ktesting.DeleteAction)
			old, err := client.Tracker().Get(resource, ns, a.GetName())
			if err != nil {
				return true, nil, err
			}
			prior := old.(*corev1.ConfigMap)
			pre := a.GetDeleteOptions().Preconditions
			if pre == nil || pre.UID == nil || pre.ResourceVersion == nil || *pre.UID != prior.UID || *pre.ResourceVersion != prior.ResourceVersion {
				return true, nil, apierrors.NewConflict(resource.GroupResource(), a.GetName(), errors.New("delete must fence UID and resourceVersion"))
			}
			return true, nil, client.Tracker().Delete(resource, ns, a.GetName())
		}
		return false, nil, nil
	})
	for _, resource := range []string{"jobs", "pods"} {
		client.PrependReactor("create", resource, func(action ktesting.Action) (bool, kruntime.Object, error) {
			obj := action.(ktesting.CreateAction).GetObject().(metav1.Object)
			serial++
			obj.SetUID(types.UID(fmt.Sprintf("owner-%d", serial)))
			return false, nil, nil
		})
	}
	return client
}

func TestOperateLaunchersRaceAndBindBeforeActivation(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	client := claimAPI(
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cfg.RepositoryPVC, Namespace: cfg.ControlNamespace, Annotations: map[string]string{PreparedCommitAnnotation: cfg.AcceptedCommit}}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.RegistryPushSecret, Namespace: cfg.ControlNamespace}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte("{}")}},
	)
	client.PrependReactor("update", "jobs", func(action ktesting.Action) (bool, kruntime.Object, error) {
		job := action.(ktesting.UpdateAction).GetObject().(*batchv1.Job)
		if job.Spec.Suspend != nil && !*job.Spec.Suspend {
			raw, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cfg.ControlNamespace, OperateLockConfigMap)
			if err != nil {
				t.Error(err)
				return true, nil, err
			}
			cm := raw.(*corev1.ConfigMap)
			if cm.Data["owner_uid"] != string(job.UID) || len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != job.UID {
				t.Error("Job activated without durable UID ownership")
			}
		}
		return false, nil, nil
	})
	results := make(chan error, 2)
	for _, id := range []string{"launcher-a", "launcher-b"} {
		go func(id string) {
			_, err := (Installer{Client: client}).StartOperate(ctx, cfg, "healthy", id, 1)
			results <- err
		}(id)
	}
	successes := 0
	for range 2 {
		if <-results == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful launchers=%d, want one", successes)
	}
	jobs, _ := client.BatchV1().Jobs(cfg.ControlNamespace).List(ctx, metav1.ListOptions{})
	if len(jobs.Items) != 1 {
		t.Fatalf("Jobs=%d, want one", len(jobs.Items))
	}
}

func TestExpiredLauncherCannotBindReplacementReservation(t *testing.T) {
	for _, sameOperation := range []bool{false, true} {
		t.Run(fmt.Sprint(sameOperation), func(t *testing.T) {
			cfg := testConfig()
			ctx := context.Background()
			client := claimAPI()
			installer := Installer{Client: client}
			old, err := installer.acquireOperateLock(ctx, cfg, "a", metav1.OwnerReference{Kind: "Job", Name: OperateJobName("a")})
			if err != nil {
				t.Fatal(err)
			}
			expired := old.DeepCopy()
			expired.CreationTimestamp = metav1.NewTime(time.Now().Add(-time.Minute))
			if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), expired, cfg.ControlNamespace); err != nil {
				t.Fatal(err)
			}
			nextID := "b"
			if sameOperation {
				nextID = "a"
			}
			next, err := installer.acquireOperateLock(ctx, cfg, nextID, metav1.OwnerReference{Kind: "Job", Name: OperateJobName(nextID)})
			if err != nil {
				t.Fatal(err)
			}
			if err := installer.bindOperateLock(ctx, cfg, old, "Job", metav1.ObjectMeta{Name: OperateJobName("a"), UID: "delayed"}); !apierrors.IsConflict(err) {
				t.Fatalf("stale bind=%v", err)
			}
			if err := installer.releaseOperateReservation(ctx, cfg, old); err != nil {
				t.Fatal(err)
			}
			got, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
			if err != nil || got.UID != next.UID {
				t.Fatalf("old cleanup removed replacement: %v %v", got, err)
			}
		})
	}
}

func TestSyncClaimBindsPodAndCancellationCleansBoth(t *testing.T) {
	cfg := testConfig()
	client := claimAPI()
	installer := Installer{Client: client, PollInterval: time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		pod := action.(ktesting.UpdateAction).GetObject().(*corev1.Pod)
		if len(pod.Spec.SchedulingGates) == 0 {
			raw, err := client.Tracker().Get(corev1.SchemeGroupVersion.WithResource("configmaps"), cfg.ControlNamespace, OperateLockConfigMap)
			if err != nil {
				t.Error(err)
			} else {
				cm := raw.(*corev1.ConfigMap)
				if cm.Data["owner_kind"] != "Pod" || cm.Data["owner_uid"] != string(pod.UID) || len(cm.OwnerReferences) != 1 || cm.OwnerReferences[0].UID != pod.UID {
					t.Error("sync scheduled before durable ownership")
				}
			}
			cancel()
		}
		return false, nil, nil
	})
	err := installer.WithSyncPod(ctx, SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}, func(context.Context, Config) error { t.Error("cancelled sync ran callback"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), RepositorySyncPodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancel left Pod: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancel left claim: %v", err)
	}
}

func TestRecoveryCannotDeleteConcurrentOwnerBinding(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	expired := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace, UID: "old", ResourceVersion: "1", CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Minute))}, Data: map[string]string{"operation_id": "old"}}
	client := claimAPI(expired)
	bound := false
	client.PrependReactor("delete", "configmaps", func(action ktesting.Action) (bool, kruntime.Object, error) {
		if bound {
			return false, nil, nil
		}
		bound = true
		// The original launcher binds after recovery reads the reservation but before
		// its DELETE arrives. Kubernetes must reject that DELETE's stale version.
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: OperateJobName("old"), Namespace: cfg.ControlNamespace, UID: "live"}}
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, cfg.ControlNamespace); err != nil {
			t.Fatal(err)
		}
		changed := expired.DeepCopy()
		changed.ResourceVersion = "2"
		changed.Data["owner_uid"] = "live"
		if err := client.Tracker().Update(corev1.SchemeGroupVersion.WithResource("configmaps"), changed, cfg.ControlNamespace); err != nil {
			t.Fatal(err)
		}
		return false, nil, nil
	})
	if _, err := (Installer{Client: client}).acquireOperateLock(ctx, cfg, "next", metav1.OwnerReference{Kind: "Job", Name: OperateJobName("next")}); err == nil {
		t.Fatal("recovery stole a concurrently bound live owner's claim")
	}
	got, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
	if err != nil || got.Data["owner_uid"] != "live" {
		t.Fatalf("bound claim lost: %v %v", got, err)
	}
}

func TestStopRetiresUIDBoundClaimsWithOwners(t *testing.T) {
	for _, kind := range []string{"Job", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			cfg := testConfig()
			ctx := context.Background()
			owner := "sync-new"
			name := RepositorySyncPodName
			if kind == "Job" {
				owner = "operate-new"
				name = OperateJobName(owner)
			}
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace, UID: "claim", ResourceVersion: "1", CreationTimestamp: metav1.Now()}, Data: map[string]string{"operation_id": owner, "owner_kind": kind, "owner_name": name, "owner_uid": "owner"}}
			client := claimAPI(cm)
			if kind == "Pod" {
				_, _ = client.CoreV1().Pods(cfg.ControlNamespace).Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{})
			} else {
				_, _ = client.BatchV1().Jobs(cfg.ControlNamespace).Create(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{ServiceAccountName: "sdo-operate"}}}}, metav1.CreateOptions{})
			}
			if _, err := Stop(ctx, client, cfg.ControlNamespace); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(ctx, OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				t.Fatalf("claim survived owner retirement: %v", err)
			}
		})
	}
}
