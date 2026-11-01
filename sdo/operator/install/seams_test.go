package install

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"strings"
	"testing"
	"time"
)

func TestOperateClaimAgeUsesInjectedClock(t *testing.T) {
	created := time.Unix(1000, 0)
	for _, test := range []struct {
		name    string
		age     time.Duration
		expired bool
	}{{"recent", 5 * time.Minute, false}, {"expired", 20 * time.Minute, true}} {
		t.Run(test.name, func(t *testing.T) {
			cfg := Config{Namespace: "demo", ControlNamespace: "demo-sdo"}
			client := fake.NewSimpleClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: "demo-sdo", CreationTimestamp: metav1.NewTime(created)}, Data: map[string]string{"operation_id": "previous"}})
			installer := Installer{Client: client, Now: func() time.Time { return created.Add(test.age) }}
			err := installer.acquireOperateLock(context.Background(), cfg, "next")
			if test.expired {
				if err != nil {
					t.Fatal(err)
				}
				claim, err := client.CoreV1().ConfigMaps("demo-sdo").Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{})
				if err != nil || claim.Data["operation_id"] != "next" {
					t.Fatalf("replacement claim: %+v %v", claim, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "previous") {
				t.Fatalf("recent claim was not preserved: %v", err)
			}
		})
	}
}

func TestInstallerPollUsesInjectedWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	installer := Installer{PollInterval: 7 * time.Second, Wait: func(got context.Context, d time.Duration) error {
		calls++
		if got != ctx || d != 7*time.Second {
			t.Fatalf("wait: %v %v", got, d)
		}
		return context.Canceled
	}}
	if err := installer.pause(ctx); err != context.Canceled || calls != 1 {
		t.Fatalf("pause: %v calls=%d", err, calls)
	}
}
