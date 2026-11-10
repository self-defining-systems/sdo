package runtime

import (
	"context"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

const syntheticMongoRead = "== server\nrole\tstandalone\nconn\t4\t100\nset\tauthorization\tdisabled\nset\treplSetName\t-\nset\tstorageReadOnly\tfalse\n== params\n== locks\nfsync\t0\nwaiting\t0\n== users\n== catalog\ndb\tapp\t0\ncoll\tapp\titems\t3\tstrict/error/0a0b0c0d\n"

// Promoting a store's healthy reading captures its restore text beside the
// baseline; a store engine without restore text runs nothing.
func TestBaselinePromotionCapturesRestoreText(t *testing.T) {
	var ran []string
	payloads := &StorePayloads{Run: func(_ context.Context, uid string, command program.Command, stdout io.Writer) (int32, error) {
		ran = append(ran, uid+"/"+command.Container)
		_, _ = io.WriteString(stdout, "== payload ejson\nvalidator\tapp\titems\tstrict/error/0a0b0c0d\t{\"$jsonSchema\":{}}\n== end\n")
		return 0, nil
	}}
	baseline := NewPodReadBaseline(0)
	baseline.Payloads = payloads
	mongo := storestate.QueryID(storestate.EngineMongoDB, "docs")
	redis := storestate.QueryID(storestate.EngineRedis, "cache")
	baseline.Promote(map[string]podread.Result{
		mongo: {Query: podread.Query{ID: mongo}, Readings: []podread.Reading{{Namespace: "app", Pod: "docs-0", UID: "uid-docs", Container: "mongo", Stdout: syntheticMongoRead}}},
		redis: {Query: podread.Query{ID: redis}, Readings: []podread.Reading{{Namespace: "app", Pod: "cache-0", UID: "uid-cache", Container: "redis", Stdout: healthyRedisRead}}},
	})
	payloads.wait()
	if strings.Join(ran, ",") != "uid-docs/mongo" {
		t.Fatalf("ran = %v", ran)
	}
	payload, ok := payloads.Payload("docs", "docs-1")
	if !ok {
		t.Fatal("no payload for the single-instance store")
	}
	if entry, ok := payload.Lookup(storestate.Key{Kind: storestate.KindValidator, Scope: "app", Name: "items"}); !ok || entry.Value != "strict/error/0a0b0c0d" {
		t.Fatalf("entry = %+v %v", entry, ok)
	}
}

// The check Pod copies the client's credential references, never values, and
// runs the store's image on the store's node against the store Pod IP.
func TestCredentialCheckPodCopiesReferences(t *testing.T) {
	store := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "db-0", Namespace: "app"}, Spec: corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "pg", Image: "postgres:17"}}}, Status: corev1.PodStatus{PodIP: "10.0.0.2"}}
	ref := &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "db-secret"}, Key: "password"}}
	client := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app"}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "web:1",
		Env: []corev1.EnvVar{{Name: "DB_USER", Value: "app"}, {Name: "DB_PASSWORD", ValueFrom: ref}}}}}}
	pod, err := credentialCheckPod(store, client, StoreCredentialProbe{Namespace: "app", StorePod: "db-0", StoreContainer: "pg", ClientPod: "web-1", ClientContainer: "web", Engine: storestate.EnginePostgres, Port: 5432})
	if err != nil {
		t.Fatal(err)
	}
	c := pod.Spec.Containers[0]
	if c.Image != "postgres:17" || pod.Spec.NodeName != "n1" || len(c.Env) != 2 || c.Env[1].ValueFrom != ref || *pod.Spec.AutomountServiceAccountToken || pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("pod = %+v", pod.Spec)
	}
	if args := strings.Join(c.Args, " "); !strings.Contains(args, "10.0.0.2 5432") {
		t.Fatalf("args = %s", args)
	}
}
