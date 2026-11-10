package storestate_test

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

func holderPod(name, ip, ownerKind, ownerName, hash string) corev1.Pod {
	yes := true
	p := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, Labels: map[string]string{},
		OwnerReferences: []metav1.OwnerReference{{Kind: ownerKind, Name: ownerName, Controller: &yes}}},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "redis:7"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip}}
	if hash != "" {
		p.Labels["pod-template-hash"] = hash
	}
	return p
}

// The redis read groups its clients by address and state; at the connection
// cap, when the store refuses CLIENT LIST, the container's TCP table still
// names the peers.
func TestHoldersParseFromSessionsOrTCP(t *testing.T) {
	sessions := parseAs(t, storestate.EngineRedis, healthyRedis+"== holders\nholder\t10.0.0.7\tidle\t40\nholder\t127.0.0.1\tidle\t3\nholder\t10.0.0.7\tactive\t2\n")
	want := []storestate.Holder{{Peer: "10.0.0.7", State: "idle", Count: 40}, {Peer: "local", State: "idle", Count: 3}, {Peer: "10.0.0.7", State: "active", Count: 2}}
	if len(sessions.Holders) != 3 || sessions.Holders[0] != want[0] || sessions.Holders[1] != want[1] || sessions.Holders[2] != want[2] {
		t.Fatalf("holders = %+v", sessions.Holders)
	}
	tcp := parseAs(t, storestate.EngineRedis, healthyRedis+"== holders\ntcp\t0100007F\t9990\ntcp\t0000000000000000FFFF00000700000A\t10\n")
	if len(tcp.Holders) != 2 || tcp.Holders[0] != (storestate.Holder{Peer: "local", State: "connected", Count: 9990}) || tcp.Holders[1] != (storestate.Holder{Peer: "10.0.0.7", State: "connected", Count: 10}) {
		t.Fatalf("tcp holders = %+v", tcp.Holders)
	}
	if none := parseAs(t, storestate.EngineRedis, healthyRedis); none.Holders != nil {
		t.Fatalf("a read without holders claims %+v", none.Holders)
	}
	pg := parseAs(t, storestate.EnginePostgres, strings.Replace(healthyPostgres, "== catalog exampledb", "== holders\nholder\tlocal\tidle-in-transaction\t5\nholder\t10.0.0.9\tblocked\t1\n== catalog exampledb", 1))
	if len(pg.Holders) != 2 || pg.Holders[0].State != storestate.HolderIdleInTransaction {
		t.Fatalf("postgres holders = %+v", pg.Holders)
	}
	mongo := parseAs(t, storestate.EngineMongoDB, strings.Replace(healthyMongo, "== users", "== holders\nholder\t10.0.0.9\tactive\t4\n== users", 1))
	if len(mongo.Holders) != 1 || mongo.Holders[0].Count != 4 {
		t.Fatalf("mongodb holders = %+v", mongo.Holders)
	}
}

func TestResolveHoldersNamesPodsAndWorkloads(t *testing.T) {
	store := holderPod("cache-0", "10.0.0.2", "StatefulSet", "cache", "")
	client := holderPod("web-7d9-abcde", "10.0.0.7", "ReplicaSet", "web-7d9", "7d9")
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{store, client}, ReplicaSetList: []appsv1.ReplicaSet{}}
	groups := storestate.ResolveHolders(view, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "cache-0"}, []storestate.Holder{
		{Peer: "local", State: "idle", Count: 50}, {Peer: "10.0.0.7", State: "active", Count: 2}, {Peer: "10.9.9.9", State: "idle", Count: 1}})
	if groups[0].Workload != "StatefulSet/cache" || groups[1].Workload != "Deployment/web" || groups[1].Pod != "web-7d9-abcde" || groups[2].Workload != "" {
		t.Fatalf("groups = %+v", groups)
	}
	text := storestate.RenderHolders(groups)
	for _, w := range []string{"50 idle from local (inside cache-0)", "2 active from Deployment/web", "10.9.9.9 (no Pod of the namespace)"} {
		if !strings.Contains(text, w) {
			t.Errorf("render lacks %q: %s", w, text)
		}
	}
}

// A store refusing SDO's read at its connection cap is reported with who
// holds the connections.
func TestRefusalAtTheCapNamesTheHolders(t *testing.T) {
	d, err := storestate.NewDetector(storestate.Config{Spec: sdk.DetectorSpec{ID: "store-state-cache"}, Component: "cache", Engine: storestate.EngineRedis, Target: podread.Target{Workload: "StatefulSet/cache"}})
	if err != nil {
		t.Fatal(err)
	}
	refused := strings.NewReplacer("role:master", "ERR max number of clients reached", "== dbsize\n12", "== dbsize\nERR max number of clients reached",
		"== role\nmaster", "== role\nERR max number of clients reached").Replace(healthyRedis)
	refused = strings.Replace(refused, "== config\nmaxmemory", "== config\nERR max number of clients reached\nmaxmemory", 1)
	refused = strings.Replace(refused, "== acl\nuser default on nopass sanitize-payload ~* &* +@all", "== acl\nERR max number of clients reached", 1)
	refused += "== holders\ntcp\t0100007F\t9990\n"
	q := d.(podread.Consumer).PodReads()[0]
	store := holderPod("cache-0", "10.0.0.2", "StatefulSet", "cache", "")
	reading := func(out string) podread.Reading {
		return podread.Reading{Namespace: "app", Pod: "cache-0", UID: "u", Container: "c", Stdout: out}
	}
	view := sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{store},
		PodReads: map[string]podread.Result{q.ID: {Query: q, Readings: []podread.Reading{reading(refused)}, Baseline: []podread.Reading{reading(healthyRedis)}}}}
	findings, err := d.Detect(context.Background(), view)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range findings {
		if f.RuleID == storestate.RuleRefused && strings.Contains(f.Evidence, "9990 connected from local (inside cache-0)") {
			found = true
		}
	}
	if !found {
		t.Fatalf("findings = %+v", findings)
	}
	changes := storestate.Changes(view)
	if len(changes) != 1 || len(changes[0].Holders) != 1 || changes[0].Holders[0].Workload != "StatefulSet/cache" {
		t.Fatalf("changes = %+v", changes)
	}
}
