package sandbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"sdo.dev/operator/sandbox"
	"sdo.dev/operator/sandbox/sandboxtest"
)

func manager(t *testing.T, limit int) (sandbox.Manager, *sandboxtest.Provider) {
	t.Helper()
	fake := &sandboxtest.Provider{}
	return sandbox.Manager{
		Dir: t.TempDir(), Cap: limit, Provider: fake, BasePort: 40000,
		Now: func() time.Time { return time.Date(2026, 11, 14, 8, 0, 0, 0, time.UTC) },
	}, fake
}

func TestAddEnforcesTheCapAndReusesTheLowestFreeIndex(t *testing.T) {
	ctx := context.Background()
	m, fake := manager(t, 2)
	a, err := m.Add(ctx, "api")
	if err != nil || a.ID() != "sandbox-1" || a.Owner != "api" {
		t.Fatalf("first add = %+v, %v", a, err)
	}
	if _, err := m.Add(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Add(ctx, ""); !errors.Is(err, sandbox.ErrCap) {
		t.Fatalf("third add must hit the cap, got %v", err)
	}
	if len(fake.Cluster) != 2 {
		t.Fatalf("a refused add must create nothing, clusters = %v", fake.Cluster)
	}
	if err := m.Remove(ctx, 1); err != nil {
		t.Fatal(err)
	}
	again, err := m.Add(ctx, "")
	if err != nil || again.Index != 1 {
		t.Fatalf("the freed index is reused, got %+v, %v", again, err)
	}
}

func TestListReportsUsageURLsAndReadiness(t *testing.T) {
	ctx := context.Background()
	m, fake := manager(t, 3)
	info, err := m.Add(ctx, "x")
	if err != nil {
		t.Fatal(err)
	}
	if info.Prometheus != "http://127.0.0.1:40002" || info.Traces != "http://127.0.0.1:40003" {
		t.Fatalf("URLs = %s %s", info.Prometheus, info.Traces)
	}
	infos, _ := m.List(ctx)
	if len(infos) != 1 || !infos[0].Ready || sandbox.Usage(len(infos), m.Cap) != "sandboxes: 1/3" {
		t.Fatalf("list = %+v", infos)
	}
	delete(fake.Cluster, info.Name) // the cluster died outside SDO
	infos, _ = m.List(ctx)
	if infos[0].Ready {
		t.Fatal("a recorded sandbox whose cluster is gone is not ready")
	}
}

func TestAFailedCreateLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	m, fake := manager(t, 3)
	fake.FailCreate = errors.New("boom")
	if _, err := m.Add(ctx, ""); err == nil {
		t.Fatal("create failure must surface")
	}
	if len(fake.Cluster) != 0 {
		t.Fatalf("half-made cluster survived: %v", fake.Cluster)
	}
	infos, _ := m.List(ctx)
	if len(infos) != 0 {
		t.Fatalf("recorded = %v", infos)
	}
}

func TestDeleteAllSweepsUnrecordedClustersToo(t *testing.T) {
	ctx := context.Background()
	m, fake := manager(t, 3)
	if _, err := m.Add(ctx, ""); err != nil {
		t.Fatal(err)
	}
	fake.Cluster[sandbox.ClusterPrefix+"9"] = sandbox.Spec{Name: sandbox.ClusterPrefix + "9"} // a crashed host's leftover
	if err := m.DeleteAll(ctx); err != nil {
		t.Fatal(err)
	}
	if len(fake.Cluster) != 0 {
		t.Fatalf("clusters survived: %v", fake.Cluster)
	}
	if infos, _ := m.List(ctx); len(infos) != 0 {
		t.Fatalf("records survived: %v", infos)
	}
}

func TestParseID(t *testing.T) {
	if n, err := sandbox.ParseID("sandbox-3"); err != nil || n != 3 {
		t.Fatalf("%d %v", n, err)
	}
	for _, bad := range []string{"3", "target", "sandbox-", "sandbox-0", "sandbox-x"} {
		if _, err := sandbox.ParseID(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
}
