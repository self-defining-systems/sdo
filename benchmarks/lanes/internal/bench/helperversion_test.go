//go:build integration

package bench

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/self-defining-systems/sdo/benchmarks/lanes/deploy"
)

// A reserved host's helper is installed by hand, and a stale one (older than the lanes binary) broke every step of a host
// without any check saying so. Host status compares the installed helper with the one embedded in the binary.

func embeddedHelperSum(t *testing.T) string {
	t.Helper()
	b, err := deploy.Files.ReadFile("host/lane-reserve")
	must(t, err)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func reservedHost() Inventory {
	return Inventory{Hosts: map[string]Host{"h": {Clusters: []string{"c1"}, Reserve: &HostReserve{CPUs: "2-3"}}}}
}

func TestStaleHelperIsReportedAsOutOfDate(t *testing.T) {
	inv := reservedHost()
	stale := func(ctx context.Context, host, script string) (string, error) {
		if !strings.Contains(script, HelperPath) {
			t.Errorf("the check must read the installed helper at %s: %s", HelperPath, script)
		}
		return strings.Repeat("0", 64) + "\n", nil
	}
	if got := inv.HelperProblem(context.Background(), "h", stale); got != "helper out of date: reinstall with lanes host install-helper" {
		t.Fatalf("problem = %q", got)
	}
}

func TestCurrentHelperAndUnreservedHostsReportNothing(t *testing.T) {
	inv := reservedHost()
	current := func(ctx context.Context, host, script string) (string, error) { return embeddedHelperSum(t) + "\n", nil }
	if got := inv.HelperProblem(context.Background(), "h", current); got != "" {
		t.Fatalf("an installed helper equal to the embedded one is current, got %q", got)
	}
	plain := Inventory{Hosts: map[string]Host{"h": {Clusters: []string{"c1"}}}}
	never := func(ctx context.Context, host, script string) (string, error) {
		t.Error("a host without reserve: has no helper to check")
		return "", nil
	}
	if got := plain.HelperProblem(context.Background(), "h", never); got != "" {
		t.Fatalf("got %q", got)
	}
}

// A helper that is not installed at all on a reserved host is also out of date; a host that cannot be asked says nothing.
func TestMissingHelperIsOutOfDateAndUnaskableHostSaysNothing(t *testing.T) {
	inv := reservedHost()
	missing := func(ctx context.Context, host, script string) (string, error) { return "missing\n", nil }
	if got := inv.HelperProblem(context.Background(), "h", missing); !strings.Contains(got, "helper out of date") {
		t.Fatalf("got %q", got)
	}
	down := func(ctx context.Context, host, script string) (string, error) { return "", context.DeadlineExceeded }
	if got := inv.HelperProblem(context.Background(), "h", down); got != "" {
		t.Fatalf("got %q", got)
	}
}
