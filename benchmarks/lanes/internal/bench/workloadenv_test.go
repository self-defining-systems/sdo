package bench

import (
	"strings"
	"testing"
)

func TestJudgeWorkloadEnvKeys(t *testing.T) {
	h := Host{WorkloadEnv: map[string]map[string]string{"sregym": {"A": "1"}}}

	// The key is the spec's workload string, not a name the author picked: a typo'd key matches no spec.
	ok, d := judgeWorkloadEnv(h, []string{"benchmarks/sregym/lanes/run.sh workload"})
	if ok || !strings.Contains(d, "sregym") || !strings.Contains(d, "benchmarks/sregym/lanes/run.sh workload") {
		t.Fatalf("a workload with no key beside an unused key must fail and name both: %v %s", ok, d)
	}
	// A host that serves another workload from its plain env is fine when every key is used.
	h.WorkloadEnv["benchmarks/sregym/lanes/run.sh workload"] = map[string]string{"A": "1"}
	if ok, d := judgeWorkloadEnv(h, []string{"benchmarks/sregym/lanes/run.sh workload"}); !ok {
		t.Fatalf("a stale key beside a workload that has its own key is harmless: %s", d)
	}
	delete(h.WorkloadEnv, "sregym")
	if ok, d := judgeWorkloadEnv(h, []string{"benchmarks/sregym/lanes/run.sh workload", ""}); !ok {
		t.Fatalf("all keys used, the default workload on the plain env: %s", d)
	}
	// No workload_env at all behaves as before.
	if ok, _ := judgeWorkloadEnv(Host{}, []string{"x"}); !ok {
		t.Fatal("a host with no workload_env has nothing to check")
	}
}
