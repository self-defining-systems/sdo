//go:build integration

package bench

import (
	"encoding/json"
	"strings"
	"testing"
)

// The workload revision a lane was prepared with is stored as workload_rev. A lane or step file written before the
// rename holds it as fuzzops_rev: it is read as the same revision (the migration), and what is written still carries the
// old key, so a manager rolled back to the previous build keeps reading it.

func TestPreparedReadsTheRevisionOfAnOldLaneFile(t *testing.T) {
	var p Prepared
	must(t, json.Unmarshal([]byte(`{"experiment":"x","namespace":"app1","app_pack":"/p","workload":"./adapter","fuzzops_rev":"0b528ac"}`), &p))
	if p.WorkloadRev != "0b528ac" || p.Workload != "./adapter" || p.Experiment != "x" {
		t.Fatalf("old file read as %+v", p)
	}
	var both Prepared
	must(t, json.Unmarshal([]byte(`{"experiment":"x","namespace":"app1","app_pack":"/p","workload_rev":"new1234","fuzzops_rev":"old"}`), &both))
	if both.WorkloadRev != "new1234" {
		t.Fatalf("the new key must win, got %q", both.WorkloadRev)
	}
}

func TestPreparedWritesTheRevisionUnderBothKeys(t *testing.T) {
	b, err := json.Marshal(Prepared{Experiment: "x", Namespace: "app1", WorkloadRev: "0b528ac"})
	must(t, err)
	for _, want := range []string{`"workload_rev":"0b528ac"`, `"fuzzops_rev":"0b528ac"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("%s lacks %s", b, want)
		}
	}
	var back Prepared
	must(t, json.Unmarshal(b, &back))
	if back.WorkloadRev != "0b528ac" || back.Experiment != "x" {
		t.Fatalf("round trip %+v", back)
	}
}

func TestStepReadsAndWritesTheRevisionLikePrepared(t *testing.T) {
	var s Step
	must(t, json.Unmarshal([]byte(`{"kind":"run","fuzzops_rev":"0b528ac"}`), &s))
	if s.WorkloadRev != "0b528ac" {
		t.Fatalf("old step file read as %+v", s)
	}
	b, err := json.Marshal(Step{Kind: StepRun, WorkloadRev: "0b528ac"})
	must(t, err)
	if !strings.Contains(string(b), `"fuzzops_rev":"0b528ac"`) || !strings.Contains(string(b), `"workload_rev":"0b528ac"`) {
		t.Errorf("step written as %s", b)
	}
}
