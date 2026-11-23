//go:build integration

package bench

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Provider eks runs every step (build, run, release, kubectl, aws, the harness) on the manager's machine, so that
// machine's load is the load of the steps. The load gate applies to it per core: "overloaded" below is far above any
// core count, "calm" is zero.
const (
	overloaded = 1e6
	calmLoad   = 0
)

// eksLoadFlab is a manager on one local eks host with one lane (c1); eksExtra is added to the host's eks: block.
func eksLoadFlab(t *testing.T, eksExtra string) *flab {
	t.Helper()
	l := newFlab(t, "h1=c1")
	l.extra["h1"] = `, provider: eks, eks: {region: us-west-2, account: "123456789012", budget_per_hour: 50, vcpu_quota: 500, max_lanes: 1` + eksExtra + `}`
	l.writeInventory()
	dir := t.TempDir()
	fakeTools(t, dir, "aws", "eksctl") // the eks platform tools, as stubs
	t.Setenv("PATH", os.Getenv("PATH")+string(os.PathListSeparator)+dir)
	l.m.EksCheck = func(context.Context, string, string) error { return nil }
	return l
}

func startedKinds(l *flab) []string {
	l.w.mu.Lock()
	defer l.w.mu.Unlock()
	var out []string
	for _, s := range l.w.started {
		out = append(out, s.Kind)
	}
	return out
}

// A machine over the limit takes no new build: the lane says why (the load per core and the limit), and the build
// starts on the first pass after the machine calms down.
func TestEksStartsNoBuildWhileTheManagersMachineIsOverloaded(t *testing.T) {
	l := eksLoadFlab(t, "")
	l.add("e1", []string{"h1"}, "a@x", 1)
	l.w.load["h1"] = overloaded
	for i := 0; i < 3; i++ {
		l.pass()
	}
	if got := startedKinds(l); len(got) != 0 {
		t.Fatalf("steps started on an overloaded manager machine: %v\n%s", got, readString(l.root.path("bench.log")))
	}
	if why := l.lane("c1").Idle; !strings.Contains(why, "the manager's machine has load5") || !strings.Contains(why, "/core over max_load5_per_core 4") {
		t.Errorf("the lane does not say why it waits: %q", why)
	}
	l.w.load["h1"] = calmLoad
	l.pass()
	if l.w.last("c1", StepBuild) == nil {
		t.Fatalf("no build after the machine calmed down: %s", readString(l.root.path("bench.log")))
	}
}

// The gate stops a lane's next run, not its release or recover: those undo or finish what is already there.
func TestEksStartsNoRunWhileTheManagersMachineIsOverloadedButStillReleases(t *testing.T) {
	l := eksLoadFlab(t, "")
	l.add("e1", []string{"h1"}, "a@x", 1)
	l.runningStep("c1", StepBuild)
	l.buildOK("c1")
	l.w.load["h1"] = overloaded
	for i := 0; i < 3; i++ {
		l.pass()
	}
	if l.w.last("c1", StepRun) != nil {
		t.Fatalf("a run started on an overloaded manager machine\n%s", readString(l.root.path("bench.log")))
	}
	lf := l.m.lanes["c1"]
	for _, kind := range []string{StepRelease, StepRecover} {
		if lf.Step != nil {
			t.Fatalf("lane busy: %+v", lf.Step)
		}
		if !l.m.start(context.Background(), lf, Step{Kind: kind}) {
			t.Fatalf("a %s step was refused on an overloaded manager machine", kind)
		}
		lf.Step = nil
	}
	l.w.load["h1"] = calmLoad
	l.runningStep("c1", StepRun)
}

// max_load5_per_core: 0 turns the gate off.
func TestEksLoadGateCanBeSwitchedOff(t *testing.T) {
	l := eksLoadFlab(t, ", max_load5_per_core: 0")
	l.add("e1", []string{"h1"}, "a@x", 1)
	l.w.load["h1"] = overloaded
	l.runningStep("c1", StepBuild)
}

const timeoutReason = "SREGym fault description failed (timeout)"

// A run that fails with a timeout while the machine was over the limit is not the item's failure: no run is recorded
// (so the item stays queued with its attempts and retries intact) and the lane's cluster is made anew. The same
// failure on a calm machine is the item's.
func TestEksTimeoutOnAnOverloadedMachineIsNotCountedAsAFailureOfItsItem(t *testing.T) {
	for _, tc := range []struct {
		name    string
		load    float64
		counted bool
	}{{"overloaded", overloaded, false}, {"calm", calmLoad, true}} {
		t.Run(tc.name, func(t *testing.T) {
			l := eksLoadFlab(t, "")
			l.add("e1", []string{"h1"}, "a@x", 1)
			run := l.toRun("c1")
			l.w.load["h1"] = tc.load
			l.w.finish(run.ID, Result{OK: false, Status: StatusInvalid, Reason: timeoutReason})
			l.pass()
			runs := l.exp("e1").Runs
			if tc.counted {
				if r := runs[RunID("a@x", 1, 1, 1)]; r.Status != StatusInvalid {
					t.Fatalf("a timeout on a calm machine must count: %+v", runs)
				}
				return
			}
			if len(runs) != 0 {
				t.Fatalf("a timeout on an overloaded machine was recorded as the item's run: %+v", runs)
			}
			if p := l.exp("e1").Progress(Seq{"a@x", 1}); p.Ended() || p.Attempt != 1 {
				t.Fatalf("the item did not stay queued: %+v", p)
			}
			if !strings.Contains(readString(l.root.path("bench.log")), "not counted as a failure of its item") {
				t.Errorf("the log does not say the failure was not counted\n%s", readString(l.root.path("bench.log")))
			}
		})
	}
}

// Another failure on the overloaded machine (not a timeout) is still the item's.
func TestEksNonTimeoutFailureOnAnOverloadedMachineStillCounts(t *testing.T) {
	l := eksLoadFlab(t, "")
	l.add("e1", []string{"h1"}, "a@x", 1)
	run := l.toRun("c1")
	l.w.load["h1"] = overloaded
	l.w.finish(run.ID, Result{OK: false, Status: StatusInvalid, Reason: "the fault was injected and the app crashed"})
	l.pass()
	if r := l.exp("e1").Runs[RunID("a@x", 1, 1, 1)]; r.Status != StatusInvalid {
		t.Fatalf("a failure that is no timeout must count: %+v", l.exp("e1").Runs)
	}
}

// A build that times out while the machine was over the limit does not count toward the lane's build failures (three
// in a row take a lane down until a human looks).
func TestEksBuildTimeoutOnAnOverloadedMachineIsNotABuildFailureOfTheLane(t *testing.T) {
	for _, tc := range []struct {
		name string
		load float64
		want int
	}{{"overloaded", overloaded, 0}, {"calm", calmLoad, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			l := eksLoadFlab(t, "")
			l.add("e1", []string{"h1"}, "a@x", 1)
			build := l.runningStep("c1", StepBuild)
			l.w.load["h1"] = tc.load
			l.w.finish(build.ID, Result{OK: false, Reason: "go run install-existing: TimeoutExpired"})
			l.pass()
			if got := l.lane("c1").BuildFailures; got != tc.want {
				t.Fatalf("build failures = %d, want %d\n%s", got, tc.want, readString(l.root.path("bench.log")))
			}
		})
	}
}

// The default limit is DefaultEksMaxLoad5PerCore; a number sets it; 0 switches the gate off; a negative one is refused.
func TestEksManagerLoadLimitDefaultsAndCanBeSetOrSwitchedOff(t *testing.T) {
	for _, tc := range []struct {
		set  string
		want float64
	}{{"", DefaultEksMaxLoad5PerCore}, {", max_load5_per_core: 2.5", 2.5}, {", max_load5_per_core: 0", 0}} {
		inv, err := loadInv(t, strings.Replace(eksHost, "idle_timeout: 5m", "idle_timeout: 5m"+tc.set, 1))
		if err != nil {
			t.Fatalf("%q: %v", tc.set, err)
		}
		if got := inv.Hosts["e"].ManagerLoadLimit(); got != tc.want {
			t.Errorf("%q: limit %v, want %v", tc.set, got, tc.want)
		}
		if got, want := inv.Hosts["e"].LoadBudget(16), tc.want*16; got != want {
			t.Errorf("%q: budget on 16 cores %v, want %v", tc.set, got, want)
		}
	}
	if _, err := loadInv(t, strings.Replace(eksHost, "idle_timeout: 5m", "idle_timeout: 5m, max_load5_per_core: -1", 1)); err == nil || !strings.Contains(err.Error(), "max_load5_per_core") {
		t.Errorf("a negative limit: %v", err)
	}
}

// `lanes preflight` reports the manager machine's load, cores and free memory for an eks host and fails it over the limit,
// so a plan is not added to a machine that cannot run it.
func TestPreflightFailsAnEksHostWhoseManagerMachineIsOverloaded(t *testing.T) {
	inv, err := loadInv(t, eksHost+"checkouts:\n  fuzzops: {abc: {e: /fz}}\n")
	must(t, err)
	spec := Spec{Name: "x", Fuzzops: "abc", Hosts: []string{"e"}}
	for _, tc := range []struct {
		load   string
		wantOK bool
		detail string
	}{{"36", true, "load5 36.0 on 16 cores = 2.25/core"}, {"9000", false, "over max_load5_per_core 4"}} {
		run := func(_ context.Context, _, script string) (string, error) {
			if strings.Contains(script, "memavail_kb") {
				return "load5 " + tc.load + "\ncores 16\nmemavail_kb 33554432\ndisk_staging_kb 1073741824\n", nil
			}
			return "", nil
		}
		var got *PreflightResult
		for _, r := range runPreflight(context.Background(), inv, []specFile{{Spec: spec}}, PreflightOptions{}, run) {
			if r.Check == "manager-machine" {
				r := r
				got = &r
			}
		}
		if got == nil {
			t.Fatalf("load %s: no manager-machine check", tc.load)
		}
		if got.OK != tc.wantOK || !strings.Contains(got.Detail, tc.detail) || !strings.Contains(got.Detail, "MemAvailable 32 GB") {
			t.Errorf("load %s: %v %q", tc.load, got.OK, got.Detail)
		}
	}
}
