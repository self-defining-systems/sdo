package program_test

import (
	"context"
	"strings"
	"testing"
	"time"


	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// A derived member cache is stale against its authoritative source. Ported
// from the expressiveness fixtures: the playbook binds the cache Pod, confirms
// an accepted data check fires on it, and resolves the cache it flushes from
// the application map, so it names no component.
const staleMembers = `component: copy
engine: redis
target: {labels: {app: copy}, image: redis}
read: {pattern: "{id}:members", type: zset, key_type: number, first: 8}
min_records: 0
lookup:
  component: truth
  engine: mongodb
  target: {labels: {app: truth}, image: mongo}
  read: {database: graph, collection: members, key: owner_id, key_type: number}
assert:
  - equivalence: [{field: "members[]", source: "members[].user_id"}]
`

type derivedRefresh struct{}

func (derivedRefresh) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "derived-refresh", Params: []program.ParamSpec{{Name: "cache", Kind: "Pod", Required: true}}}
}

func (derivedRefresh) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	m, err := v.AppMap()
	if err != nil {
		return program.Verdict{}, err
	}
	cache, ok := m.ComponentOf(v, b.Objects["cache"])
	if !ok || cache.Data == nil || cache.Data.Role != program.DataRoleDerived {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	for _, verdict := range datacheck.Verdicts(v) {
		if verdict.Error != "" && verdict.Component == cache.Name {
			return program.Verdict{Result: program.Unknown}, nil
		}
		if verdict.Firing() && verdict.Component == cache.Name {
			return program.Verdict{Result: program.Match, CauseClass: "data-corrupt", Reach: program.ReachInScope,
				Evidence: []program.Evidence{{Resource: b.Objects["cache"], Fact: verdict.ID, Observed: verdict.Findings[0].Evidence}}}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func (derivedRefresh) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	m, err := v.AppMap()
	if err != nil {
		return program.Plan{}, err
	}
	cache, _ := m.ComponentOf(v, b.Objects["cache"])
	target, err := program.StoreTargetFor(v, cache.Name)
	if err != nil {
		return program.Plan{}, err
	}
	return program.Plan{Actions: []program.Action{program.InvalidateDerived{Target: target, Scope: program.DataScope{Selector: "*:members"}, Reason: "rebuild stale copies from the source"}}}, nil
}

func staleView(t *testing.T, cacheStdout string) sdktest.Snapshot {
	t.Helper()
	d := datacheck.NewDetector(sdk.DetectorSpec{ID: "data-members"}, "members", staleMembers)
	reads := d.(podread.Consumer).PodReads()
	source := "Q\t1\nQ\t2\nR\t1\t{\"owner_id\":1,\"members\":[{\"user_id\":2},{\"user_id\":3}]}\nR\t2\t{\"owner_id\":2,\"members\":[{\"user_id\":1}]}\nE\n"
	answers := []struct{ pod, stdout string }{{"copy-1-a", cacheStdout}, {"truth-0", source}}
	s := mapSnapshot()
	s.AcceptedDataChecks = map[string]string{"members": staleMembers}
	s.PodReads = map[string]podread.Result{}
	for i, q := range reads {
		s.PodReads[q.ID] = podread.Result{Query: q, Readings: []podread.Reading{{Namespace: "app", Pod: answers[i].pod, Container: "kv", ObservedAt: time.Unix(100, 0).UTC(), Stdout: answers[i].stdout}}}
	}
	return s
}

func TestDerivedRefreshResolvesTheCacheFromTheMap(t *testing.T) {
	stale := staleView(t, "R\t1\t{\"members\":[\"2\",\"999999\"],\"size\":2}\nR\t2\t{\"members\":[\"1\"],\"size\":1}\nK\t1\nK\t2\nE\n")
	fresh := staleView(t, "R\t1\t{\"members\":[\"2\",\"3\"],\"size\":2}\nR\t2\t{\"members\":[\"1\"],\"size\":1}\nK\t1\nK\t2\nE\n")
	b := program.Bindings{Objects: program.ObjectBindings{"cache": podRef("copy-1-a")}}
	got := program.RunDiagnose(context.Background(), derivedRefresh{}, stale.RuntimeReadView(), b, time.Second)
	if got.Result != program.Match || !strings.Contains(got.Evidence[0].Observed, "differs") {
		t.Fatalf("stale verdict = %+v", got)
	}
	if got := program.RunDiagnose(context.Background(), derivedRefresh{}, fresh.RuntimeReadView(), b, time.Second); got.Result != program.NoMatch {
		t.Fatalf("fresh verdict = %+v", got)
	}
	plan, err := derivedRefresh{}.Mitigate(context.Background(), stale.RuntimeReadView(), b)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}
	if target := plan.Actions[0].(program.InvalidateDerived).Target; target.Component != "copy" || target.Container != "kv" || target.Pod != podRef("copy-1-a") {
		t.Fatalf("target = %+v", target)
	}
}
