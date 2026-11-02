package mongoauth

import (
	"context"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

func TestRelationDerivedScriptAndObservedDiagnosis(t *testing.T) {
	c, err := sdktest.LoadCorpusCase("../../corpus/mongo-auth")
	if err != nil {
		t.Fatal(err)
	}
	p := Program{}
	ctx := context.Background()
	open := &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}
	if got := program.RunDiagnose(ctx, p, open, c.Bindings, time.Second); got.Result != program.Match {
		t.Fatalf("open: %+v", got)
	}
	fixed := &sdktest.ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}
	if got := program.RunDiagnose(ctx, p, fixed, c.Bindings, time.Second); got.Result != program.NoMatch {
		t.Fatalf("fixed: %+v", got)
	}
	plan, err := p.Mitigate(ctx, &sdktest.ReplayReadView{Snapshot: c.Open, Calls: c.PhaseRecordings("open")}, c.Bindings)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Actions) != 1 {
		t.Fatalf("expected one inline script, got %d", len(plan.Actions))
	}
	script, ok := plan.Actions[0].(program.RunScript)
	if !ok {
		t.Fatalf("repair is %T", plan.Actions[0])
	}
	if len(script.Env) != 5 {
		t.Fatalf("relation inputs missing: %+v", script.Env)
	}
	for name, value := range script.Env {
		if value.Origin() != program.ObservedValue {
			t.Fatalf("%s has no live relation provenance", name)
		}
	}
	if _, err := sdktest.ScriptOrigin(script, c); err != nil {
		t.Fatal(err)
	}
}

func TestMissingOrAmbiguousInitializationRelationRemainsUnknown(t *testing.T) {
	for _, mode := range []string{"missing", "ambiguous", "unrecorded catalog"} {
		t.Run(mode, func(t *testing.T) {
			c, err := sdktest.LoadCorpusCase("../../corpus/mongo-auth")
			if err != nil {
				t.Fatal(err)
			}
			calls := c.PhaseRecordings("open")
			switch mode {
			case "missing":
				c.Open.PodList[0].Spec.Volumes = nil
			case "ambiguous":
				c.Open.PodList[0].Spec.Volumes = append(c.Open.PodList[0].Spec.Volumes, c.Open.PodList[0].Spec.Volumes[0])
			case "unrecorded catalog":
				calls = nil
			}
			got := program.RunDiagnose(context.Background(), Program{}, &sdktest.ReplayReadView{Snapshot: c.Open, Calls: calls}, c.Bindings, time.Second)
			if got.Result != program.Unknown {
				t.Fatalf("unavailable relation became %s", got.Result)
			}
		})
	}
}

func TestFixedDiagnosisDoesNotStartScript(t *testing.T) {
	c, err := sdktest.LoadCorpusCase("../../corpus/mongo-auth")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := (Program{}).Mitigate(context.Background(), &sdktest.ReplayReadView{Snapshot: c.Fixed, Calls: c.PhaseRecordings("fixed")}, c.Bindings)
	if err != nil || len(plan.Actions) != 0 {
		t.Fatalf("fixed state proposed a script: %+v %v", plan, err)
	}
}
