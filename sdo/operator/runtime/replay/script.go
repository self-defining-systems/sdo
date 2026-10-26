package replay

import (
	"context"
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"time"
)

func scriptAction(action program.Action) (program.RunScript, bool) {
	switch a := action.(type) {
	case program.RunScript:
		return a, true
	case *program.RunScript:
		if a != nil {
			return *a, true
		}
	}
	return program.RunScript{}, false
}
func containsScript(plan program.Plan) bool {
	for _, a := range plan.Actions {
		if _, ok := scriptAction(a); ok {
			return true
		}
	}
	return false
}

// observedScriptTransition tests recorded predicates, never supplies fixed.json
// to an action simulator. This evidence basis is explicitly weaker than causal
// typed-effect simulation and is not independently verified live repair credit.
func observedScriptTransition(ctx context.Context, p program.Diagnoser, c Case, plan program.Plan, source *source, r Result) Result {
	fail := func(err error) Result {
		r.Errors = append(r.Errors, "program.mitigation-not-repaired: "+err.Error())
		return r
	}
	if err := program.ValidateBindings(p.Spec(), c.Corpus.Bindings); err != nil {
		return fail(err)
	}
	objects, err := snapshotObjects(c.Corpus.Open)
	if err != nil {
		return fail(err)
	}
	in := program.ValueInputs{Spec: p.Spec(), Bindings: c.Corpus.Bindings, SourceCommit: "offline-accepted", Source: func(ref sdk.ObjectRef) (map[string]any, error) {
		o, err := source.AcceptedManifest(ctx, "offline-accepted", ref)
		if err != nil {
			return nil, err
		}
		return o.Object, nil
	}, Observed: func(ref sdk.ObjectRef) (map[string]any, error) {
		o := objects[ref]
		if o == nil {
			return nil, fmt.Errorf("observed script input unavailable")
		}
		return o.Object, nil
	}}
	for _, action := range plan.Actions {
		script, ok := scriptAction(action)
		if !ok {
			continue
		}
		receipt, err := sdktest.ScriptOrigin(script, c.Corpus)
		if err != nil {
			return fail(err)
		}
		for _, value := range script.Env {
			var resolved any
			var err error
			if value.Origin() == program.ObservedValue {
				// Protected captures intentionally omit write metadata. Validate
				// the recorded field without inventing a live resourceVersion.
				object := objects[value.Ref()]
				if object == nil {
					return fail(fmt.Errorf("observed script input object unavailable"))
				}
				resolved, err = program.FieldValue(object.Object, value.Path())
			} else {
				resolved, _, err = program.ResolveValue(value, in)
			}
			if err != nil {
				return fail(fmt.Errorf("script input: %w", err))
			}
			if _, err = program.ScriptEnvText(resolved); err != nil {
				return fail(err)
			}
		}
		r.Effects = append(r.Effects, "observed:origin-receipt:"+receipt.ID+":script:"+program.ScriptDigest(script.Script))
	}
	r.Post = program.RunDiagnose(ctx, p, newReadView(ctx, c.Corpus.Fixed, c.Corpus.PhaseRecordings("fixed"), source), c.Corpus.Bindings, 5*time.Second)
	if r.Post.Result != program.NoMatch {
		return fail(fmt.Errorf("recorded fixed diagnosis is %s, want NoMatch", r.Post.Result))
	}
	r.ReplayBasis = "observed-transition"
	r.Status = "replayed"
	return r
}
