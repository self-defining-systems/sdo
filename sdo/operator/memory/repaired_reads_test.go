package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type repairedReads struct {
	program.ReadView
}

func (repairedReads) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, nil
}

func (repairedReads) DatabaseUsers(program.DatabaseTarget) ([]program.DatabaseUser, error) {
	return []program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}}}, nil
}

// A plan's own fixed verification (one catalog read) must not stop the fixed
// boundary from re-reading every other observation of that open phase.
func TestRepairedReadsCoverEveryOpenObservation(t *testing.T) {
	root := t.TempDir()
	if err := CaptureIncidentSnapshot(root, "incident", "open@pre-apply", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	if err := CaptureIncidentSnapshot(root, "incident", "fixed", sdktest.Snapshot{NamespaceName: "app"}, "app"); err != nil {
		t.Fatal(err)
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db", Database: "app"}
	call := func(phase, method string, argument, result any) sdktest.PrimitiveCall {
		args, _ := json.Marshal([]any{argument})
		raw, _ := json.Marshal(result)
		return sdktest.PrimitiveCall{Phase: phase, Method: method, Arguments: args, Result: raw}
	}
	for _, c := range []sdktest.PrimitiveCall{
		call("", "DiffAgainstSource", ref, program.Diff{Changed: true}),
		call("", "DiffAgainstSource", ref, program.Diff{Changed: true}),
		call("", "DatabaseUsers", target, []program.DatabaseUser{}),
		call("fixed", "DatabaseUsers", target, []program.DatabaseUser{{Name: "app", Roles: []program.DatabaseRole{{Database: "app", Name: "readWrite"}}}}),
	} {
		if err := AppendCapturedRecording(root, "incident", "open@pre-apply", c); err != nil {
			t.Fatal(err)
		}
	}
	view := func(sdk.DetectionContext) program.ReadView { return repairedReads{} }
	count := func() map[string]int {
		dir, err := IncidentCorpusDir(root, "incident")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "open@pre-apply-recordings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			t.Fatal(err)
		}
		counts := map[string]int{}
		for _, c := range calls {
			counts[c.Phase+"/"+c.Method]++
		}
		return counts
	}
	for range 2 {
		if err := CaptureRepairedProgramReads(root, "incident", view); err != nil {
			t.Fatal(err)
		}
		if got := count(); got["fixed/DiffAgainstSource"] != 2 || got["fixed/DatabaseUsers"] != 1 {
			t.Fatalf("fixed recordings do not cover the open phase exactly once: %v", got)
		}
	}
}
