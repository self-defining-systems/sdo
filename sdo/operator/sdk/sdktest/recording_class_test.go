package sdktest_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type failingDiffView struct {
	program.ReadView
	err error
}

func (v failingDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{}, v.err
}

var classRef = sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: "app", Name: "cm-a"}

func recordThenReplay(t *testing.T, err error) error {
	t.Helper()
	recorder := &sdktest.RecordingReadView{ReadView: failingDiffView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView(), err: err}}
	_, _ = recorder.DiffAgainstSource(classRef)
	raw, marshalErr := json.Marshal(recorder.Calls)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var calls []sdktest.PrimitiveCall
	if marshalErr = json.Unmarshal(raw, &calls); marshalErr != nil {
		t.Fatal(marshalErr)
	}
	_, replayErr := sdktest.Snapshot{NamespaceName: "app", Readings: calls}.DiffAgainstSource(classRef)
	return replayErr
}

func TestRecordedReadErrorKeepsItsClassThroughJSONAndReplay(t *testing.T) {
	for _, c := range []struct {
		name  string
		err   error
		class string
	}{
		{"refused", fmt.Errorf("login: %w", sdk.ErrRefused), sdk.ReadClassRefused},
		{"invalid", fmt.Errorf("secret: %w", sdk.ErrInvalidRead), sdk.ReadClassInvalid},
		{"unavailable", fmt.Errorf("no client: %w", sdk.ErrUnavailable), sdk.ReadClassUnavailable},
		{"unclassified", errors.New("boom"), sdk.ReadClassUnavailable},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := recordThenReplay(t, c.err)
			if got == nil || got.Error() != c.err.Error() {
				t.Fatalf("replayed error = %v, want text %q", got, c.err)
			}
			if class := sdk.ReadErrorClass(got); class != c.class {
				t.Fatalf("replayed class = %q, want %q", class, c.class)
			}
		})
	}
}

func TestLegacyRecordingWithoutAClassIsUnavailable(t *testing.T) {
	args, _ := json.Marshal([]any{classRef})
	legacy := []sdktest.PrimitiveCall{{Method: "DiffAgainstSource", Arguments: args, Result: json.RawMessage(`{}`), Error: "object not captured: v1/ConfigMap app/cm-a"}}
	_, err := sdktest.Snapshot{NamespaceName: "app", Readings: legacy}.DiffAgainstSource(classRef)
	if got := sdk.ReadErrorClass(err); got != sdk.ReadClassUnavailable {
		t.Fatalf("legacy class = %q, want unavailable", got)
	}
}

func TestUnrecordedReadIsUnavailable(t *testing.T) {
	_, err := sdktest.Snapshot{NamespaceName: "app"}.DiffAgainstSource(classRef)
	var unrecorded *sdktest.UnrecordedError
	if !errors.As(err, &unrecorded) || !errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("unrecorded read = %v, want an unavailable *UnrecordedError", err)
	}
}

type refusedCatalog struct{ program.ReadView }

func (refusedCatalog) DatabaseUsers(program.DatabaseTarget) ([]program.DatabaseUser, error) {
	return nil, fmt.Errorf("catalog said no: %w", sdk.ErrRefused)
}

func TestDatabaseCatalogRefusalIsRecordedWithoutItsTextAndReplaysRefused(t *testing.T) {
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-a-0"}, Container: "store", Database: "admin"}
	recorder := &sdktest.RecordingReadView{ReadView: refusedCatalog{sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView()}}
	_, live := recorder.DatabaseUsers(target)
	if sdk.ReadErrorClass(live) != sdk.ReadClassRefused {
		t.Fatalf("live class = %q (%v)", sdk.ReadErrorClass(live), live)
	}
	raw, _ := json.Marshal(recorder.Calls)
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	if string(raw) == "" || json.Valid(raw) == false || contains(string(raw), "catalog said no") {
		t.Fatalf("a recording holds only the safe text: %s", raw)
	}
	_, replayed := sdktest.Snapshot{NamespaceName: "app", Readings: calls}.DatabaseUsers(target)
	if sdk.ReadErrorClass(replayed) != sdk.ReadClassRefused {
		t.Fatalf("replayed class = %q (%v)", sdk.ReadErrorClass(replayed), replayed)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
