package sdktest_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// rowsView answers every Read with fixed rows.
type rowsView struct {
	program.ReadView
	rows []program.Row
}

func (v rowsView) Read(program.Query) (program.Observation, error) {
	return program.Observation{Count: 5, Rows: v.rows}, nil
}

var recordedQuery = program.Query{Store: &program.StoreQuery{
	Target: program.StoreTarget{Component: "db", Engine: "mysql", Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "db-0"}, Container: "db"},
	Scope:  program.DataScope{Collection: "items", Selector: "price IS NULL"},
	Fields: []string{"id", "price"},
}}

func TestReadRecordsAndReplaysOnlyTheQueriedFields(t *testing.T) {
	recorder := &sdktest.RecordingReadView{ReadView: rowsView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView(), rows: []program.Row{{"id": int64(1), "price": nil}}}}
	live, err := recorder.Read(recordedQuery)
	if err != nil || live.Count != 5 || len(live.Rows) != 1 {
		t.Fatalf("live read = %+v, %v", live, err)
	}
	raw, err := json.Marshal(recorder.Calls)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "password") {
		t.Fatalf("a recording holds only what the query named: %s", raw)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	replayed, err := sdktest.Snapshot{NamespaceName: "app", Readings: calls}.Read(recordedQuery)
	if err != nil {
		t.Fatal(err)
	}
	if n, ok := replayed.Rows[0].Int("id"); !ok || n != 1 || !replayed.Rows[0].IsNull("price") || replayed.Count != 5 {
		t.Fatalf("replay differs from the live answer: %+v", replayed)
	}
}

func TestReadWithNoRecordedAnswerIsNoRecordedCase(t *testing.T) {
	_, err := sdktest.Snapshot{NamespaceName: "app"}.Read(recordedQuery)
	var unrecorded *sdktest.UnrecordedError
	if !errors.As(err, &unrecorded) {
		t.Fatalf("an unrecorded query = %v, want UnrecordedError", err)
	}
}

func TestReadRecordingRefusesAnUnboundedAnswer(t *testing.T) {
	rows := make([]program.Row, 33)
	recorder := &sdktest.RecordingReadView{ReadView: rowsView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView(), rows: rows}}
	if _, err := recorder.Read(recordedQuery); err == nil {
		t.Fatal("33 rows must not be recorded")
	}
}

func TestReadRecordingCarriesNoCredentialQuery(t *testing.T) {
	recorder := &sdktest.RecordingReadView{ReadView: rowsView{ReadView: sdktest.Snapshot{NamespaceName: "app"}.RuntimeReadView()}}
	bad := program.Query{Store: &program.StoreQuery{Target: recordedQuery.Store.Target, Scope: recordedQuery.Store.Scope, Fields: []string{"password_hash"}}}
	if _, err := recorder.Read(bad); err == nil {
		t.Fatal("a credential field must be refused before anything is read or recorded")
	}
	if len(recorder.Calls) != 0 {
		t.Fatalf("a refused query was recorded: %+v", recorder.Calls)
	}
}
