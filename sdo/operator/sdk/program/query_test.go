package program_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

var (
	queryService = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "status"}
	queryStore   = program.StoreTarget{Component: "db", Engine: "mysql", Pod: syntheticStore.Pod, Container: "db"}
)

func TestQueryValidates(t *testing.T) {
	store := program.StoreQuery{Target: queryStore, Scope: program.DataScope{Collection: "items", Selector: "price IS NULL"}, Fields: []string{"id", "price"}}
	probe := program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"state": "/0/Status", "count": "/n"}}}
	for name, test := range map[string]struct {
		query program.Query
		ok    bool
	}{
		"store":                        {program.Query{Store: &store}, true},
		"probe":                        {program.Query{Probe: &probe}, true},
		"neither":                      {program.Query{}, false},
		"both":                         {program.Query{Store: &store, Probe: &probe}, false},
		"store credential table":       {program.Query{Store: &program.StoreQuery{Target: queryStore, Scope: program.DataScope{Database: "mysql", Collection: "user"}, Fields: []string{"User"}}}, false},
		"store credential field":       {program.Query{Store: &program.StoreQuery{Target: queryStore, Scope: store.Scope, Fields: []string{"password_hash"}}}, false},
		"store function selector":      {program.Query{Store: &program.StoreQuery{Target: queryStore, Scope: program.DataScope{Collection: "items", Selector: "SLEEP(5) = 0"}, Fields: []string{"id"}}}, false},
		"store without fields":         {program.Query{Store: &program.StoreQuery{Target: queryStore, Scope: store.Scope}}, false},
		"store on redis":               {program.Query{Store: &program.StoreQuery{Target: redisStore, Fields: []string{"key"}}}, false},
		"probe without fields":         {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status"}}}, false},
		"probe secret-named pointer":   {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"t": "/auth/token"}}}}, false},
		"probe secret-named field":     {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"password": "/x"}}}}, false},
		"probe credential header":      {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status", Headers: map[string]string{"Authorization": "x"}, Fields: map[string]string{"a": "/a"}}}}, false},
		"probe credential query":       {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status?api_token=1", Fields: map[string]string{"a": "/a"}}}}, false},
		"probe write method":           {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "POST", Path: "/status", Fields: map[string]string{"a": "/a"}}}}, false},
		"probe bad pointer":            {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"a": "a.b"}}}}, false},
		"probe a Pod":                  {program.Query{Probe: &program.ProbeQuery{To: syntheticStore.Pod, Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"a": "/a"}}}}, false},
		"probe too many fields":        {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/s", Fields: manyFields(17)}}}, false},
		"probe the whole body pointer": {program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/s", Fields: map[string]string{"all": ""}}}}, false},
	} {
		t.Run(name, func(t *testing.T) {
			if err := test.query.Validate(); (err == nil) != test.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func manyFields(n int) map[string]string {
	fields := map[string]string{}
	for i := 0; i < n; i++ {
		fields["f"+strings.Repeat("a", i)] = "/x"
	}
	return fields
}

func TestObservationNormalizesToTypedScalars(t *testing.T) {
	got, err := program.Observation{Count: 2, Rows: []program.Row{{"id": float64(7), "ratio": 0.5, "name": "a", "ok": true, "gone": nil}}}.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	row := got.Rows[0]
	if row["id"] != int64(7) || row["ratio"] != 0.5 {
		t.Fatalf("a whole float from JSON must read back as int64: %#v", row)
	}
	if n, ok := row.Int("id"); !ok || n != 7 {
		t.Fatalf("Int(id) = %d, %v", n, ok)
	}
	if s, ok := row.String("name"); !ok || s != "a" {
		t.Fatalf("String(name) = %q, %v", s, ok)
	}
	if b, ok := row.Bool("ok"); !ok || !b {
		t.Fatalf("Bool(ok) = %v, %v", b, ok)
	}
	if _, ok := row.Int("gone"); ok {
		t.Fatal("a nil value is not an integer")
	}
	for name, bad := range map[string]program.Observation{
		"array":     {Rows: []program.Row{{"a": []any{1}}}},
		"object":    {Rows: []program.Row{{"a": map[string]any{}}}},
		"long":      {Rows: []program.Row{{"a": strings.Repeat("x", 257)}}},
		"too many":  {Rows: make([]program.Row, 33)},
		"many keys": {Rows: []program.Row{rowOf(17)}},
	} {
		if _, err := bad.Normalize(); err == nil {
			t.Fatalf("%s: Normalize accepted it", name)
		}
	}
}

func rowOf(n int) program.Row {
	row := program.Row{}
	for i := 0; i < n; i++ {
		row["f"+strings.Repeat("a", i)] = int64(1)
	}
	return row
}

// readsOK answers every Read with one row.
type readsOK struct {
	program.ReadView
	reads int
}

func (v *readsOK) Read(program.Query) (program.Observation, error) {
	v.reads++
	return program.Observation{Count: 1, Rows: []program.Row{{"n": int64(1)}}}, nil
}

type fourReads struct{ errs []error }

func (fourReads) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "four-reads"} }

func (d *fourReads) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	q := program.Query{Store: &program.StoreQuery{Target: queryStore, Scope: program.DataScope{Collection: "items"}, Fields: []string{"id"}}}
	for i := 0; i < 4; i++ {
		_, err := view.Read(q)
		d.errs = append(d.errs, err)
	}
	return program.Verdict{Result: program.NoMatch}, nil
}

func TestADiagnoseMakesAtMostThreeReads(t *testing.T) {
	inner := &readsOK{ReadView: mapSnapshot().RuntimeReadView()}
	d := &fourReads{}
	got := program.RunDiagnose(context.Background(), d, inner, program.Bindings{}, time.Second)
	if inner.reads != program.MaxDiagnoseReads {
		t.Fatalf("the view served %d reads, want %d", inner.reads, program.MaxDiagnoseReads)
	}
	for i, err := range d.errs[:program.MaxDiagnoseReads] {
		if err != nil {
			t.Fatalf("read %d failed: %v", i+1, err)
		}
	}
	if err := d.errs[program.MaxDiagnoseReads]; !errors.Is(err, program.ErrDeadline) {
		t.Fatalf("read %d = %v, want ErrDeadline", program.MaxDiagnoseReads+1, err)
	}
	if got.Result != program.Unknown {
		t.Fatalf("a Diagnose whose read ran past the budget must be Unknown, got %s", got.Result)
	}
}

func TestReadIsUnavailableWithoutALiveSource(t *testing.T) {
	_, err := mapSnapshot().RuntimeReadView().Read(program.Query{Probe: &program.ProbeQuery{To: queryService, Req: program.HTTPProbe{Method: "GET", Path: "/s", Fields: map[string]string{"a": "/a"}}}})
	if err == nil {
		t.Fatal("a snapshot with no recorded read must not answer")
	}
}
