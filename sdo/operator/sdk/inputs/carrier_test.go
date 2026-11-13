package inputs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate/carried"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/logs"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/usage"
	"sdo.dev/operator/sdk/vantage"
)

// Inventory SDK source interfaces from disk, not a hand-maintained count. Adding
// an optional source requires registering it in Sources and this contract table.
var contentSet = regexp.MustCompile(`^[A-Z][a-z]+Set$`)

func TestSDKInputInterfaceInventory(t *testing.T) {
	registered := map[string]reflect.Type{
		"sdk.ObservationClock":             reflect.TypeFor[sdk.ObservationClock](),
		"traffic.Source":                   reflect.TypeFor[traffic.Source](),
		"traffic.WindowSet":                reflect.TypeFor[traffic.WindowSet](),
		"metrics.ResultSet":                reflect.TypeFor[metrics.ResultSet](),
		"servicehealth.ReachabilitySource": reflect.TypeFor[servicehealth.ReachabilitySource](),
		"metrics.Source":                   reflect.TypeFor[metrics.Source](),
		"metrics.AlertSource":              reflect.TypeFor[metrics.AlertSource](),
		"usage.Source":                     reflect.TypeFor[usage.Source](),
		"logs.Source":                      reflect.TypeFor[logs.Source](),
		"podread.Source":                   reflect.TypeFor[podread.Source](),
		"podread.ResultSet":                reflect.TypeFor[podread.ResultSet](),
		"metrics.QuerySource":              reflect.TypeFor[metrics.QuerySource](),
		"metrics.UsageSource":              reflect.TypeFor[metrics.UsageSource](),
		"sdk.ObjectCaptureSource":          reflect.TypeFor[sdk.ObjectCaptureSource](),
		"sdk.ScopedObservationSource":      reflect.TypeFor[sdk.ScopedObservationSource](),
		"vantage.Source":                   reflect.TypeFor[vantage.Source](),
		"vantage.ResultSet":                reflect.TypeFor[vantage.ResultSet](),
		"carried.Source":                   reflect.TypeFor[carried.Source](),
		"carried.ResultSet":                reflect.TypeFor[carried.ResultSet](),
	}
	seen := map[string]bool{}
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range f.Decls {
			g, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range g.Specs {
				s, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if _, ok := s.Type.(*ast.InterfaceType); !ok {
					continue
				}
				name := s.Name.Name
				// Content sets (traffic.WindowSet, metrics.ResultSet) list a
				// source's whole content; snapshot capture copies them.
				if name != "ObservationClock" && !strings.HasSuffix(name, "Source") && !contentSet.MatchString(name) {
					continue
				}
				key := f.Name.Name + "." + name
				typ, ok := registered[key]
				if !ok {
					t.Errorf("SDK input interface %s is not registered in the carrier contract", key)
					continue
				}
				seen[key] = true
				if !reflect.TypeFor[inputs.Carrier]().Implements(typ) || !reflect.TypeFor[inputs.Sources]().Implements(typ) {
					t.Errorf("carrier contract missing %s", key)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key := range registered {
		if !seen[key] {
			t.Errorf("stale input registration %s", key)
		}
	}
}

func TestCarrierUpdatesAreCopyOnWrite(t *testing.T) {
	source := sdktest.Snapshot{ObservedAtTime: ptrTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)), Traffic: map[string]traffic.Window{"q": {LoadError: "traffic"}}, Reachability: &servicehealth.ReachabilityWindow{LoadError: "reachability"}, Metrics: map[string]metrics.Result{"q": {Error: "metrics"}}, Alerts: &metrics.AlertsResult{Error: "alerts"}, Usage: &usage.Result{Error: "usage"}, Logs: map[string]logs.Result{"q": {Error: "logs"}}, PodReads: map[string]podread.Result{"q": {Error: "pod reads"}}, AppState: map[string]carried.Result{"q": {Reader: "q"}}}
	empty := inputs.New(struct{ sdk.DetectionContext }{})
	full := inputs.New(source)
	methods := []string{"Clock", "Traffic", "Reachability", "Metrics", "Alerts", "Usage", "Logs", "PodReads", "AppState"}
	accessors := []string{"ObservedAt", "TrafficWindow", "ServiceReachability", "MetricResult", "AlertsResult", "UsageResult", "LogResult", "PodReadResult", "AppStateResult"}
	read := func(c inputs.Carrier, i int) []reflect.Value {
		var args []reflect.Value
		if i == 1 || i == 3 || i == 6 || i == 7 || i == 8 {
			args = []reflect.Value{reflect.ValueOf("q")}
		}
		return reflect.ValueOf(c).MethodByName(accessors[i]).Call(args)
	}
	same := func(a, b []reflect.Value) bool {
		for i := range a {
			if !reflect.DeepEqual(a[i].Interface(), b[i].Interface()) {
				return false
			}
		}
		return true
	}
	var visit func(inputs.Carrier, int)
	visit = func(c inputs.Carrier, mask int) {
		for i := range methods {
			want := empty
			if mask&(1<<i) != 0 {
				want = full
			}
			if !same(read(c, i), read(want, i)) {
				t.Fatalf("mask %d lost %s", mask, methods[i])
			}
		}
		for i, name := range methods {
			if mask&(1<<i) != 0 {
				continue
			}
			before := read(c, i)
			next := reflect.ValueOf(c).MethodByName("With" + name).Call([]reflect.Value{reflect.ValueOf(source)})[0].Interface().(inputs.Carrier)
			if !same(read(c, i), before) {
				t.Fatalf("With%s mutated its predecessor", name)
			}
			visit(next, mask|1<<i)
		}
	}
	visit(empty, 0)
}

// The permutation walk above doubles per input (and runs about 45 s at nine),
// so app state, added later, is checked on its own.
func TestCarrierAppStateUpdateIsCopyOnWrite(t *testing.T) {
	source := sdktest.Snapshot{AppState: map[string]carried.Result{"q": {Reader: "q"}}}
	empty := inputs.New(struct{ sdk.DetectionContext }{})
	next := empty.WithAppState(source)
	if _, ok := empty.AppStateResult("q"); ok {
		t.Fatal("WithAppState mutated its predecessor")
	}
	if got, ok := next.AppStateResult("q"); !ok || got.Reader != "q" {
		t.Fatalf("lost app state: %+v", got)
	}
	if len(inputs.New(source).AppStateResults()) != 1 {
		t.Fatal("results not listed")
	}
}
func ptrTime(at time.Time) *time.Time { return &at }
