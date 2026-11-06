package telemetry_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetry/telemetrytest"
)

type fakeOptions struct {
	Tenant string `json:"tenant"`
}

func fakeAdapter(name string, caps ...telemetry.Capability) telemetry.Adapter[fakeOptions] {
	return telemetry.Adapter[fakeOptions]{
		Name:    name,
		Kinds:   []telemetry.Kind{telemetry.KindMetrics, telemetry.KindLogs},
		Dialect: "fake query language",
		Caps:    caps,
		Validate: func(o fakeOptions, ep telemetry.Endpoint) error {
			if o.Tenant == "forbidden" {
				return errString("tenant is forbidden")
			}
			return nil
		},
		Open: func(o fakeOptions, ep telemetry.Endpoint, env telemetry.Env) (telemetry.Client, error) {
			return telemetrytest.NewFake(telemetry.KindMetrics, telemetry.Result{}), nil
		},
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestRegisterRejectsMalformedAdapters(t *testing.T) {
	good := fakeAdapter("fake")
	mutations := map[string]func(*telemetry.Adapter[fakeOptions]){
		"bad name":       func(a *telemetry.Adapter[fakeOptions]) { a.Name = "Bad Name" },
		"no kinds":       func(a *telemetry.Adapter[fakeOptions]) { a.Kinds = nil },
		"unknown kind":   func(a *telemetry.Adapter[fakeOptions]) { a.Kinds = []telemetry.Kind{"events"} },
		"duplicate kind": func(a *telemetry.Adapter[fakeOptions]) { a.Kinds = []telemetry.Kind{"logs", "logs"} },
		"unknown cap":    func(a *telemetry.Adapter[fakeOptions]) { a.Caps = []telemetry.Capability{"teleport"} },
		"no dialect":     func(a *telemetry.Adapter[fakeOptions]) { a.Dialect = "" },
		"no validate":    func(a *telemetry.Adapter[fakeOptions]) { a.Validate = nil },
		"no open":        func(a *telemetry.Adapter[fakeOptions]) { a.Open = nil },
	}
	for name, mut := range mutations {
		a := good
		mut(&a)
		if err := telemetry.Register(telemetry.NewRegistry(), a); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	r := telemetry.NewRegistry()
	if err := telemetry.Register(r, good); err != nil {
		t.Fatal(err)
	}
	if err := telemetry.Register(r, good); err == nil {
		t.Error("duplicate registration must fail")
	}
	if err := telemetry.Register[fakeOptions](nil, good); err == nil {
		t.Error("nil registry must fail")
	}
	if err := telemetry.Register(&telemetry.Registry{}, good); err == nil {
		t.Error("zero registry must fail")
	}
}

func TestDescribeDoesNoIOAndCopies(t *testing.T) {
	r := telemetry.NewRegistry()
	if err := telemetry.Register(r, fakeAdapter("fake", telemetry.CapRange)); err != nil {
		t.Fatal(err)
	}
	d, ok := r.Describe("fake")
	if !ok || d.Dialect != "fake query language" || len(d.Kinds) != 2 {
		t.Fatalf("descriptor = %+v ok = %v", d, ok)
	}
	d.Kinds[0] = "mutated"
	again, _ := r.Describe("fake")
	if again.Kinds[0] != telemetry.KindMetrics {
		t.Fatal("Describe must return a copy")
	}
	if _, ok := r.Describe("missing"); ok {
		t.Fatal("unknown adapter must not be described")
	}
	if got := r.Adapters(); len(got) != 1 || got[0] != "fake" {
		t.Fatalf("Adapters = %v", got)
	}
}

func TestCheckBinding(t *testing.T) {
	r := telemetry.NewRegistry()
	if err := telemetry.Register(r, fakeAdapter("fake")); err != nil {
		t.Fatal(err)
	}
	good := telemetry.Binding{ID: "prom", Adapter: "fake", Kinds: []telemetry.Kind{telemetry.KindMetrics}, Options: []byte(`{"tenant":"a"}`)}
	if err := r.Check(good); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		mut  func(*telemetry.Binding)
		want string
	}{
		"bad id":           {func(b *telemetry.Binding) { b.ID = "Prom" }, "must match"},
		"unknown adapter":  {func(b *telemetry.Binding) { b.Adapter = "nope" }, "unknown adapter"},
		"no kinds":         {func(b *telemetry.Binding) { b.Kinds = nil }, "no kinds"},
		"unsupported kind": {func(b *telemetry.Binding) { b.Kinds = []telemetry.Kind{telemetry.KindTraces} }, "does not serve"},
		"duplicate kind":   {func(b *telemetry.Binding) { b.Kinds = []telemetry.Kind{"logs", "logs"} }, "twice"},
		"unknown option":   {func(b *telemetry.Binding) { b.Options = []byte(`{"tenent":"a"}`) }, "unknown field"},
		"trailing options": {func(b *telemetry.Binding) { b.Options = []byte(`{} {}`) }, "trailing"},
		"adapter rejects":  {func(b *telemetry.Binding) { b.Options = []byte(`{"tenant":"forbidden"}`) }, "forbidden"},
		"bad origin":       {func(b *telemetry.Binding) { b.Origin = "elsewhere" }, "origin"},
	}
	for name, c := range cases {
		b := good
		c.mut(&b)
		err := r.Check(b)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want substring %q", name, err, c.want)
		}
	}
	for _, empty := range []string{"", "null"} {
		b := good
		b.Options = []byte(empty)
		if err := r.Check(b); err != nil {
			t.Errorf("options %q must mean defaults: %v", empty, err)
		}
	}
}

func TestSourceFromBinding(t *testing.T) {
	r := telemetry.NewRegistry()
	if err := telemetry.Register(r, fakeAdapter("fake", telemetry.CapRange)); err != nil {
		t.Fatal(err)
	}
	s, err := r.Source(telemetry.Binding{ID: "prom", Adapter: "fake", Kinds: []telemetry.Kind{telemetry.KindLogs}, Default: true})
	if err != nil {
		t.Fatal(err)
	}
	if s.Origin != telemetry.OriginOperator || s.Dialect != "fake query language" || !s.Has(telemetry.CapRange) || !s.Serves(telemetry.KindLogs) || s.Serves(telemetry.KindMetrics) || !s.Default {
		t.Fatalf("source = %+v", s)
	}
}

func TestOpenChecksAndEnforcesAlertReader(t *testing.T) {
	r := telemetry.NewRegistry()
	alerting := fakeAdapter("alerting", telemetry.CapAlerts)
	if err := telemetry.Register(r, alerting); err != nil {
		t.Fatal(err)
	}
	noAlerts := fakeAdapter("noalerts", telemetry.CapAlerts)
	noAlerts.Open = func(fakeOptions, telemetry.Endpoint, telemetry.Env) (telemetry.Client, error) {
		return plainClient{}, nil
	}
	if err := telemetry.Register(r, noAlerts); err != nil {
		t.Fatal(err)
	}
	nilClient := fakeAdapter("nilclient")
	nilClient.Open = func(fakeOptions, telemetry.Endpoint, telemetry.Env) (telemetry.Client, error) { return nil, nil }
	if err := telemetry.Register(r, nilClient); err != nil {
		t.Fatal(err)
	}
	failing := fakeAdapter("failing")
	failing.Open = func(fakeOptions, telemetry.Endpoint, telemetry.Env) (telemetry.Client, error) {
		return nil, errString("boom")
	}
	if err := telemetry.Register(r, failing); err != nil {
		t.Fatal(err)
	}
	bind := func(adapter string) telemetry.Binding {
		return telemetry.Binding{ID: "s", Adapter: adapter, Kinds: []telemetry.Kind{telemetry.KindMetrics}}
	}
	c, err := r.Open(bind("alerting"), telemetry.Env{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.(telemetry.AlertReader); !ok {
		t.Fatal("opened client must be an AlertReader")
	}
	for adapter, want := range map[string]string{"noalerts": "AlertReader", "nilclient": "nil client", "failing": "boom"} {
		if _, err := r.Open(bind(adapter), telemetry.Env{}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", adapter, err, want)
		}
	}
	bad := bind("alerting")
	bad.Options = []byte(`{"nope":1}`)
	if _, err := r.Open(bad, telemetry.Env{}); err == nil {
		t.Error("Open must check the binding first")
	}
}

// plainClient answers queries but is not an AlertReader.
type plainClient struct{}

func (plainClient) Query(context.Context, telemetry.Query, time.Time) (telemetry.Result, error) {
	return telemetry.Result{}, nil
}
