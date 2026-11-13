package appstate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/appstate"
	"sdo.dev/operator/sdk/program"
)

// base is the host's live read factory: it embeds program.ReadView (whose
// Observe can exec commands and send authenticated HTTP) and answers only the
// two reads a reader may make.
type base struct {
	program.ReadView
	reads   int
	fail    map[int]error
	observe int
}

func (b *base) Read(q program.Query) (program.Observation, error) {
	b.reads++
	if err := b.fail[b.reads]; err != nil {
		return program.Observation{}, err
	}
	return program.Observation{Rows: []program.Row{{"setting": int64(7)}}}, nil
}

func (b *base) KubernetesRead(r program.KubernetesRequest) (json.RawMessage, error) {
	b.reads++
	return json.RawMessage(`{}`), nil
}

func (b *base) Observe(program.ExternalOperation) (int32, error) { b.observe++; return 0, nil }

type readerFunc func(context.Context, appstate.View) (appstate.Values, error)

func (f readerFunc) ReadState(ctx context.Context, v appstate.View) (appstate.Values, error) {
	return f(ctx, v)
}

var query = program.Query{Probe: &program.ProbeQuery{
	To:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "web"},
	Req: program.HTTPProbe{Method: "GET", Path: "/status", Fields: map[string]string{"setting": "/setting"}},
}}

func TestViewExposesOnlyNarrowReads(t *testing.T) {
	typ := reflect.TypeFor[appstate.View]()
	for _, banned := range []string{"Observe", "ProbeHTTP", "ProbeTCP", "Logs", "DatabaseUsers", "ReadView"} {
		if _, ok := typ.MethodByName(banned); ok {
			t.Errorf("View exposes %s", banned)
		}
	}
	for _, want := range []string{"Read", "KubernetesRead", "ObservedAt", "Namespace"} {
		if _, ok := typ.MethodByName(want); !ok {
			t.Errorf("View lacks %s", want)
		}
	}
}

func TestRunReturnsTheReadersValues(t *testing.T) {
	b := &base{}
	values, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
		obs, err := v.Read(query)
		if err != nil {
			return appstate.Values{}, err
		}
		var out appstate.Values
		for k, x := range obs.Rows[0] {
			out.Set(k, fmt.Sprint(x))
		}
		return out, nil
	}), b)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := values.Get("setting"); got != "7" {
		t.Fatalf("values = %v", values)
	}
}

func TestRunRefusesMoreThanEightReadsPerCall(t *testing.T) {
	b := &base{}
	_, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
		for i := 0; i < appstate.MaxReads+1; i++ {
			if _, err := v.Read(query); err != nil {
				return appstate.Values{}, err
			}
		}
		return appstate.Values{}, nil
	}), b)
	if !errors.Is(err, appstate.ErrReadBudget) {
		t.Fatalf("err = %v, want ErrReadBudget", err)
	}
	if b.reads != appstate.MaxReads {
		t.Fatalf("the base saw %d reads, want exactly %d", b.reads, appstate.MaxReads)
	}
}

func TestBudgetCountsKubernetesAndStoreReadsTogether(t *testing.T) {
	b := &base{}
	_, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
		for i := 0; i < 5; i++ {
			v.Read(query)
			v.KubernetesRead(program.KubernetesRequest{Method: "GET", Path: "/api/v1/namespaces/app/configmaps/x"})
		}
		return appstate.Values{}, nil
	}), b)
	if !errors.Is(err, appstate.ErrReadBudget) {
		t.Fatalf("err = %v", err)
	}
}

func TestAFailedReadFailsTheCallEvenWhenTheReaderSwallowsIt(t *testing.T) {
	b := &base{fail: map[int]error{1: errors.New("store down")}}
	_, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
		v.Read(query) // error ignored
		var out appstate.Values
		out.Set("partial", "1")
		return out, nil
	}), b)
	if err == nil || !strings.Contains(err.Error(), "store down") {
		t.Fatalf("err = %v, want the read failure", err)
	}
}

func TestKubernetesReadIsReadOnlyAndNeverReachesSecrets(t *testing.T) {
	b := &base{}
	for _, req := range []program.KubernetesRequest{
		{Method: "DELETE", Path: "/api/v1/namespaces/app/configmaps/x"},
		{Method: "POST", Path: "/api/v1/namespaces/app/configmaps"},
		{Method: "GET", Path: "/api/v1/namespaces/app/secrets/x"},
		{Method: "GET", Path: "/api/v1/namespaces/app/secrets"},
	} {
		_, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
			_, err := v.KubernetesRead(req)
			return appstate.Values{}, err
		}), b)
		if err == nil {
			t.Errorf("%s %s was allowed", req.Method, req.Path)
		}
	}
	if b.reads != 0 {
		t.Fatalf("refused reads reached the host: %d", b.reads)
	}
}

func TestInvalidQueryIsRefusedBeforeTheHost(t *testing.T) {
	b := &base{}
	_, err := appstate.Run(context.Background(), readerFunc(func(_ context.Context, v appstate.View) (appstate.Values, error) {
		_, err := v.Read(program.Query{})
		return appstate.Values{}, err
	}), b)
	if err == nil || b.reads != 0 {
		t.Fatalf("err=%v reads=%d", err, b.reads)
	}
}

func TestCredentialNamedKeyFailsTheCall(t *testing.T) {
	_, err := appstate.Run(context.Background(), readerFunc(func(context.Context, appstate.View) (appstate.Values, error) {
		var out appstate.Values
		out.Set("admin_password", "x")
		return out, nil
	}), &base{})
	if err == nil || !strings.Contains(err.Error(), "admin_password") {
		t.Fatalf("err = %v", err)
	}
}

func TestRunHonoursTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := appstate.Run(ctx, readerFunc(func(ctx context.Context, v appstate.View) (appstate.Values, error) {
		_, err := v.Read(query)
		return appstate.Values{}, err
	}), &base{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestRunRecoversAPanickingReader(t *testing.T) {
	_, err := appstate.Run(context.Background(), readerFunc(func(context.Context, appstate.View) (appstate.Values, error) {
		panic("boom")
	}), &base{})
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v", err)
	}
}
