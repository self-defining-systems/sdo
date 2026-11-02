package traffic_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/traffic"
)

// checkout is a two-step write journey through a JSON API: add an item to a
// synthetic cart, then place an order, as a shop's own load generator does.
func checkout() traffic.Scenario {
	return traffic.Scenario{
		ID: "checkout", Target: traffic.Target{Service: "frontend", Port: 8080},
		DependsOn:  []string{"frontend", "cart", "checkout", "payment"},
		SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
		Steps: []traffic.Step{
			{Name: "add", Endpoint: traffic.POST("/api/cart", nil).WithJSON(
				`{"userId":"{user}","item":{"productId":"{product}","quantity":{quantity}}}`,
				traffic.Params{"user": traffic.Const("sdo-synthetic-1"), "product": traffic.OneOf(`OL"JC`), "quantity": traffic.IntBetween(1, 3)})},
			{Name: "order", Endpoint: traffic.POST("/api/checkout", nil).WithJSON(
				`{"userId":"{user}","email":"sdo-synthetic@example.com"}`, traffic.Params{"user": traffic.Const("sdo-synthetic-1")}).
				Contains(`"orderId"`)},
		},
	}
}

func TestWithJSONSendsAValidJSONBodyFilledFromParamsAndState(t *testing.T) {
	app := &recorder{handle: func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/checkout" {
			_, _ = w.Write([]byte(`{"orderId":"o-1"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}}
	e := engine(t, workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "checkout"}),
		traffic.Catalog{checkout()}, serve(t, app))
	if _, sample := e.Iterate(context.Background(), 0); sample.Outcome != traffic.OutcomeOK {
		t.Fatalf("healthy checkout: %+v", sample)
	}
	if app.count() != 2 {
		t.Fatalf("want two requests, got %d", app.count())
	}
	var added struct {
		UserID string `json:"userId"`
		Item   struct {
			ProductID string `json:"productId"`
			Quantity  int    `json:"quantity"`
		} `json:"item"`
	}
	if err := json.Unmarshal([]byte(app.bodies[0]), &added); err != nil {
		t.Fatalf("body must be valid JSON with the value escaped: %v: %s", err, app.bodies[0])
	}
	if added.UserID != "sdo-synthetic-1" || added.Item.ProductID != `OL"JC` || added.Item.Quantity < 1 || added.Item.Quantity > 3 {
		t.Fatalf("placeholders must be filled from params: %+v", added)
	}
	if got := app.requests[0].Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("JSON body needs its content type, got %q", got)
	}
}

func TestWithJSONFillsPlaceholdersFromEarlierSteps(t *testing.T) {
	app := &recorder{handle: func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"cart-7"}`))
	}}
	scenario := traffic.Scenario{
		ID: "chain", Target: traffic.Target{Service: "frontend", Port: 8080},
		SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
		Steps: []traffic.Step{
			{Name: "open", Endpoint: traffic.POST("/carts", nil).WithJSON(`{"owner":"sdo-synthetic"}`, nil).SaveJSON("id", "cart")},
			{Name: "use", Endpoint: traffic.PUT("/carts", nil).WithJSON(`{"owner":"sdo-synthetic","cart":"{cart}"}`, nil)},
		},
	}
	e := engine(t, workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "chain"}),
		traffic.Catalog{scenario}, serve(t, app))
	if _, sample := e.Iterate(context.Background(), 0); sample.Outcome != traffic.OutcomeOK {
		t.Fatalf("chained JSON steps: %+v", sample)
	}
	if app.bodies[1] != `{"owner":"sdo-synthetic","cart":"cart-7"}` {
		t.Fatalf("state placeholder not filled: %s", app.bodies[1])
	}
}

func TestWithJSONRefusesABodyThatIsNotJSONOrHasAnUnknownPlaceholder(t *testing.T) {
	for name, endpoint := range map[string]*traffic.HTTPEndpoint{
		"invalid": traffic.POST("/x", nil).WithJSON(`{"user":sdo-synthetic}`, nil),
		"unknown": traffic.POST("/x", nil).WithJSON(`{"user":"sdo-synthetic","cart":"{cart}"}`, nil),
		"form":    traffic.POST("/x", nil).WithJSON(`{"user":"sdo-synthetic"}`, nil).WithForm(traffic.Params{"a": traffic.Const("b")}),
	} {
		t.Run(name, func(t *testing.T) {
			app := &recorder{}
			scenario := traffic.Scenario{ID: "bad", Target: traffic.Target{Service: "frontend", Port: 8080},
				SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
				Steps: []traffic.Step{{Name: "write", Endpoint: endpoint}}}
			e := engine(t, workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "bad"}),
				traffic.Catalog{scenario}, serve(t, app))
			if _, sample := e.Iterate(context.Background(), 0); sample.Outcome != traffic.OutcomeInvalid || app.count() != 0 {
				t.Fatalf("an unbuildable body must be refused before sending: %+v, %d sent", sample, app.count())
			}
		})
	}
}

func TestUndrivenLinksNamesEveryDeclaredEdgeNoHealthScenarioExercises(t *testing.T) {
	browse := traffic.Scenario{ID: "browse", Target: traffic.Target{Service: "frontend", Port: 8080},
		DependsOn: []string{"frontend", "catalog"}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "list", Endpoint: traffic.GET("/products", nil)}}}
	catalog := traffic.Catalog{browse, checkout()}
	links := traffic.Workload{APIVersion: traffic.APIVersion, Kind: traffic.WorkloadKind, Name: "links",
		Purpose: traffic.PurposeLinkProbe, Links: []traffic.Link{
			{From: "frontend", To: "catalog", Port: 8080},
			{From: "checkout", To: "payment", Port: 8080},
			{From: "checkout", To: "email", Port: 8080},
		}}.WithDefaults()
	readsOnly := workload(t, "health", traffic.PurposeHealthProbe, traffic.WorkloadScenario{ID: "browse"})
	got := traffic.UndrivenLinks(catalog, []traffic.Workload{readsOnly, links})
	if len(got) != 2 || got[0].String() != "checkout -> email:8080" || got[1].String() != "checkout -> payment:8080" {
		t.Fatalf("a read-only health probe leaves the write edges undriven: %v", got)
	}
	// A scenario that only runs on demand does not drive an edge: nothing
	// would notice it break between repairs.
	burst := workload(t, "verify", traffic.PurposeVerifyBurst, traffic.WorkloadScenario{ID: "checkout"})
	if got := traffic.UndrivenLinks(catalog, []traffic.Workload{readsOnly, burst, links}); len(got) != 2 {
		t.Fatalf("on-demand workloads must not count as driving an edge: %v", got)
	}
	withWrites := workload(t, "health", traffic.PurposeHealthProbe,
		traffic.WorkloadScenario{ID: "browse"}, traffic.WorkloadScenario{ID: "checkout"})
	got = traffic.UndrivenLinks(catalog, []traffic.Workload{withWrites, links})
	if len(got) != 1 || got[0].String() != "checkout -> email:8080" {
		t.Fatalf("the checkout scenario drives checkout -> payment only: %v", got)
	}
	if !strings.Contains(traffic.UndrivenLinkNote(got[0]), "checkout -> email:8080") {
		t.Fatalf("note must name the edge: %s", traffic.UndrivenLinkNote(got[0]))
	}
}
