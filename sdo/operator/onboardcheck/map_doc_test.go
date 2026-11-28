package onboardcheck

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

const mapDocFile = "../../skills/sdo-operations-map/references/request-metrics.md"

// docExample returns the YAML block of the request-metrics reference, the
// worked example the metric reds point at.
func docExample(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(mapDocFile)
	if err != nil {
		t.Fatal(err)
	}
	section := string(data)
	if !strings.Contains(mapDoc, "references/request-metrics.md") {
		t.Fatalf("%s has no section the reds name (%s)", mapDocFile, mapDoc)
	}
	_, block, ok := strings.Cut(section, "```yaml\n")
	if !ok {
		t.Fatal("the reference has no yaml example")
	}
	block, _, _ = strings.Cut(block, "```")
	return block
}

// The documented entries, observe pointers and reasons parse strictly and
// pass the metric items as the application they describe would answer: an
// entry counter labelled by handler, method and status, and a static file
// server that counts nothing and is excused by its recorded reasons.
func TestMapDocExamplePassesTheMetricItems(t *testing.T) {
	m, err := appmap.Parse(docExample(t))
	if err != nil {
		t.Fatalf("documented example does not parse: %v", err)
	}
	p := &fakeProm{}
	emit := func(op appmap.Operation) map[string]string {
		e, _ := m.EntryFor(op.Entry.Service)
		labels := map[string]string{e.Labels.Route: op.RouteValue(), e.Labels.Method: op.Entry.Method, e.Labels.Status: "200"}
		for k, v := range e.Selector {
			labels[k] = v
		}
		return labels
	}
	r, err := Run(context.Background(), Config{
		Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: emit},
		Exposed:  []Exposure{{Service: "web", Via: "NodePort"}, {Service: "api", Via: "NodePort"}},
		Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range r.Results {
		// Probes and the route list are other sections of the map.
		if x.Status == Red && x.Item != ItemProbe && x.Item != ItemRoutesAssigned {
			t.Errorf("documented example is red: %s %s: %s", x.Item, x.Subject, x.Detail)
		}
	}
	for _, want := range [][2]string{{ItemJourneys, "open-page"}, {ItemPointers, "open-page"}, {ItemRoutes, "web"}} {
		if status(r, want[0], want[1]) != Note {
			t.Errorf("%s %s should be a note carrying its reason:\n%s", want[0], want[1], r.Text())
		}
	}
	for _, want := range [][2]string{{ItemJourneys, "list-items"}, {ItemErrorsSeparable, "list-items"}, {ItemPointers, "list-items"}, {ItemRoutes, "api"}} {
		if status(r, want[0], want[1]) != Green {
			t.Errorf("%s %s should be green:\n%s", want[0], want[1], r.Text())
		}
	}
}

// Every red about a missing entry or pointer names the reason line that
// clears that very red (its item and subject) and the documented example.
func TestMetricRedsNameTheLineThatClearsThem(t *testing.T) {
	m, err := appmap.Parse(docExample(t))
	if err != nil {
		t.Fatal(err)
	}
	m.Reasons = nil
	p := &fakeProm{}
	r, err := Run(context.Background(), Config{
		Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute},
		Exposed:  []Exposure{{Service: "web", Via: "NodePort"}},
		Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct{ item, subject, line string }{
		{ItemJourneys, "open-page", "{item: journeys-move-own-metrics, subject: open-page,"},
		{ItemPointers, "open-page", "{item: map-pointers, subject: <operation>,"},
		{ItemRoutes, "web", "{item: routes-match-operations, subject: web,"},
	} {
		x := find(r, want.item, want.subject)
		if x.Status != Red || !strings.Contains(x.Detail, want.line) || !strings.Contains(x.Detail, mapDoc) {
			t.Errorf("%s %s: want red naming %q and %q, got %s: %s", want.item, want.subject, want.line, mapDoc, x.Status, x.Detail)
		}
	}
}

func find(r Report, item, subject string) Result {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject {
			return x
		}
	}
	return Result{}
}
