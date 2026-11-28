package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

const unscrapedMapYAML = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false, observe: {logs: "container web: access log"}}
edges: []
operations:
  - id: list-items
    entry: {service: web, port: 80, method: GET, path: /items}
    path: [web]
probes:
  - {scenario: browse, kind: call, operations: [list-items]}
`

// A Service SDO's Prometheus scrapes no request counter from has nothing to
// map: the metric items say so once as a note and ask no entries record,
// reason or metrics pointer for it.
func TestNoScrapedRequestCounterIsNotRed(t *testing.T) {
	m, err := appmap.Parse(unscrapedMapYAML)
	if err != nil {
		t.Fatal(err)
	}
	p := &fakeProm{}
	for name, scraped := range map[string]Scraped{
		"not scraped":         func(string) ScrapeFacts { return ScrapeFacts{} },
		"scraped, no counter": func(string) ScrapeFacts { return ScrapeFacts{Scraped: true} },
	} {
		r, err := Run(context.Background(), Config{
			Map: m, Commit: "c", Prom: p, Sender: fakeSender{prom: p, emit: perRoute},
			Requests: 5, Settle: time.Second, Poll: time.Second, Wait: p.wait, Scraped: scraped,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range r.Results {
			if x.Status == Red && (x.Item == ItemJourneys || (x.Item == ItemPointers && name == "not scraped")) {
				t.Fatalf("%s: red for a counter that does not exist: %+v\n%s", name, x, r.Text())
			}
		}
		if status(r, ItemJourneys, "list-items") != Note || !strings.Contains(r.Text(), "scrapes no request counter") {
			t.Fatalf("%s: no note saying there is nothing to map:\n%s", name, r.Text())
		}
	}
}

// Without the scrape facts (nil) a missing entries record stays red.
func TestMissingEntriesRecordStaysRedWithoutScrapeFacts(t *testing.T) {
	r, _ := run(t, unscrapedMapYAML, 0, time.Second, perRoute)
	if status(r, ItemJourneys, "list-items") != Red {
		t.Fatalf("missing entries record:\n%s", r.Text())
	}
}
