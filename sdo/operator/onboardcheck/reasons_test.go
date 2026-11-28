package onboardcheck

import (
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
)

// A reason the map records turns that red item into a note, whichever check
// raised it, and every red names the map lines that clear it.
func TestReasonsApplyToEveryRedItemAndRedsNameTheFix(t *testing.T) {
	cfg := Config{Map: appmap.Map{Reasons: []appmap.Reason{{Item: ItemPointers, Subject: "view-page", Reason: "static files only"}}}}
	r := Report{Results: []Result{
		{Item: ItemPointers, Subject: "view-page", Status: Red, Detail: pointersDetail([]string{"frontend"})},
		{Item: ItemPointers, Subject: "other", Status: Red, Detail: pointersDetail([]string{"frontend"})},
	}}
	applyReasons(cfg, &r)
	if r.Results[0].Status != Note || !strings.Contains(r.Results[0].Detail, "static files only") || r.Results[1].Status != Red {
		t.Fatalf("%+v", r.Results)
	}
	for _, detail := range []string{pointersDetail([]string{"frontend"}), noEntriesDetail("frontend", "view-page"), routesListDetail} {
		if !strings.Contains(detail, "reasons:") && !strings.Contains(detail, "routes:") || !strings.Contains(detail, "{") {
			t.Errorf("no map lines to add: %s", detail)
		}
	}
}
