package storestate_test

import (
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/storestate"
)

// An id sequence at the end of its type's range fails every insert into its
// table: the read lists it and the detector reports it without a baseline.
func TestPostgresSequenceAtItsMaximumIsReported(t *testing.T) {
	exhausted := strings.Replace(healthyPostgres, "waiting\t0\n", "waiting\t0\nseq\tpublic.notes_id_seq\t2147483647\t2147483647\n", 1)
	f := parsePostgres(t, exhausted)
	if g, ok := f.Sequences["public.notes_id_seq"]; !ok || g.Used != 2147483647 || g.Max != 2147483647 {
		t.Fatalf("sequences = %+v", f.Sequences)
	}
	if len(parsePostgres(t, healthyPostgres).Sequences) != 0 {
		t.Fatal("a healthy read lists no sequence")
	}
	c := config(t)
	c.Component, c.Engine = "db", storestate.EnginePostgres
	d := detector(t, c)
	findings, err := d.Detect(context.Background(), snapshot(t, d, reading(exhausted)))
	if err != nil {
		t.Fatal(err)
	}
	var found *sdk.Finding
	for i := range findings {
		if findings[i].RuleID == storestate.RuleSequenceNearLimit {
			found = &findings[i]
		}
	}
	if found == nil || !strings.Contains(found.Summary, "public.notes_id_seq") {
		t.Fatalf("findings = %+v, want the exhausted sequence named", findings)
	}
}
