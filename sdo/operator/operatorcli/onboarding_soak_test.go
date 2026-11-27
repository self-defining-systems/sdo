package operatorcli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"sdo.dev/operator/journeycheck"
)

// The command waits for the installed controller's record, then: green is one
// line and exit 0; red prints the follow-up turn and exits 1; a soak that has
// not ended within --wait exits 2.
func TestOnboardingSoakReportsTheInstalledSoak(t *testing.T) {
	green := &journeycheck.SoakRecord{Results: []journeycheck.Result{{Item: journeycheck.ItemSoak, Subject: "probe/a", Status: journeycheck.Green}}}
	red := &journeycheck.SoakRecord{Results: []journeycheck.Result{{Item: journeycheck.ItemSoak, Subject: "probe/a", Status: journeycheck.Red, Detail: "1 of 9 samples failed while healthy: synthetic"}}}
	for name, tc := range map[string]struct {
		records []*journeycheck.SoakRecord
		code    int
		want    string
	}{
		"green after waiting": {[]*journeycheck.SoakRecord{nil, nil, green}, exitClean, "green"},
		"red":                 {[]*journeycheck.SoakRecord{red}, exitDiagnostics, "reply with one line"},
		"not ended":           {[]*journeycheck.SoakRecord{nil, nil, nil, nil, nil}, exitInfra, ""},
	} {
		t.Run(name, func(t *testing.T) {
			reads := 0
			read := func(context.Context) (*journeycheck.SoakRecord, error) {
				record := tc.records[min(reads, len(tc.records)-1)]
				reads++
				return record, nil
			}
			var out bytes.Buffer
			code := waitOnboardingSoak(context.Background(), read, 3, func(context.Context) error { return nil }, &out)
			if code != tc.code || !strings.Contains(out.String(), tc.want) {
				t.Fatalf("code %d, output:\n%s", code, out.String())
			}
		})
	}
}
