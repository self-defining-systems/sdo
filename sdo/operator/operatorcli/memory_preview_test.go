package operatorcli

import (
	"bytes"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestPlanPreviewTextFromTheBrokerReport(t *testing.T) {
	report := &contracts.MemoryCheckReport{CoverageGaps: []*contracts.MemoryDiagnostic{
		{Message: "program first preview (production dry-run needs an accepted source fixture)"},
		{Message: "program second preview (production plan build failed on recorded inputs)"},
		{Message: "unrelated advisory"},
	}}
	for _, id := range []string{"", "first"} {
		var output bytes.Buffer
		writePlanPreviewGaps(&output, report, id)
		got := output.String()
		if !strings.Contains(got, "program first preview") ||
			strings.Contains(got, "program second preview") != (id == "") ||
			strings.Contains(got, "unrelated advisory") {
			t.Errorf("preview output for %q = %q", id, got)
		}
	}
}
