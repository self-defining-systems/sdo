package operatorcli

import (
	"strings"
	"testing"
	"time"
)

func TestStartNoticeSaysHowLongAndWhereTheReportGoes(t *testing.T) {
	got := startNotice(false, 90*time.Second, 10*time.Minute, true, 60*time.Second, 4, "/tmp/r.txt")
	for _, want := range []string{"(full)", "at most 16m0s", "background", "/tmp/r.txt"} {
		if !strings.Contains(got, want) {
			t.Errorf("notice %q lacks %q", got, want)
		}
	}
	if short := startNotice(true, 90*time.Second, 0, false, 0, 5, ""); strings.Contains(short, "also written") || !strings.Contains(short, "(quick)") || !strings.Contains(short, "at most 1m30s") {
		t.Errorf("notice without break or report file = %q", short)
	}
}
