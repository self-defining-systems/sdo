package operatorcli

import (
	"strings"
	"testing"
	"time"
)

func TestStartNoticeSaysHowLongAndWhereTheReportGoes(t *testing.T) {
	got := startNotice(90*time.Second, 10*time.Minute, true, 60*time.Second, 5, "/tmp/r.txt")
	for _, want := range []string{"about 19m0s", "background", "/tmp/r.txt"} {
		if !strings.Contains(got, want) {
			t.Errorf("notice %q lacks %q", got, want)
		}
	}
	if short := startNotice(90*time.Second, 0, false, 0, 5, ""); strings.Contains(short, "also written") || !strings.Contains(short, "about 2m0s") {
		t.Errorf("notice without break or report file = %q", short)
	}
}
