package operatorcli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func runOperabilityUnit(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := RunOperability(context.Background(), args, func(string) string { return "" }, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestOperabilityUsageErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no command":   {nil, "usage: sdo-operator operability"},
		"unknown":      {[]string{"bogus"}, "unknown operability command"},
		"live only":    {[]string{"check"}, "only --offline"},
		"stage":        {[]string{"check", "--offline", "--stage", "later"}, "unknown stage"},
		"positional":   {[]string{"check", "--offline", "extra"}, "no arguments"},
		"unknown flag": {[]string{"check", "--offline", "--nope"}, "nope"},
		"missing root": {[]string{"check", "--offline", "--root", "/definitely/not/here"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			code, out, errOut := runOperabilityUnit(tc.args...)
			if code != exitInfra || !strings.Contains(out+errOut, tc.want) {
				t.Fatalf("code %d out=%q err=%q", code, out, errOut)
			}
		})
	}
	if code, out, _ := runOperabilityUnit("help"); code != exitClean || !strings.Contains(out, "check --offline") {
		t.Fatalf("help = %d %q", code, out)
	}
}
