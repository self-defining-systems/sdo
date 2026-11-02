package runtime

import (
	"strings"
	"testing"

)

func TestImmutableValidationImageRequiresCompleteDigest(t *testing.T) {
	for _, test := range []struct {
		image string
		want  bool
	}{
		{"validator:mutable", false},
		{"validator@sha256:bad", false},
		{"validator@sha256:" + strings.Repeat("z", 64), false},
		{"validator@sha256:" + strings.Repeat("a", 64), true},
	} {
		if got := immutableValidationImage(test.image); got != test.want {
			t.Errorf("%q immutable=%v", test.image, got)
		}
	}
}

