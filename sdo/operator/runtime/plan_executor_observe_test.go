package runtime

import (
	"testing"
	"testing/synctest"
)

func TestResourceVersionAtLeast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			have, want string
			ok         bool
		}{
			{"10", "9", true}, {"9", "10", false}, {"7", "7", true}, {"", "7", false},
			{"abc", "abc", true}, {"abc", "abd", false},
		} {
			if got := resourceVersionAtLeast(tc.have, tc.want); got != tc.ok {
				t.Errorf("resourceVersionAtLeast(%q, %q) = %t, want %t", tc.have, tc.want, got, tc.ok)
			}
		}
	})
}
