//go:build integration

package bench

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// `lanes host adopt` and `reserve` ask the root helper to raise the kernel minimums (the defaults plus the host's own),
// as one call, after the block work.
func TestHostAdoptAndReserveRaiseTheKernelMinimums(t *testing.T) {
	root, _ := hostsForReserve(t, `reserve: {cpus: "88-111"}, kernel_minimums: {vm.max_map_count: 524288}`)
	for _, verb := range []string{"adopt", "reserve"} {
		f := &fakeHost{block: "88-111", other: "0-87,112-255"}
		var out, errw bytes.Buffer
		if rc := HostCommand(context.Background(), root, []string{verb, "h1"}, &out, &errw, f.helper, nil); rc != 0 {
			t.Fatalf("%s: %d %s", verb, rc, errw.String())
		}
		var got string
		for _, c := range f.calls {
			if strings.HasPrefix(c, "kernel raise ") {
				got = c
			}
		}
		want := "kernel raise fs.inotify.max_user_instances=8192 fs.inotify.max_user_watches=1048576 vm.max_map_count=524288"
		if got != want {
			t.Fatalf("%s: helper calls %q, want %q", verb, f.calls, want)
		}
	}
}
