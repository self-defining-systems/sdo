package program

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestBoundedLogOptionsAskForTheNewestLinesNotAByteLimit(t *testing.T) {
	bounded, limit, err := BoundedLogOptions(corev1.PodLogOptions{Container: "c"})
	if err != nil || bounded.LimitBytes != nil || bounded.TailLines == nil || limit != MaxLogBytes {
		t.Fatalf("%+v %d %v", bounded, limit, err)
	}
	small := int64(10)
	if _, limit, err := BoundedLogOptions(corev1.PodLogOptions{LimitBytes: &small}); err != nil || limit != 10 {
		t.Fatalf("%d %v", limit, err)
	}
	huge := int64(MaxLogBytes + 1)
	if _, _, err := BoundedLogOptions(corev1.PodLogOptions{LimitBytes: &huge}); err == nil {
		t.Fatal("a limit above the maximum is refused")
	}
}

func TestNewestLogBytesKeepsTheNewestWholeLines(t *testing.T) {
	data := "old line 1\nold line 2\nnew line 3\nnew line 4\n"
	got := NewestLogBytes(data, 25)
	if got != "new line 3\nnew line 4\n" {
		t.Fatalf("%q", got)
	}
	if NewestLogBytes(data, 1000) != data || !strings.HasSuffix(NewestLogBytes(strings.Repeat("x\n", 100000), MaxLogBytes), "x\n") {
		t.Fatal("short data is kept whole and the newest end survives")
	}
}
