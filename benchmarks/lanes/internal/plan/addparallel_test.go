//go:build integration

package plan

import (
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// At add every distinct rendered spec is checked, and checking one asks hosts and checkouts: the specs are checked
// together (a bound), not one after another, and a refusal is still the first one in node order.
func TestAddChecksDistinctSpecsTogetherAndReportsTheFirstRefusalInOrder(t *testing.T) {
	l := newLab(t)
	l.write("tpl-plain.yaml", `{"name": "x", "app": "examples/${item}", "operator": {"hooks": "h"}, "faults": {"ids": ["${item}"]}}`)
	file := l.write("plan.yaml", "plan: tp\npins: {workload: fuzzops@4652656, subject: sdo@9b2cadb7f}\narms: [{name: p1, spec: tpl-plain.yaml}]\nitems: [a, b, c, d]\n")
	var running, peak atomic.Int32
	validate := func(b []byte) error {
		n := running.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(150 * time.Millisecond)
		running.Add(-1)
		if strings.Contains(string(b), "examples/b") || strings.Contains(string(b), "examples/d") {
			return errors.New("refused")
		}
		return nil
	}
	start := time.Now()
	_, err := Add(filepath.Join(l.root, "plans"), file, l.clock, validate)
	if err == nil || !strings.Contains(err.Error(), "item b") {
		t.Fatalf("err = %v, want the refusal of item b (the first in order)", err)
	}
	if took := time.Since(start); took > 450*time.Millisecond || peak.Load() < 2 {
		t.Fatalf("4 specs took %s with %d at once: checked one after another", took, peak.Load())
	}
}
