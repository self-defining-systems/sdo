package helm_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm/internal/policy"
)

// A chart in Git is attacker-influenced input to the controller process. These
// charts each make an unbounded or amplifying render; every one must come back
// as a typed policy error, never a Go fatal (stack overflow, out of memory) or
// a hang. A stack overflow cannot be recovered, so a regression here kills the
// test binary.
func TestRenderBoundsRejectRunawayCharts(t *testing.T) {
	cm := func(value string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: " + value + "\n"
	}
	// A chain of 40 helpers each including the next twice: 2^40 executions,
	// each shallow, so only a CPU bound can stop it.
	var chain strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&chain, `{{ define "h%d" }}{{ include "h%d" . }}{{ include "h%d" . }}{{ end }}`, i, i+1, i+1)
	}
	chain.WriteString(`{{ define "h40" }}x{{ end }}`)
	cases := map[string]struct {
		files map[string]string
		rule  policy.Rule
	}{
		"tpl that expands itself": {map[string]string{
			"c/values.yaml":      "s: '{{ tpl .Values.s . }}'\n",
			"c/templates/x.yaml": cm("{{ tpl .Values.s . | quote }}")}, policy.RuleRenderDepth},
		"tpl that expands itself twice per level": {map[string]string{
			"c/values.yaml":      "s: '{{ tpl .Values.s . }}{{ tpl .Values.s . }}'\n",
			"c/templates/x.yaml": cm("{{ tpl .Values.s . | quote }}")}, policy.RuleRenderDepth},
		"include of a template that includes itself": {map[string]string{
			"c/templates/_h.tpl": `{{ define "h" }}{{ include "h" . }}{{ end }}`,
			"c/templates/x.yaml": cm(`{{ include "h" . | quote }}`)}, policy.RuleRenderDepth},
		"two helpers that include each other": {map[string]string{
			"c/templates/_h.tpl": `{{ define "a" }}{{ include "b" . }}{{ end }}{{ define "b" }}{{ include "a" . }}{{ end }}`,
			"c/templates/x.yaml": cm(`{{ include "a" . | quote }}`)}, policy.RuleRenderDepth},
		"until with a huge count": {map[string]string{
			"c/templates/x.yaml": cm(`{{ len (until 1000000000) }}`)}, policy.RuleRenderBudget},
		"untilStep with a huge span": {map[string]string{
			"c/templates/x.yaml": cm(`{{ len (untilStep 0 1000000000000 1) }}`)}, policy.RuleRenderBudget},
		"repeat past the buffer cap": {map[string]string{
			"c/templates/x.yaml": cm(`{{ repeat 1000000 (repeat 1000000 "x") | len }}`)}, policy.RuleRenderBudget},
		"indent with a huge width": {map[string]string{
			"c/templates/x.yaml": cm(`{{ indent 1000000000 "x" | len }}`)}, policy.RuleRenderBudget},
		"output past the buffer cap": {map[string]string{
			"c/templates/_h.tpl": `{{ define "big" }}{{ repeat 1000000 "0123456789abcdef" }}{{ end }}`,
			"c/templates/x.yaml": cm(`{{ include "big" . }}{{ include "big" . }}{{ include "big" . | quote }}`)}, policy.RuleRenderBudget},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := renderChart(t, minimal(tc.files), render.Env{Namespace: "n", Release: "r"}, cfg("c"))
			if !policy.Violates(err, tc.rule) {
				t.Fatalf("want a %s violation, got %v", tc.rule, err)
			}
		})
	}

	t.Run("fan-out stops at the caller's deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		files := minimal(map[string]string{
			"c/templates/_h.tpl": chain.String(),
			"c/templates/x.yaml": cm(`{{ include "h0" . | quote }}`)})
		_, err := renderChartContext(ctx, t, files, render.Env{Namespace: "n", Release: "r"}, cfg("c"))
		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("want the deadline error, got %v", err)
		}
	})
}
