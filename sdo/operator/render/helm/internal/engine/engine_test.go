package engine_test

import (
	"maps"
	"testing"
	"text/template"

	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	helmengine "helm.sh/helm/v3/pkg/engine"

	"sdo.dev/operator/render/helm/internal/engine"
)

func render(t *testing.T) (map[string]string, chartutil.Values, func(e engine.Engine) map[string]string) {
	t.Helper()
	return renderChart(t, "app")
}

func renderChart(t *testing.T, name string) (map[string]string, chartutil.Values, func(e engine.Engine) map[string]string) {
	t.Helper()
	ch, err := loader.LoadDir("../../testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	vals, err := chartutil.ToRenderValues(ch, nil, chartutil.ReleaseOptions{Name: "r", Namespace: "n", Revision: 1, IsInstall: true}, chartutil.DefaultCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	want, err := helmengine.Render(ch, vals)
	if err != nil {
		t.Fatal(err)
	}
	return want, vals, func(e engine.Engine) map[string]string {
		t.Helper()
		got, err := e.Render(ch, vals)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
}

// The trimmed copy must render what the Helm engine it was copied from
// renders. NOTES.txt is the one intended difference (never executed here).
//
// umbrella is a synthetic umbrella chart with the helper patterns large
// charts rely on (tpl over values, computed include, required, fail-guarded
// helpers, duration, a checksum over another template); no files from any real chart.
func TestEngineMatchesHelm(t *testing.T) {
	for _, chart := range []string{"app", "umbrella"} {
		t.Run(chart, func(t *testing.T) {
			want, _, run := renderChart(t, chart)
			got := run(engine.Engine{})
			for name, w := range want {
				if g, ok := got[name]; ok && g != w || !ok && !isNotes(name) {
					t.Errorf("%s differs between the copy and Helm", name)
				}
			}
			if len(got) == 0 {
				t.Fatal("nothing rendered")
			}
		})
	}
}

func isNotes(name string) bool { return len(name) >= 9 && name[len(name)-9:] == "NOTES.txt" }

// The hook sees the engine's finished map and what it replaces is what every
// template calls.
func TestFuncsHookReplacesAFunctionForEveryTemplate(t *testing.T) {
	want, _, run := render(t)
	seen := false
	got := run(engine.Engine{Funcs: func(f template.FuncMap) {
		seen = f["quote"] != nil && f["include"] != nil && f["tpl"] != nil
		f["quote"] = func(any) string { return "REPLACED" }
	}})
	if !seen {
		t.Fatal("hook did not see the engine's map with include and tpl bound")
	}
	if maps.EqualFunc(want, got, func(a, b string) bool { return a == b }) {
		t.Fatal("replacing quote changed no output: the hook is not applied")
	}
}
