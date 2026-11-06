package renderers_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/operatorcli/catalog/renderers"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/rendertest"
)

// A chart (or an agent-authored patch to one) is Git content rendered on
// behalf of the controller. Whatever a template does, the production catalog
// must return an error, never crash or hang the calling process: these
// templates exhaust memory or loop without calling a template function, where
// no in-template hook can see them.

func hostileChart(templates map[string]string) rendertest.MapFS {
	fsys := rendertest.MapFS{"c/Chart.yaml": "apiVersion: v2\nname: c\nversion: 0.1.0\n"}
	for name, body := range templates {
		fsys["c/templates/"+name] = body
	}
	return fsys
}

func renderHostile(ctx context.Context, t *testing.T, fsys rendertest.MapFS) error {
	t.Helper()
	config, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "c"})
	if err != nil {
		t.Fatal(err)
	}
	// The render runs on its own goroutine so that a hang fails the test
	// instead of holding the test binary until go test's timeout.
	done := make(chan error, 1)
	go func() {
		_, err := renderers.Catalog().Render(ctx, fsys, render.Env{Namespace: "n"}, "helm", config)
		done <- err
	}()
	watchdog, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("hostile chart rendered without error")
		}
		return err
	case <-watchdog.Done():
		t.Fatal("render still running 8s after it started: the render deadline did not stop it")
		return nil
	}
}

func configMap(value string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: " + value + "\n"
}

// review main-4 F1: values held in template variables are not bounded by any
// per-call or per-buffer cap. The render process runs out of memory, which must
// come back as a render-budget violation (a verdict on the source).
func TestMemoryExhaustingChartsFailTheRenderNotTheProcess(t *testing.T) {
	var fanOut strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&fanOut, `{{ $v%d := include "big" . }}`, i)
	}
	cases := map[string]rendertest.MapFS{
		"string doubling in a variable": hostileChart(map[string]string{
			"x.yaml": configMap(`{{ $s := repeat 1000 "x" }}{{ range until 30 }}{{ $s = printf "%s%s" $s $s }}{{ end }}{{ len $s }}`)}),
		"include results held in variables": hostileChart(map[string]string{
			"_h.tpl": `{{ define "big" }}{{ repeat 1000000 "0123456789abcdef" }}{{ end }}`,
			"x.yaml": configMap(fanOut.String() + `{{ "ok" }}`)}),
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			err := renderHostile(t.Context(), t, fsys)
			if !strings.Contains(err.Error(), "helm policy render-budget") || !strings.Contains(err.Error(), "memory") {
				t.Fatalf("want a render-budget violation naming memory, got %v", err)
			}
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatalf("a memory verdict must not look transient: %v", err)
			}
		})
	}
}

// review main-4 F2: `template` actions and `range` call no template function,
// so the engine's per-call deadline check never runs. The caller's deadline
// must still stop the render.
func TestFunctionFreeLoopsStopAtTheCallersDeadline(t *testing.T) {
	var chain strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&chain, `{{ define "h%d" }}{{ template "h%d" . }}{{ template "h%d" . }}{{ end }}`, i, i+1, i+1)
	}
	chain.WriteString(`{{ define "h40" }}{{ end }}`)
	cases := map[string]rendertest.MapFS{
		"template action fan-out": hostileChart(map[string]string{
			"_h.tpl": chain.String(),
			"x.yaml": configMap(`{{ template "h0" . }}x`)}),
		"nested range over a variable": hostileChart(map[string]string{
			"x.yaml": configMap(`{{ $l := until 1000000 }}{{ range $l }}{{ range $l }}{{ range $l }}{{ end }}{{ end }}{{ end }}x`)}),
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			err := renderHostile(ctx, t, fsys)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want the caller's deadline, got %v", err)
			}
		})
	}
}

// The isolated render is the in-process render: same objects (types
// included), phases, recorded inputs and typed errors, in both Secret modes.
func TestIsolatedHelmMatchesInProcess(t *testing.T) {
	inProcess := render.NewCatalog()
	if err := helm.Register(inProcess, helm.InProcess); err != nil {
		t.Fatal(err)
	}
	chart := rendertest.MapFS{
		"c/Chart.yaml":  "apiVersion: v2\nname: c\nversion: 0.1.0\n",
		"c/values.yaml": "replicas: 1\nratio: 0.5\nlabels: {team: a}\n",
		"c/templates/deploy.yaml": "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: d, labels: {{ toJson .Values.labels }}}\n" +
			"spec:\n  replicas: {{ .Values.replicas }}\n  template:\n    metadata: {annotations: {ratio: {{ .Values.ratio | quote }}}}\n" +
			"    spec: {containers: [{name: c, image: {{ .Values.image | quote }}}]}\n",
		"c/templates/hook.yaml": "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: migrate\n  annotations: {helm.sh/hook: pre-install}\n" +
			"spec: {template: {spec: {restartPolicy: Never, containers: [{name: m, image: m}]}}}\n",
		"c/templates/secret.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: s}\nstringData: {k: {{ .Values.image | quote }}}\n",
		"overrides.yaml":          "replicas: 3\nlabels: {tier: web}\n",
	}
	inline, err := structpb.NewStruct(map[string]any{"image": "registry.example/app:1", "labels": map[string]any{"team": nil}})
	if err != nil {
		t.Fatal(err)
	}
	good, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "c", ValueFiles: []string{"overrides.yaml"}, Values: inline})
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []render.SecretMode{render.SecretsOmit, render.SecretsInclude} {
		env := render.Env{Namespace: "n", KubeVersion: "v1.29.0", Secrets: mode}
		want, err := inProcess.Render(t.Context(), chart, env, "helm", good)
		if err != nil {
			t.Fatal(err)
		}
		got, err := renderers.Catalog().Render(t.Context(), chart, env, "helm", good)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mode %v: isolated render\n%+v\nwant\n%+v", mode, got, want)
		}
		if len(got.Phases) != 2 || len(got.Inputs) == 0 {
			t.Fatalf("fixture lost its hook phase or inputs: %+v", got)
		}
	}

	forbidden := with(chart, "c/templates/now.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: now}\ndata: {t: {{ now | quote }}}\n")
	bad, err := anypb.New(&contracts.HelmRenderConfig{ChartPath: "c"})
	if err != nil {
		t.Fatal(err)
	}
	env := render.Env{Namespace: "n"}
	_, want := inProcess.Render(t.Context(), forbidden, env, "helm", bad)
	_, got := renderers.Catalog().Render(t.Context(), forbidden, env, "helm", bad)
	if want == nil || got == nil || got.Error() != want.Error() || !strings.Contains(got.Error(), "helm policy forbidden-function") {
		t.Fatalf("isolated error %v, in-process error %v", got, want)
	}
}

func with(fsys rendertest.MapFS, name, body string) rendertest.MapFS {
	out := rendertest.MapFS{name: body}
	for k, v := range fsys {
		out[k] = v
	}
	return out
}
