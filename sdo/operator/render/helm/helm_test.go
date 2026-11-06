package helm_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/helm"
	"sdo.dev/operator/render/helm/internal/policy"
	"sdo.dev/operator/render/rendertest"
)

func catalog(t *testing.T) *render.Catalog {
	t.Helper()
	c := render.NewCatalog()
	if err := helm.Register(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func cfg(path string, valueFiles ...string) *contracts.HelmRenderConfig {
	return &contracts.HelmRenderConfig{ChartPath: path, ValueFiles: valueFiles}
}

// fixture loads a testdata chart into a MapFS under prefix.
func fixture(t *testing.T, name, prefix string) rendertest.MapFS {
	t.Helper()
	out := rendertest.MapFS{}
	root := filepath.Join("testdata", name)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(p)
		rel, _ := filepath.Rel(root, p)
		out[prefix+"/"+filepath.ToSlash(rel)] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func with(m rendertest.MapFS, files map[string]string) rendertest.MapFS {
	out := rendertest.MapFS{}
	for k, v := range m {
		out[k] = v
	}
	for k, v := range files {
		out[k] = v
	}
	return out
}

// minimal is a one-ConfigMap chart that negative fixtures extend.
func minimal(files map[string]string) rendertest.MapFS {
	return with(rendertest.MapFS{
		"c/Chart.yaml":        "apiVersion: v2\nname: c\nversion: 0.1.0\n",
		"c/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\n",
	}, files)
}

func tgz(t *testing.T, files map[string]string) string {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestConformance(t *testing.T) {
	env := render.Env{Namespace: "demo", Release: "rel"}
	app := fixture(t, "app", "chart")
	appWant := []string{"Deployment demo/rel-app", "Secret /rel-app-creds", "Service /rel-db"}
	rendertest.TestRenderer(t, catalog(t), helm.ID, []rendertest.Case{
		{Name: "chart with helpers, vendored subchart, schema, helmignore", FS: app, Env: env, Config: cfg("chart"), Want: appWant},
		{
			Name: "value files merge in order, inline wins", FS: app, Env: env,
			Config: &contracts.HelmRenderConfig{
				ChartPath: "chart", ValueFiles: []string{"chart/values-prod.yaml"},
				Values: mustStruct(t, map[string]any{"replicas": 3}),
			},
			Want: appWant,
		},
		{
			Name: "vendored subchart as a tgz",
			FS: minimal(map[string]string{
				"c/Chart.yaml": "apiVersion: v2\nname: c\nversion: 0.1.0\ndependencies:\n- {name: sub, version: 0.1.0, repository: 'https://charts.invalid/x'}\n",
				"c/charts/sub-0.1.0.tgz": tgz(t, map[string]string{
					"sub/Chart.yaml":        "apiVersion: v2\nname: sub\nversion: 0.1.0\n",
					"sub/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: from-sub}\n",
				}),
			}),
			Env: env, Config: cfg("c"),
			Want: []string{"ConfigMap /cm", "ConfigMap /from-sub"},
		},
		{
			Name: "pre-install and post-install hooks are ephemeral phases",
			FS:   fixture(t, "hooks", "h"), Env: env, Config: cfg("h"),
			Want: []string{"ConfigMap /main-cm"},
		},
		{Name: "missing chart", FS: rendertest.MapFS{}, Env: env, Config: cfg("nope"), WantErr: "helm chart directory nope"},
		{Name: "chart_path escapes", FS: app, Env: env, Config: cfg("../x"), WantErr: "relative path inside the source tree"},
		{Name: "chart_path empty", FS: app, Env: env, Config: cfg(""), WantErr: "relative path inside the source tree"},
		{Name: "no release", FS: app, Env: render.Env{Namespace: "demo"}, Config: cfg("chart"), WantErr: "Env.Release"},
		{Name: "missing value file", FS: app, Env: env, Config: cfg("chart", "chart/none.yaml"), WantErr: "none.yaml"},
		{
			Name: "values violate the schema", FS: app, Env: env,
			Config:  &contracts.HelmRenderConfig{ChartPath: "chart", Values: mustStruct(t, map[string]any{"replicas": 0})},
			WantErr: "replicas",
		},
		{
			Name: "file limit", FS: app, Env: env, Config: cfg("chart"),
			Limits: &render.Limits{FileBytes: 30}, WantErr: "limit", WantLimit: true,
		},
	})
}

func renderChart(t *testing.T, fsys rendertest.MapFS, env render.Env, c *contracts.HelmRenderConfig) (render.Result, error) {
	t.Helper()
	packed, err := anypb.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return catalog(t).Render(context.Background(), fsys, env, helm.ID, packed)
}

func TestValueFilesAndInlineValuesAreApplied(t *testing.T) {
	c := &contracts.HelmRenderConfig{
		ChartPath: "chart", ValueFiles: []string{"chart/values-prod.yaml"},
		Values: mustStruct(t, map[string]any{"replicas": 3}),
	}
	res, err := renderChart(t, fixture(t, "app", "chart"), render.Env{Namespace: "n", Release: "r"}, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range res.Objects() {
		if o.GetKind() != "Deployment" {
			continue
		}
		spec := o.Object["spec"].(map[string]any)
		container := spec["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
		if spec["replicas"] != int64(3) {
			t.Errorf("replicas %v: inline values must win over the value file (2) and values.yaml (1)", spec["replicas"])
		}
		if container["image"] != "web:v2" {
			t.Errorf("image %v: value file must override values.yaml", container["image"])
		}
		env := container["env"].([]any)[0].(map[string]any)
		if env["name"] != "MODE" || env["value"] != "prod" {
			t.Errorf("env %v", env)
		}
		return
	}
	t.Fatal("no Deployment")
}

func TestHooksBecomePhases(t *testing.T) {
	res, err := renderChart(t, fixture(t, "hooks", "h"), render.Env{Namespace: "n", Release: "r"}, cfg("h"))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range res.Phases {
		var objs []string
		for _, o := range p.Objects {
			objs = append(objs, o.GetName())
		}
		names = append(names, p.Name+"="+strings.Join(objs, ","))
		if p.Ephemeral != (p.Name != "main") || p.Gate != render.GateReady {
			t.Errorf("phase %s: ephemeral %v gate %v", p.Name, p.Ephemeral, p.Gate)
		}
	}
	// Hooks run by weight: seed (-1) before migrate (5).
	want := "pre-install=seed,migrate main=main-cm post-install=smoke"
	if got := strings.Join(names, " "); got != want {
		t.Fatalf("phases %q, want %q", got, want)
	}
	if got := len(res.Desired()); got != 1 {
		t.Fatalf("Desired has %d objects, want only the main one", got)
	}
}

func TestUnsupportedHookKindsAreRejected(t *testing.T) {
	for _, event := range []string{"pre-upgrade", "post-upgrade", "pre-delete", "post-delete", "pre-rollback", "post-rollback", "test", "pre-install,pre-upgrade"} {
		t.Run(event, func(t *testing.T) {
			fsys := minimal(map[string]string{"c/templates/hook.yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: h\n  annotations: {\"helm.sh/hook\": \"" + event + "\"}\n"})
			_, err := renderChart(t, fsys, render.Env{Namespace: "n", Release: "r"}, cfg("c"))
			if !policy.Violates(err, policy.RuleUnsupportedHook) {
				t.Fatalf("want an unsupported-hook violation, got %v", err)
			}
		})
	}
}

func TestPolicyRejectsUnsafeCharts(t *testing.T) {
	env := render.Env{Namespace: "n", Release: "r"}
	tpl := func(body string) rendertest.MapFS {
		return minimal(map[string]string{"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\ndata:\n  v: {{ " + body + " }}\n"})
	}
	cases := map[string]struct {
		fs   rendertest.MapFS
		rule policy.Rule
	}{
		"lookup":               {tpl(`(lookup "v1" "Namespace" "" "kube-system").metadata.name`), policy.RuleForbiddenFunction},
		"now":                  {tpl(`now | date "2006"`), policy.RuleForbiddenFunction},
		"date with a string":   {tpl(`date "2006" "garbage"`), policy.RuleForbiddenFunction},
		"randAlphaNum":         {tpl(`randAlphaNum 8`), policy.RuleForbiddenFunction},
		"uuidv4":               {tpl(`uuidv4`), policy.RuleForbiddenFunction},
		"genCA":                {tpl(`(genCA "x" 1).Cert`), policy.RuleForbiddenFunction},
		"bcrypt":               {tpl(`bcrypt "pw"`), policy.RuleForbiddenFunction},
		"getHostByName":        {tpl(`getHostByName "example.com"`), policy.RuleForbiddenFunction},
		"in a helper":          {minimal(map[string]string{"c/templates/_h.tpl": `{{ define "h" }}{{ randInt 1 5 }}{{ end }}`}), policy.RuleForbiddenFunction},
		"in a nested pipeline": {tpl(`list 1 2 | join (toString (randInt 1 3))`), policy.RuleForbiddenFunction},
		"inside if/with/range": {minimal(map[string]string{"c/templates/x.yaml": "{{ if .Values.a }}{{ with .Values.b }}{{ range .Values.c }}{{ uuidv4 }}{{ end }}{{ end }}{{ end }}"}), policy.RuleForbiddenFunction},
		"in an else branch":    {minimal(map[string]string{"c/templates/x.yaml": "{{ if .Values.a }}a{{ else }}{{ shuffle (list 1 2) }}{{ end }}"}), policy.RuleForbiddenFunction},
		"in a subchart": {minimal(map[string]string{
			"c/Chart.yaml":                "apiVersion: v2\nname: c\nversion: 0.1.0\ndependencies:\n- {name: s, version: 0.1.0, repository: 'x'}\n",
			"c/charts/s/Chart.yaml":       "apiVersion: v2\nname: s\nversion: 0.1.0\n",
			"c/charts/s/templates/a.yaml": "{{ genPrivateKey \"rsa\" }}",
		}), policy.RuleForbiddenFunction},
		"dependency not vendored": {minimal(map[string]string{"c/Chart.yaml": "apiVersion: v2\nname: c\nversion: 0.1.0\ndependencies:\n- {name: redis, version: 1.0.0, repository: 'https://charts.invalid/x'}\n"}), policy.RuleDependencyNotVendored},
		"crds directory":          {minimal(map[string]string{"c/crds/crd.yaml": "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: x.example.com}\n"}), policy.RuleCRDs},
		"template syntax":         {tpl(`.Values.a | (`), policy.RuleTemplateSyntax},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := renderChart(t, tc.fs, env, cfg("c"))
			if !policy.Violates(err, tc.rule) {
				t.Fatalf("want a %s violation, got %v", tc.rule, err)
			}
			var perr *policy.Error
			if !errors.As(err, &perr) || perr.Chart == "" {
				t.Fatalf("violation is not a typed policy.Error naming the chart: %v", err)
			}
		})
	}
}

func TestEveryForbiddenFunctionIsRejected(t *testing.T) {
	for _, fn := range []string{"lookup", "now", "date", "dateInZone", "htmlDate", "ago", "randAlpha", "randAlphaNum", "randNumeric", "randAscii", "randInt", "randBytes", "uuidv4", "genCA", "genCAWithKey", "genPrivateKey", "genSelfSignedCert", "genSelfSignedCertWithKey", "genSignedCert", "genSignedCertWithKey", "htpasswd", "bcrypt", "encryptAES", "getHostByName", "shuffle"} {
		fsys := minimal(map[string]string{"c/templates/x.yaml": "{{ " + fn + " }}"})
		_, err := renderChart(t, fsys, render.Env{Namespace: "n", Release: "r"}, cfg("c"))
		if !policy.Violates(err, policy.RuleForbiddenFunction) {
			t.Errorf("%s: want a forbidden-function violation, got %v", fn, err)
		}
	}
}

func TestCapabilitiesDefaultAndOverride(t *testing.T) {
	version := func(env render.Env) string {
		t.Helper()
		res, err := renderChart(t, fixture(t, "app", "chart"), env, cfg("chart"))
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range res.Objects() {
			if o.GetKind() == "Deployment" {
				return o.GetLabels()["kube"]
			}
		}
		t.Fatal("no Deployment")
		return ""
	}
	base := render.Env{Namespace: "n", Release: "r"}
	if got := version(base); got != "v1.30.0" {
		t.Fatalf("default kube version %q", got)
	}
	base.KubeVersion = "v1.28.3"
	if got := version(base); got != "v1.28.3" {
		t.Fatalf("overridden kube version %q", got)
	}
	base.KubeVersion = "not-a-version"
	if _, err := renderChart(t, fixture(t, "app", "chart"), base, cfg("chart")); err == nil {
		t.Fatal("an invalid kube version must fail")
	}
}

func TestSecretsOmitDropsDataIncludeKeepsIt(t *testing.T) {
	c := &contracts.HelmRenderConfig{ChartPath: "chart", Values: mustStruct(t, map[string]any{"token": "hunter2"})}
	for mode, wantData := range map[render.SecretMode]bool{render.SecretsOmit: false, render.SecretsInclude: true} {
		res, err := renderChart(t, fixture(t, "app", "chart"), render.Env{Namespace: "n", Release: "r", Secrets: mode}, c)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, o := range res.Objects() {
			if o.GetKind() == "Secret" {
				_, has := o.Object["stringData"]
				found = true
				if has != wantData {
					t.Errorf("mode %v: Secret stringData present %v, want %v", mode, has, wantData)
				}
			}
		}
		if !found {
			t.Fatal("no Secret rendered")
		}
	}
}

func TestInputsRecordSchemaProbeAndSkipIgnoredFiles(t *testing.T) {
	res, err := renderChart(t, minimal(nil), render.Env{Namespace: "n", Release: "r"}, cfg("c"))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]render.InputKind{}
	for _, in := range res.Inputs {
		kinds[in.Path] = in.Kind
	}
	if kinds["c/values.schema.json"] != render.InputAbsent || kinds["c/Chart.yaml"] != render.InputRead || kinds["c/templates/cm.yaml"] != render.InputRead {
		t.Fatalf("inputs %v", kinds)
	}

	res, err = renderChart(t, fixture(t, "app", "chart"), render.Env{Namespace: "n", Release: "r"}, cfg("chart"))
	if err != nil {
		t.Fatal(err)
	}
	for _, in := range res.Inputs {
		if in.Path == "chart/templates/skipped.yaml" {
			t.Errorf(".helmignore'd file was read: %v", in)
		}
	}
	for _, o := range res.Objects() {
		if o.GetName() == "ignored" {
			t.Error(".helmignore'd template rendered")
		}
	}
}
