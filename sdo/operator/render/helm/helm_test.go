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
	"slices"
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
		{
			Name: "no release names the release after the namespace", FS: app, Env: render.Env{Namespace: "demo"}, Config: cfg("chart"),
			Want: []string{"Deployment demo/demo-app", "Secret /demo-app-creds", "Service /demo-db"},
		},
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

func TestPolicyRejectsUnsafeCharts(t *testing.T) {
	env := render.Env{Namespace: "n", Release: "r"}
	tpl := func(body string) rendertest.MapFS {
		return minimal(map[string]string{"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: x\ndata:\n  v: {{ " + body + " }}\n"})
	}
	cases := map[string]struct {
		fs   rendertest.MapFS
		rule policy.Rule
	}{
		"lookup":             {tpl(`(lookup "v1" "Namespace" "" "kube-system").metadata.name`), policy.RuleForbiddenFunction},
		"now":                {tpl(`now | date "2006"`), policy.RuleForbiddenFunction},
		"date with a string": {tpl(`date "2006" "garbage"`), policy.RuleForbiddenFunction},
		"randAlphaNum":       {tpl(`randAlphaNum 8`), policy.RuleForbiddenFunction},
		"uuidv4":             {tpl(`uuidv4`), policy.RuleForbiddenFunction},
		"genCA":              {tpl(`(genCA "x" 1).Cert`), policy.RuleForbiddenFunction},
		"bcrypt":             {tpl(`bcrypt "pw"`), policy.RuleForbiddenFunction},
		// Bypasses of the former name denylist (review-2 B2).
		"date_in_zone alias": {tpl(`date_in_zone "2006-01-02T15:04:05" "x" "UTC"`), policy.RuleForbiddenFunction},
		"tpl over a literal": {tpl(`tpl "{{ randAlphaNum 8 }}" .`), policy.RuleForbiddenFunction},
		"tpl over values": {minimal(map[string]string{"c/values.yaml": "s: \"{{ now }}\"\n",
			"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: {{ tpl .Values.s . }}\n"}), policy.RuleForbiddenFunction},
		"computed include of a forbidden helper": {minimal(map[string]string{
			"c/templates/_h.tpl": `{{ define "h" }}{{ randInt 1 5 }}{{ end }}`,
			"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: {{ include (printf \"%s\" \"h\") . }}\n"}), policy.RuleForbiddenFunction},
		"env":       {tpl(`env "HOME"`), policy.RuleForbiddenFunction},
		"expandenv": {tpl(`expandenv "$HOME"`), policy.RuleForbiddenFunction},
		"unixEpoch": {tpl(`now | unixEpoch`), policy.RuleForbiddenFunction},
		"tpl reaching a helper": {minimal(map[string]string{
			"c/values.yaml":      "s: '{{ include \"h\" . }}'\n",
			"c/templates/_h.tpl": `{{ define "h" }}{{ uuidv4 }}{{ end }}`,
			"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: {{ tpl .Values.s . }}\n"}), policy.RuleForbiddenFunction},
		"include of an output file": {minimal(map[string]string{
			"c/templates/hidden.yaml": "{{ randAlphaNum 4 | printf \"\" }}",
			"c/templates/x.yaml":      "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: {{ include \"c/templates/hidden.yaml\" . }}\n"}), policy.RuleForbiddenFunction},
		"getHostByName": {tpl(`getHostByName "example.com"`), policy.RuleForbiddenFunction},
		"in a used helper": {minimal(map[string]string{"c/templates/_h.tpl": `{{ define "h" }}{{ randInt 1 5 }}{{ end }}`,
			"c/templates/x.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: {{ include \"h\" . }}\n"}), policy.RuleForbiddenFunction},
		"in a nested pipeline": {tpl(`list 1 2 | join (toString (randInt 1 3))`), policy.RuleForbiddenFunction},
		"inside if/with/range": {minimal(map[string]string{"c/values.yaml": "a: true\nb: {k: 1}\nc: [1]\n", "c/templates/x.yaml": "{{ if .Values.a }}{{ with .Values.b }}{{ range $.Values.c }}{{ uuidv4 }}{{ end }}{{ end }}{{ end }}"}), policy.RuleForbiddenFunction},
		"in an else branch":    {minimal(map[string]string{"c/values.yaml": "a: false\n", "c/templates/x.yaml": "{{ if .Values.a }}a{{ else }}{{ shuffle (list 1 2) }}{{ end }}"}), policy.RuleForbiddenFunction},
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
			if tc.rule == policy.RuleForbiddenFunction && (perr.File == "" || !strings.Contains(perr.Detail, "template function")) {
				t.Fatalf("a function violation must name the function and the template: %+v", perr)
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

func TestUnknownAPIVersionsFollowTheKubeVersion(t *testing.T) {
	// Charts pick the Ingress and PodDisruptionBudget API from capabilities.
	tpl := `{{ range $gv := list "extensions/v1beta1" "networking.k8s.io/v1beta1" "networking.k8s.io/v1" "policy/v1beta1" "policy/v1" "batch/v1beta1" }}{{ if $.Capabilities.APIVersions.Has $gv }}{{ $gv }} {{ end }}{{ end }}`
	fsys := minimal(map[string]string{"c/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: cm}\ndata:\n  apis: \"" + tpl + "\"\n"})
	apis := func(env render.Env) string {
		t.Helper()
		res, err := renderChart(t, fsys, env, cfg("c"))
		if err != nil {
			t.Fatal(err)
		}
		v, _, _ := unstructuredNestedString(res.Objects()[0].Object, "data", "apis")
		return strings.TrimSpace(v)
	}
	base := render.Env{Namespace: "n", Release: "r"}
	if got := apis(base); got != "networking.k8s.io/v1 policy/v1" {
		t.Fatalf("default (v1.30) serves %q", got)
	}
	// Negative control: a cluster that still serves them keeps the old APIs.
	base.KubeVersion = "v1.21.0"
	if got := apis(base); got != "extensions/v1beta1 networking.k8s.io/v1beta1 networking.k8s.io/v1 policy/v1beta1 policy/v1 batch/v1beta1" {
		t.Fatalf("v1.21 serves %q", got)
	}
	base.KubeVersion = "v1.24.0"
	if got := apis(base); got != "networking.k8s.io/v1 policy/v1beta1 policy/v1 batch/v1beta1" {
		t.Fatalf("v1.24 serves %q", got)
	}
	// An explicit set is used as given.
	base.APIVersions = []string{"v1", "extensions/v1beta1"}
	if got := apis(base); got != "extensions/v1beta1" {
		t.Fatalf("explicit set gave %q", got)
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

func objectNames(res render.Result) []string {
	var out []string
	for _, p := range res.Phases {
		for _, o := range p.Objects {
			out = append(out, p.Name+"/"+o.GetName())
		}
	}
	return out
}

// subchart is a vendored subchart with one ConfigMap, a CRD, and a template
// that calls a forbidden function: everything policy must not see while the
// subchart is disabled.
func subchart(name string) map[string]string {
	return map[string]string{
		"c/charts/" + name + "/Chart.yaml":          "apiVersion: v2\nname: " + name + "\nversion: 0.1.0\n",
		"c/charts/" + name + "/values.yaml":         "exported: {key: from-" + name + "}\n",
		"c/charts/" + name + "/crds/crd.yaml":       "apiVersion: apiextensions.k8s.io/v1\nkind: CustomResourceDefinition\nmetadata: {name: x.example.com}\n",
		"c/charts/" + name + "/templates/cm.yaml":   "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "-cm}\n",
		"c/charts/" + name + "/templates/_rnd.tpl":  `{{ define "` + name + `.rnd" }}{{ randAlphaNum 4 }}{{ end }}`,
		"c/charts/" + name + "/templates/lkup.yaml": "{{ lookup \"v1\" \"Namespace\" \"\" \"x\" }}",
	}
}

func parentChart(deps string, values string, extra ...map[string]string) rendertest.MapFS {
	files := map[string]string{
		"c/Chart.yaml":  "apiVersion: v2\nname: c\nversion: 0.1.0\ndependencies:\n" + deps,
		"c/values.yaml": values,
	}
	for _, e := range extra {
		for k, v := range e {
			files[k] = v
		}
	}
	return minimal(files)
}

func renderWith(t *testing.T, fsys rendertest.MapFS, values map[string]any) (render.Result, error) {
	t.Helper()
	c := cfg("c")
	if values != nil {
		c.Values = mustStruct(t, values)
	}
	return renderChart(t, fsys, render.Env{Namespace: "n", Release: "r"}, c)
}

func TestDisabledSubchartsAreNotPartOfTheReleaseSet(t *testing.T) {
	deps := "- {name: a, version: 0.1.0, repository: 'file://a', condition: a.enabled}\n" +
		"- {name: t, version: 0.1.0, repository: 'file://t', tags: [extra]}\n" +
		"- {name: gone, version: 0.1.0, repository: 'https://charts.invalid/gone', condition: gone.enabled}\n"
	build := func() rendertest.MapFS {
		return parentChart(deps, "a: {enabled: false}\ntags: {extra: false}\ngone: {enabled: false}\n", subchart("a"), subchart("t"))
	}
	res, err := renderWith(t, build(), nil)
	if err != nil {
		t.Fatalf("disabled subcharts must not be checked or rendered: %v", err)
	}
	if got := strings.Join(objectNames(res), " "); got != "main/cm" {
		t.Fatalf("objects %q, want only the parent's", got)
	}

	// Negative controls: each rule fires once the same subchart is enabled.
	enableA := map[string]any{"a": map[string]any{"enabled": true}}
	if _, err := renderWith(t, build(), enableA); !policy.Violates(err, policy.RuleCRDs) {
		t.Fatalf("enabled subchart a must be rejected for its crds, got %v", err)
	}
	noCRDs := build()
	delete(noCRDs, "c/charts/a/crds/crd.yaml")
	if _, err := renderWith(t, noCRDs, enableA); !policy.Violates(err, policy.RuleForbiddenFunction) {
		t.Fatalf("enabled subchart a must be rejected for the lookup it executes, got %v", err)
	}
	if _, err := renderWith(t, build(), map[string]any{"tags": map[string]any{"extra": true}}); !policy.Violates(err, policy.RuleCRDs) {
		t.Fatalf("enabling by tag must put the subchart under policy, got %v", err)
	}
	if _, err := renderWith(t, build(), map[string]any{"gone": map[string]any{"enabled": true}}); !policy.Violates(err, policy.RuleDependencyNotVendored) {
		t.Fatalf("an enabled dependency that is not vendored must be rejected, got %v", err)
	}
}

func TestEnabledSubchartRendersAndImportValuesMerge(t *testing.T) {
	deps := "- {name: a, version: 0.1.0, repository: 'file://a', condition: a.enabled, import-values: [{child: exported, parent: imported}]}\n"
	sub := subchart("a")
	delete(sub, "c/charts/a/crds/crd.yaml")
	delete(sub, "c/charts/a/templates/_rnd.tpl")
	delete(sub, "c/charts/a/templates/lkup.yaml")
	fsys := parentChart(deps, "a: {enabled: true}\n", sub, map[string]string{
		"c/templates/imp.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: imp}\ndata:\n  key: {{ .Values.imported.key }}\n",
	})
	res, err := renderWith(t, fsys, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := objectNames(res)
	slices.Sort(names)
	if got := strings.Join(names, " "); got != "main/a-cm main/cm main/imp" {
		t.Fatalf("objects %q", got)
	}
	for _, o := range res.Objects() {
		if o.GetName() == "imp" {
			if v, _, _ := unstructuredNestedString(o.Object, "data", "key"); v != "from-a" {
				t.Fatalf("import-values did not merge the child's value: %q", v)
			}
		}
	}
	// Negative control: disabling the subchart removes its objects and the import.
	if _, err := renderWith(t, fsys, map[string]any{"a": map[string]any{"enabled": false}}); err == nil {
		t.Fatal("without the subchart the imported value is absent and the template must fail")
	}
}

func unstructuredNestedString(obj map[string]any, fields ...string) (string, bool, error) {
	var cur any = obj
	for _, f := range fields {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false, nil
		}
		cur = m[f]
	}
	s, ok := cur.(string)
	return s, ok, nil
}

func hookTemplate(name, events, body string) map[string]string {
	return map[string]string{"c/templates/" + name + ".yaml": "apiVersion: v1\nkind: Pod\nmetadata:\n  name: " + body + "\n  annotations: {\"helm.sh/hook\": \"" + events + "\"}\n"}
}

func TestHooksAreSelectedByTheInstallEvent(t *testing.T) {
	fsys := minimal(map[string]string{})
	for name, events := range map[string]string{
		"both": "pre-install,pre-upgrade", "upgrade-only": "pre-upgrade", "post-upgrade": "post-upgrade",
		"delete": "pre-delete,post-delete", "rollback": "pre-rollback", "test": "test", "legacy-test": "test-success",
	} {
		fsys = with(fsys, hookTemplate(name, events, name))
	}
	res, err := renderWith(t, fsys, nil)
	if err != nil {
		t.Fatalf("hooks of other operations must not reject the chart: %v", err)
	}
	if got := strings.Join(objectNames(res), " "); got != "pre-install/both main/cm" {
		t.Fatalf("objects %q, want the install hook and the main object only", got)
	}
	// Negative control: an install event on the same template keeps it.
	res, err = renderWith(t, with(fsys, hookTemplate("upgrade-only", "pre-install", "upgrade-only")), nil)
	if err != nil || !strings.Contains(strings.Join(objectNames(res), " "), "pre-install/upgrade-only") {
		t.Fatalf("an install event must keep the hook: %v %v", objectNames(res), err)
	}
}

func TestForbiddenFunctionsAreJudgedByWhatExecutes(t *testing.T) {
	podName := `name-{{ randAlphaNum 5 }}`
	manifest := func(name string) string { return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + name + "}\n" }
	t.Run("a template whose guard is off executes nothing", func(t *testing.T) {
		fsys := minimal(map[string]string{
			"c/values.yaml":      "on: false\n",
			"c/templates/g.yaml": "{{ if .Values.on }}" + manifest("g-{{ randAlphaNum 5 }}") + "{{ end }}",
		})
		if _, err := renderWith(t, fsys, nil); err != nil {
			t.Fatal(err)
		}
		// Negative control: the same template with the guard on is rejected.
		if _, err := renderWith(t, fsys, map[string]any{"on": true}); !policy.Violates(err, policy.RuleForbiddenFunction) {
			t.Fatalf("an executing forbidden function must be rejected, got %v", err)
		}
	})
	t.Run("hooks are executed whatever their events, so randomness in one is rejected", func(t *testing.T) {
		for _, event := range []string{"test", "pre-upgrade", "pre-install"} {
			if _, err := renderWith(t, minimal(hookTemplate("t", event, podName)), nil); !policy.Violates(err, policy.RuleForbiddenFunction) {
				t.Errorf("%s hook: want a forbidden-function violation, got %v", event, err)
			}
		}
	})
	t.Run("a helper nobody calls is not executed", func(t *testing.T) {
		fsys := minimal(map[string]string{"c/templates/_h.tpl": `{{ define "h" }}{{ randAlphaNum 4 }}{{ end }}`})
		if _, err := renderWith(t, fsys, nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("NOTES.txt is discarded, so it is not executed", func(t *testing.T) {
		if _, err := renderWith(t, minimal(map[string]string{"c/templates/NOTES.txt": "{{ randAlphaNum 5 }}"}), nil); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the error names the template that called the function and the file that reached it", func(t *testing.T) {
		fsys := minimal(map[string]string{
			"c/templates/_h.tpl": `{{ define "h" }}{{ uuidv4 }}{{ end }}`,
			"c/templates/x.yaml": manifest(`{{ include "h" . }}`),
		})
		_, err := renderWith(t, fsys, nil)
		var perr *policy.Error
		if !errors.As(err, &perr) || perr.File != "h" || !strings.Contains(perr.Detail, `"uuidv4"`) || !strings.Contains(perr.Detail, "reached from c/templates/x.yaml") {
			t.Fatalf("got %+v (%v)", perr, err)
		}
	})
}

// Real charts compute values with tpl, computed include and duration. Each of
// these is deterministic over the chart, its values and the release, so they
// render; whatever they evaluate still runs under the restricted function map.
func TestDeterministicDynamicTemplatesRender(t *testing.T) {
	cm := func(data string) string {
		return "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: x}\ndata:\n  v: " + data + "\n"
	}
	cases := map[string]struct {
		files map[string]string
		want  string
	}{
		"tpl over values": {map[string]string{
			"c/values.yaml":      "name: web\nhost: '{{ .Values.name }}.{{ .Release.Namespace }}'\n",
			"c/templates/x.yaml": cm(`{{ tpl .Values.host . | quote }}`)}, "web.n"},
		"tpl over a literal": {map[string]string{"c/templates/x.yaml": cm(`{{ tpl "{{ add 1 2 }}" . | quote }}`)}, "3"},
		"computed include": {map[string]string{
			"c/values.yaml":      "kind: db\n",
			"c/templates/_h.tpl": `{{ define "name.db" }}database{{ end }}{{ define "name.web" }}frontend{{ end }}`,
			"c/templates/x.yaml": cm(`{{ include (printf "name.%s" .Values.kind) . | quote }}`)}, "database"},
		"tpl calling a computed include": {map[string]string{
			"c/values.yaml":      "kind: web\ns: '{{ include (printf \"name.%s\" .Values.kind) . }}'\n",
			"c/templates/_h.tpl": `{{ define "name.web" }}frontend{{ end }}`,
			"c/templates/x.yaml": cm(`{{ tpl .Values.s . | quote }}`)}, "frontend"},
		"duration": {map[string]string{"c/templates/x.yaml": cm(`{{ duration (int64 3661) | quote }}`)}, "1h1m1s"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			res, err := renderWith(t, minimal(tc.files), nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, o := range res.Objects() {
				if o.GetKind() == "ConfigMap" {
					if got, _, _ := unstructuredNestedString(o.Object, "data", "v"); got == tc.want {
						return
					}
				}
			}
			t.Fatalf("no ConfigMap with data.v %q in %v", tc.want, objectNames(res))
		})
	}
}
