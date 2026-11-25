package podtext

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk/podtext/podtexttest"
)

func TestKubeletModel(t *testing.T) {
	env := map[string]string{"X": "value"}
	for in, want := range map[string]string{"$(X)": "value", "$(Y)": "$(Y)", "$$(X)": "$(X)", "$$": "$", "a$": "a$", "$": "$", "$(X": "$(X", "$x": "$x", "$$$(X)": "$value"} {
		if got := podtexttest.Expand(in, env); got != want {
			t.Errorf("Expand(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLiteralReachesProcessByteIdentical(t *testing.T) {
	fragments := []string{"$", "$$", "$(X)", "$(Y)", "$$(X)", "(", ")", "X", "\n", "\r", "é", "\xff", "\x00", "DO $$ BEGIN END $$;", "$(date)", " "}
	random := rand.New(rand.NewSource(1))
	text := func() string {
		var out strings.Builder
		for range random.Intn(12) {
			out.WriteString(fragments[random.Intn(len(fragments))])
		}
		return out.String()
	}
	for range 20_000 {
		value, arg := text(), text()
		container := corev1.Container{Command: []string{"run", arg}, Args: []string{value}, Env: []corev1.EnvVar{{Name: "X", Value: "defined"}, {Name: "T", Value: value}},
			StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{arg}}}}}
		spec := &corev1.PodSpec{Containers: []corev1.Container{container}, InitContainers: []corev1.Container{container}}
		Literal(spec)
		for _, got := range append(spec.Containers, spec.InitContainers...) {
			env, argv := podtexttest.Process(got)
			if env["T"] != value || !slices.Equal(argv, []string{"run", arg, value}) || podtexttest.Expand(got.StartupProbe.Exec.Command[0], env) != arg {
				t.Fatalf("value %q arg %q became env %q argv %q", value, arg, env["T"], argv)
			}
		}
		if container.Env[1].Value != value || container.Command[1] != arg || container.StartupProbe.Exec.Command[0] != arg {
			t.Fatal("Literal changed the caller's container")
		}
	}
}

func TestCheckRefusesUndeliverableText(t *testing.T) {
	for _, text := range []string{"\xff", "a\x00b", strings.Repeat("a", MaxBytes+1)} {
		if Check(text) == nil {
			t.Errorf("accepted %.10q", text)
		}
	}
	if err := Check("DO $$ é\n$(X) $"); err != nil {
		t.Fatal(err)
	}
}

// Every file that builds a Pod spec makes its text literal, or is listed with
// the reason it needs not.
//
//ci:whole-tree
func TestEveryAuthoredPodSpecIsLiteral(t *testing.T) {
	exempt := map[string]string{
		"core/pod_admission.go":                 "dry-run admission copy of a template; no process runs",
		"sdk/scoped.go":                         "projection of an observed Pod",
		"telemetry/jaeger/recipe/recipe.go":     "constant text without $",
		"telemetry/prometheus/recipe/recipe.go": "constant text without $",
		"toolchain/toolchain.go":                "adds an init container to a spec its caller makes literal",
		"install/evidence_sink.go":              "adds an init container to a spec its caller makes literal",
		"worker_registry/api_history.go":        "adds a sidecar to a spec its caller makes literal",
	}
	used := map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir("../..", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.Contains(path, "/testdata/") {
			return err
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		// A function that builds a spec calls podtext itself: one file can hold several builders.
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			builds, literal := false, false
			ast.Inspect(fn, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.CompositeLit:
					if sel, ok := n.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "Container" && len(n.Elts) > 0 {
						builds = true
					}
					if array, ok := n.Type.(*ast.ArrayType); ok && len(n.Elts) > 0 {
						if sel, ok := array.Elt.(*ast.SelectorExpr); ok && sel.Sel.Name == "Container" {
							if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "corev1" {
								builds = true
							}
						}
					}
				case *ast.SelectorExpr:
					if pkg, ok := n.X.(*ast.Ident); ok && pkg.Name == "podtext" {
						literal = true
					}
				}
				return true
			})
			name := filepath.ToSlash(strings.TrimPrefix(path, "../../"))
			if builds && !literal {
				if exempt[name] == "" {
					t.Errorf("%s: %s builds containers without podtext.Literal: the kubelet rewrites $$ and $(NAME) in their text", fset.Position(fn.Pos()), fn.Name.Name)
				}
				used[name] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name := range exempt {
		if !used[name] {
			t.Errorf("stale exemption %s", name)
		}
	}
}
