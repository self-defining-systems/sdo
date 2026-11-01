// Package determinism ratchets direct nondeterministic APIs across Go modules.
// Like the functional-core gate, it inspects syntax, including renamed imports
// and function values, without trusting build selection or generated markers.
package determinism

import (
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"
)

// Edges are exact repository-relative package directories, never subtree globs.
// Existing mixed orchestration packages remain on the ratchet, not exempted.
var Edges = map[string]bool{
	"sdo/operator/runtime/internal/h2testio":        true, // In-memory HTTP/2 conformance peer configuration; tests only
	"sdo/operator/memory/internal/memoryio":         true, // Git and shell syntax subprocesses
	"sdo/operator/install/internal/installio":       true, // Repository processes, configuration environment and timer adapter
	"sdo/operator/runtime/internal/lifecycleio":     true, // Kubernetes driver
	"sdo/operator/runtime/prober":                   true, // protocol dial adapter
	"sdo/operator/runtime/prometheus":               true, // Prometheus HTTP adapter
	"sdo/operator/memoryhost/hostio":                true, // Memory-host process launcher, Unix socket and read-only network adapter
	"sdo/operator/toolchain":                        true, // Go compiler process adapter
	"sdo/operator/devmount":                         true, // development mount process adapter
	"sdo/operator/runtime/sim":                      true, // simulation world: virtual time inside synctest bubbles, seed replay, in-memory transport; tests only
	"sdo/operator/deployment/internal/deploymentio": true, // Deployment process, socket and wait adapter

	"sdo/operator/core/internal/kubeio":           true, // Kubernetes configuration and default clock
	"sdo/operator/sdk/internal/clockio":           true, // SDK default wall clock
	"sdo/operator/builder/internal/buildio":       true, // Compiler, cache seeding and controller launcher
	"sdo/operator/operatorcli/internal/processio": true, // CLI Git, kubectl and environment adapter
	"sdo/operator/broker/internal/processio":      true, // Git and bounded validator process adapter
}

func integrationOnly(file *ast.File) (bool, error) {
	for _, group := range file.Comments {
		if group.Pos() > file.Package {
			continue
		}
		for _, comment := range group.List {
			if !strings.HasPrefix(comment.Text, "//go:build ") {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err != nil {
				return false, err
			}
			// Is the expression unsatisfiable with integration=false? Other tags can
			// be anything: integration || linux must not exempt a default Linux test.
			var possible func(constraint.Expr, bool) bool
			possible = func(e constraint.Expr, want bool) bool {
				switch e := e.(type) {
				case *constraint.TagExpr:
					return e.Tag != "integration" || !want
				case *constraint.NotExpr:
					return possible(e.X, !want)
				case *constraint.AndExpr:
					if want {
						return possible(e.X, true) && possible(e.Y, true)
					}
					return possible(e.X, false) || possible(e.Y, false)
				case *constraint.OrExpr:
					if want {
						return possible(e.X, true) || possible(e.Y, true)
					}
					return possible(e.X, false) && possible(e.Y, false)
				}
				return true
			}
			return !possible(expr, true), nil
		}
	}
	return false, nil
}

// Inspect returns stable file:symbol keys. References are checked, not just
// calls, so aliasing a forbidden function does not escape the gate.
func Inspect(filename string, file *ast.File) ([]string, error) {
	test := strings.HasSuffix(filename, "_test.go")
	if test {
		exempt, err := integrationOnly(file)
		if err != nil {
			return nil, err
		}
		if exempt {
			return nil, nil
		}
	} else if Edges[path.Dir(filename)] {
		return nil, nil
	}
	imports := map[string]string{}
	found := map[string]bool{}
	add := func(symbol string) { found[filename+":"+symbol] = true }
	for _, spec := range file.Imports {
		pkg, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		name := path.Base(pkg)
		if pkg == "math/rand/v2" {
			name = "rand"
		}
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = pkg
		if name == "." {
			add("dot-import:" + pkg)
		}
		if pkg == "os/exec" {
			add(pkg)
		}
	}
	// Time APIs inside an inline synctest bubble use virtual time. Other I/O
	// remains forbidden there. Record lexical ranges, never source text.
	type span struct{ start, end token.Pos }
	var virtual []span
	if test {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || id.Obj != nil || imports[id.Name] != "testing/synctest" || !contains(sel.Sel.Name, "Test Run") {
				return true
			}
			for _, arg := range call.Args {
				if fn, ok := arg.(*ast.FuncLit); ok {
					virtual = append(virtual, span{fn.Pos(), fn.End()})
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		pkg, ok := imports[id.Name]
		// Parser object resolution distinguishes a locally shadowed import name.
		if !ok || id.Obj != nil {
			return true
		}
		name := sel.Sel.Name
		forbidden := false
		switch pkg {
		case "time":
			for _, bubble := range virtual {
				if sel.Pos() >= bubble.start && sel.End() <= bubble.end {
					return true
				}
			}
			forbidden = contains(name, "Now Since Until Sleep After AfterFunc Tick NewTimer NewTicker")
		case "math/rand", "math/rand/v2":
			// Constructing an explicitly seeded, injected RNG is deterministic.
			forbidden = !contains(name, "New NewSource NewPCG NewChaCha8 Rand Source Source64 Zipf NewZipf")
		case "crypto/rand":
			forbidden = true
		case "os":
			forbidden = contains(name, "Getenv LookupEnv Environ ExpandEnv StartProcess")
		case "syscall":
			forbidden = contains(name, "Getenv Environ Socket Socketpair Connect Bind Listen Accept Accept4 ForkExec Exec StartProcess")
		case "crypto/tls":
			forbidden = contains(name, "Dial DialWithDialer Listen Dialer")
		case "net":
			forbidden = strings.HasPrefix(name, "Dial") || strings.HasPrefix(name, "Listen") || strings.HasPrefix(name, "Resolve")
		case "net/http":
			forbidden = contains(name, "Client Transport DefaultClient DefaultTransport Get Head Post PostForm NewServeMux Server ListenAndServe ListenAndServeTLS Serve ServeTLS NewFileTransport ProxyFromEnvironment")
		case "net/http/httptest":
			forbidden = contains(name, "NewServer NewTLSServer NewUnstartedServer Server")
		}
		if forbidden {
			add(pkg + "." + name)
		}
		return true
	})
	result := make([]string, 0, len(found))
	for key := range found {
		result = append(result, key)
	}
	sort.Strings(result)
	return result, nil
}

func contains(name, words string) bool {
	for _, word := range strings.Fields(words) {
		if name == word {
			return true
		}
	}
	return false
}

// Compare rejects new violations, stale allowances, and allowances added since
// the committed base, even when a developer regenerates and stages the list.
func Compare(actual, allowed, committed []string) error {
	set := func(lines []string) map[string]bool {
		result := map[string]bool{}
		for _, line := range lines {
			if line != "" && !strings.HasPrefix(line, "#") {
				result[line] = true
			}
		}
		return result
	}
	a, b, c := set(actual), set(allowed), set(committed)
	var errors []string
	for key := range a {
		if !b[key] {
			errors = append(errors, "new violation: "+key)
		}
	}
	for key := range b {
		if !a[key] {
			errors = append(errors, "remove fixed allowance: "+key)
		}
		if !c[key] {
			errors = append(errors, "allowlist may only shrink: "+key)
		}
	}
	sort.Strings(errors)
	if len(errors) > 0 {
		return fmt.Errorf("%s", strings.Join(errors, "\n"))
	}
	return nil
}
