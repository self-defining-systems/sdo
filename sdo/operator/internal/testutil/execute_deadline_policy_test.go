package testutil

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// durationUnits are the time package constants a deadline literal is built from.
var durationUnits = map[string]time.Duration{
	"Nanosecond": time.Nanosecond, "Microsecond": time.Microsecond, "Millisecond": time.Millisecond,
	"Second": time.Second, "Minute": time.Minute, "Hour": time.Hour,
}

// literalDuration evaluates `time.Unit` and `N * time.Unit` (either order). Other
// expressions, such as a variable, are not literals and report false.
func literalDuration(expr ast.Expr) (time.Duration, bool) {
	switch value := expr.(type) {
	case *ast.ParenExpr:
		return literalDuration(value.X)
	case *ast.SelectorExpr:
		pkg, ok := value.X.(*ast.Ident)
		unit, known := durationUnits[value.Sel.Name]
		return unit, ok && pkg.Name == "time" && known
	case *ast.BinaryExpr:
		if value.Op != token.MUL {
			return 0, false
		}
		for _, pair := range [][2]ast.Expr{{value.X, value.Y}, {value.Y, value.X}} {
			count, ok := pair[0].(*ast.BasicLit)
			unit, isUnit := literalDuration(pair[1])
			if ok && isUnit && count.Kind == token.INT {
				if n, err := strconv.Atoi(count.Value); err == nil {
					return time.Duration(n) * unit, true
				}
			}
		}
	}
	return 0, false
}

// shortExecuteDeadlines finds `ctx, _ := context.WithTimeout(_, <literal under a second>)`
// (or WithDeadline of time.Now().Add(<literal>)) whose ctx is passed to an Execute call in
// the same function. Execute polls until its context ends, so a fixed short deadline races
// it against the scheduler: the race detector under load can outlast the deadline.
func shortExecuteDeadlines(file *ast.File) []token.Pos {
	var found []token.Pos
	scan := func(body *ast.BlockStmt) {
		deadlines := map[string]token.Pos{}
		ast.Inspect(body, func(node ast.Node) bool {
			assign, ok := node.(*ast.AssignStmt)
			if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) == 0 {
				return true
			}
			call, ok := assign.Rhs[0].(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			fn, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if pkg, ok := fn.X.(*ast.Ident); !ok || pkg.Name != "context" {
				return true
			}
			limit := call.Args[1]
			switch fn.Sel.Name {
			case "WithTimeout":
			case "WithDeadline":
				// time.Now().Add(<literal>)
				add, ok := limit.(*ast.CallExpr)
				if !ok || len(add.Args) != 1 {
					return true
				}
				limit = add.Args[0]
			default:
				return true
			}
			if d, literal := literalDuration(limit); literal && d < time.Second {
				if name, ok := assign.Lhs[0].(*ast.Ident); ok {
					deadlines[name.Name] = call.Pos()
				}
			}
			return true
		})
		if len(deadlines) == 0 {
			return
		}
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if fn, ok := call.Fun.(*ast.SelectorExpr); !ok || fn.Sel.Name != "Execute" {
				return true
			}
			for _, arg := range call.Args {
				if name, ok := arg.(*ast.Ident); ok {
					if pos, short := deadlines[name.Name]; short {
						found = append(found, pos)
						delete(deadlines, name.Name)
					}
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if decl, ok := node.(*ast.FuncDecl); ok && decl.Body != nil {
			scan(decl.Body)
			return false
		}
		return true
	})
	return found
}

func parseSource(t *testing.T, source string) *ast.File {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestExecuteDeadlinePolicyNegativeControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond); defer cancel(); e.Execute(ctx, task) }`,
		`package p; func f(){ ctx, cancel := context.WithTimeout(bg, time.Millisecond); _, _ = bound.Execute(ctx, task); cancel() }`,
		`package p; func f(){ ctx, c := context.WithDeadline(bg, time.Now().Add(999*time.Millisecond)); go func(){ e.Execute(ctx, t) }(); c() }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, (50*time.Microsecond)); e.Execute(ctx, t); c() }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, time.Millisecond*5); e.Execute(ctx, t); c() }`,
	} {
		if len(shortExecuteDeadlines(parseSource(t, source))) == 0 {
			t.Errorf("short Execute deadline passed: %s", source)
		}
	}
	for _, source := range []string{
		`package p; func f(){ ctx, c := context.WithTimeout(bg, 30*time.Second); defer c(); e.Execute(ctx, task) }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, time.Second); defer c(); e.Execute(ctx, task) }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, 50*time.Millisecond); defer c(); <-ctx.Done() }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, 50*time.Millisecond); defer c(); other.Run(ctx) }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, limit); defer c(); e.Execute(ctx, task) }`,
		`package p; func f(){ ctx, c := context.WithTimeout(bg, 50*time.Millisecond); c(); e.Execute(context.Background(), task) }`,
	} {
		if found := shortExecuteDeadlines(parseSource(t, source)); len(found) != 0 {
			t.Errorf("safe fixture rejected: %s", source)
		}
	}
}

func TestNoFixedShortDeadlineAroundExecuteInTests(t *testing.T) {
	operator := filepath.Join("..", "..")
	err := filepath.WalkDir(operator, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && path != operator && (entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".")) {
			return fs.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, nil, 0)
		if err != nil {
			return err
		}
		for _, pos := range shortExecuteDeadlines(file) {
			t.Errorf("%s: Execute runs under a fixed sub-second deadline; wait for the condition the test asserts and keep only a generous ceiling (worker_registry executeUntil)", positions.Position(pos))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
