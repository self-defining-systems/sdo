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

// Counted loops with literal sleep budgets below five seconds race CI scheduling.
// Nested loops have their own budget and must not be charged to their parent.
func shortCountedPolls(file *ast.File) []token.Pos {
	var found []token.Pos
	ast.Inspect(file, func(node ast.Node) bool {
		loop, ok := node.(*ast.ForStmt)
		if !ok {
			return true
		}
		init, ok := loop.Init.(*ast.AssignStmt)
		if !ok || len(init.Lhs) != 1 || len(init.Rhs) != 1 {
			return true
		}
		name, ok := init.Lhs[0].(*ast.Ident)
		if !ok {
			return true
		}
		start, ok := integerLiteral(init.Rhs[0])
		if !ok {
			return true
		}
		cond, ok := loop.Cond.(*ast.BinaryExpr)
		if !ok || (cond.Op != token.LSS && cond.Op != token.LEQ) {
			return true
		}
		counter, ok := cond.X.(*ast.Ident)
		if !ok || counter.Name != name.Name {
			return true
		}
		end, ok := integerLiteral(cond.Y)
		if !ok {
			return true
		}
		post, ok := loop.Post.(*ast.IncDecStmt)
		if !ok || post.Tok != token.INC {
			return true
		}
		increment, ok := post.X.(*ast.Ident)
		if !ok || increment.Name != name.Name {
			return true
		}
		count := end - start
		if cond.Op == token.LEQ {
			count++
		}
		if count <= 0 {
			return true
		}
		var sleep time.Duration
		ast.Inspect(loop.Body, func(n ast.Node) bool {
			switch n.(type) {
			case *ast.ForStmt, *ast.RangeStmt, *ast.FuncLit:
				return false
			}
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return true
			}
			fn, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || fn.Sel.Name != "Sleep" {
				return true
			}
			pkg, ok := fn.X.(*ast.Ident)
			if !ok || pkg.Name != "time" {
				return true
			}
			if duration, literal := literalDuration(call.Args[0]); literal && duration > 0 {
				sleep += duration
			}
			return true
		})
		if sleep > 0 && count <= int((5*time.Second-1)/sleep) {
			found = append(found, loop.Pos())
		}
		return true
	})
	return found
}

func integerLiteral(expr ast.Expr) (int, bool) {
	value, ok := expr.(*ast.BasicLit)
	if !ok || value.Kind != token.INT {
		return 0, false
	}
	n, err := strconv.Atoi(value.Value)
	return n, err == nil
}

func TestCountedPollingPolicyNegativeControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ for i:=0;i<200;i++ { if ready(){break};time.Sleep(time.Millisecond) } }`,
		`package p; func f(){ for i:=1;i<=100;i++ { time.Sleep(10*time.Millisecond);if ready(){return} } }`,
		`package p; func f(){ for i:=0;i<3;i++ { time.Sleep(time.Second) } }`,
	} {
		if len(shortCountedPolls(parseSource(t, source))) != 1 {
			t.Errorf("missed small counted sleep budget: %s", source)
		}
	}
	for _, source := range []string{
		`package p; func f(){ for i:=0;i<300;i++ { time.Sleep(100*time.Millisecond);if ready(){break} } }`,
		`package p; func f(){ for i:=0;i<20;i++ { for !ready(){ time.Sleep(time.Millisecond) } } }`,
		`package p; func f(){ for i:=0;i<200;i++ { step() } }`,
		`package p; func f(){ for !ready(){time.Sleep(time.Millisecond)} }`,
	} {
		if len(shortCountedPolls(parseSource(t, source))) != 0 {
			t.Errorf("rejected safe loop: %s", source)
		}
	}
}

// Allowances are local to the loop and always explain deliberate timing.
func pollingAllowed(file *ast.File, positions *token.FileSet, pos token.Pos) bool {
	line := positions.Position(pos).Line
	for _, group := range file.Comments {
		for _, comment := range group.List {
			commentLine := positions.Position(comment.Pos()).Line
			if commentLine != line && commentLine != line-1 {
				continue
			}
			const marker = "test-polling: allow -- "
			if _, reason, ok := strings.Cut(comment.Text, marker); ok && strings.TrimSpace(reason) != "" {
				return true
			}
		}
	}
	return false
}

func TestCountedPollingAllowances(t *testing.T) {
	for _, fixture := range []struct {
		comment string
		allowed bool
	}{
		{"// test-polling: allow -- deliberate event pacing\n", true},
		{"// test-polling: allow -- \n", false},
		{"// test-polling: allow -- deliberate event pacing\n\n", false},
		{"// ordinary comment\n", false},
	} {
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, "fixture.go", "package p\nfunc f(){\n"+fixture.comment+"for i:=0;i<200;i++ { time.Sleep(time.Millisecond) }\n}", parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		polls := shortCountedPolls(file)
		if len(polls) != 1 {
			t.Fatal("fixture must contain one short counted loop")
		}
		if got := pollingAllowed(file, positions, polls[0]); got != fixture.allowed {
			t.Errorf("%q: allowed=%v; want %v", fixture.comment, got, fixture.allowed)
		}
	}
}

func TestNoShortCountedPollingInTests(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && path != root && (entry.Name() == "third_party" || entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".")) {
			return fs.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		for _, pos := range shortCountedPolls(file) {
			allowed := pollingAllowed(file, positions, pos)
			if !allowed {
				t.Errorf("%s: counted sleep budget below 5s; wait for the asserted condition with a generous ceiling, or use test-polling: allow -- <reason> for intentional timing", positions.Position(pos))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
