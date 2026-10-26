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
)

// gitCreation finds repository-creation command arguments in Go syntax. It
// checks direct calls and command vectors, including vectors executed in loops.
// It does not inspect prose, shell snippets, map fields or unrelated init names.
func gitCreation(file *ast.File) []token.Pos {
	var found []token.Pos
	literal := func(e ast.Expr) string {
		if value, ok := e.(*ast.BasicLit); ok && value.Kind == token.STRING {
			text, _ := strconv.Unquote(value.Value)
			return text
		}
		return ""
	}
	creation := func(s string) bool { return s == "init" || s == "clone" }
	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.CompositeLit:
			array, ok := value.Type.(*ast.ArrayType)
			if !ok {
				break
			}
			rows := []*ast.CompositeLit{value}
			element := array.Elt
			if inner, ok := element.(*ast.ArrayType); ok {
				element = inner.Elt
				rows = nil
				for _, item := range value.Elts {
					if row, ok := item.(*ast.CompositeLit); ok {
						rows = append(rows, row)
					}
				}
			}
			elem, ok := element.(*ast.Ident)
			if !ok || elem.Name != "string" {
				break
			}
			for _, row := range rows {
				if len(row.Elts) == 0 {
					continue
				}
				first := literal(row.Elts[0])
				if creation(first) || (first == "git" && len(row.Elts) > 1 && creation(literal(row.Elts[1]))) {
					found = append(found, row.Pos())
				}
			}
		case *ast.CallExpr:
			name := ""
			switch fn := value.Fun.(type) {
			case *ast.Ident:
				name = fn.Name
			case *ast.SelectorExpr:
				name = fn.Sel.Name
			}
			gitCall := name == "git" || name == "runGit" || name == "nativeGit" || name == "Git"
			if name == "Command" || name == "CommandContext" {
				for _, arg := range value.Args {
					gitCall = gitCall || literal(arg) == "git"
				}
			}
			if gitCall {
				for _, arg := range value.Args {
					if creation(literal(arg)) {
						found = append(found, value.Pos())
						break
					}
				}
			}
		}
		return true
	})
	return found
}

func TestGitCreationPolicyNegativeControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ nativeGit(t, root, "init", "-q") }`,
		`package p; func f(){ exec.Command("git", "clone", a, b) }`,
		`package p; var commands = [][]string{{"init"}, {"config", "user.name", "Test"}}`,
		`package p; var commands = [][]string{{"git", "init", "-q", root}}`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "negative.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(gitCreation(file)) == 0 {
			t.Errorf("unsafe fixture passed: %s", source)
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), "safe.go", `package p; func f(){ testutil.InitGit(t, root) }; var names = []string{"none", "init"}`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(gitCreation(file)) != 0 {
		t.Fatal("safe fixture rejected")
	}
}

func TestGitFixtureCreationUsesHelper(t *testing.T) {
	operator := filepath.Join("..", "..")
	for _, area := range []string{"broker", "runtime"} {
		err := filepath.WalkDir(filepath.Join(operator, area), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			positions := token.NewFileSet()
			file, err := parser.ParseFile(positions, path, nil, 0)
			if err != nil {
				return err
			}
			for _, pos := range gitCreation(file) {
				t.Errorf("%s: git-backed temp repos must use testutil.InitGit or CloneGit; detached maintenance races TempDir cleanup", positions.Position(pos))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
