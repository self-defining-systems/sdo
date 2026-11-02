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
	var flatten func([]ast.Expr) []ast.Expr
	flatten = func(args []ast.Expr) []ast.Expr {
		var result []ast.Expr
		for _, arg := range args {
			switch value := arg.(type) {
			case *ast.CompositeLit:
				result = append(result, value.Elts...)
				continue
			case *ast.CallExpr:
				if fn, ok := value.Fun.(*ast.Ident); ok && fn.Name == "append" {
					result = append(result, flatten(value.Args)...)
					continue
				}
			}
			result = append(result, arg)
		}
		return result
	}
	creation := func(args []ast.Expr) bool {
		args = flatten(args)
		if len(args) > 0 && literal(args[0]) == "git" {
			args = args[1:]
		}
		for len(args) > 0 {
			first := literal(args[0])
			switch first {
			case "-C", "-c", "--git-dir", "--work-tree", "--namespace", "--config-env":
				if len(args) < 2 {
					return false
				}
				args = args[2:]
			default:
				if strings.HasPrefix(first, "-") {
					args = args[1:]
					continue
				}
				return first == "init" || first == "clone"
			}
		}
		return false
	}
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
				if creation(row.Elts) {
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
			var args []ast.Expr
			switch name {
			case "git", "runGit", "nativeGit", "Git":
				if len(value.Args) > 2 {
					args = value.Args[2:]
				}
			case "Command", "CommandContext":
				offset := 0
				if name == "CommandContext" {
					offset = 1
				}
				if len(value.Args) > offset && literal(value.Args[offset]) == "git" {
					args = value.Args[offset+1:]
				}
			}
			if creation(args) {
				found = append(found, value.Pos())
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
		`package p; var commands = [][]string{{"git", "-C", root, "init", "-q"}}`,
		`package p; func f(){ exec.Command("git", append([]string{"-C", root}, "init")...) }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "negative.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(gitCreation(file)) == 0 {
			t.Errorf("unsafe fixture passed: %s", source)
		}
	}
	file, err := parser.ParseFile(token.NewFileSet(), "safe.go", `package p; func f(){ testutil.InitGit(t, root); git(t, root, "show", "HEAD:init") }; var names = []string{"none", "init"}`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(gitCreation(file)) != 0 {
		t.Fatal("safe fixture rejected")
	}
	file, err = parser.ParseFile(token.NewFileSet(), "filename.go", `package p; func f(){ nativeGit(t, root, "add", "init"); exec.Command("git", "show", "HEAD", "--", "init") }`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(gitCreation(file)) != 0 {
		t.Fatal("filename init mistaken for a subcommand")
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
