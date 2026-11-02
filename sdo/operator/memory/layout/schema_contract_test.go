package layout

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// Pin the physical path constants and serialized fields that define this edge.
// It deliberately ignores functions and comments, so ordinary fixes don't need
// a version bump. Semantic validation changes still require human review.
func TestSchemaContractVersionAndMigration(t *testing.T) {
	for version := 1; version <= CurrentSchemaVersion; version++ {
		if schemaMigrations[version] == nil {
			t.Fatalf("schema %d has no migration", version)
		}
	}
	var contract []string
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, pattern := range []string{"../*.go", "../../sdk/sdktest/recording.go", "../../contracts/gen/sdodev/contracts/v1alpha1/outcome.pb.go"} {
		extra, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, extra...)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.TypeSpec:
				if s, ok := v.Type.(*ast.StructType); ok {
					for _, f := range s.Fields.List {
						if f.Tag != nil {
							var names []string
							for _, n := range f.Names {
								names = append(names, n.Name)
							}
							var typ bytes.Buffer
							if err := format.Node(&typ, token.NewFileSet(), f.Type); err != nil {
								t.Fatal(err)
							}
							contract = append(contract, filepath.Dir(path)+":"+v.Name.Name+":"+strings.Join(names, ",")+":"+typ.String()+":"+f.Tag.Value)
						}
					}
				}
			case *ast.GenDecl:
				if v.Tok == token.CONST && filepath.Dir(path) == "." {
					for _, spec := range v.Specs {
						value := spec.(*ast.ValueSpec)
						if len(value.Names) == 1 && value.Names[0].Name != "CurrentSchemaVersion" {
							for _, expr := range value.Values {
								var b strings.Builder
								ast.Inspect(expr, func(n ast.Node) bool {
									switch x := n.(type) {
									case *ast.BasicLit:
										b.WriteString(x.Value)
									case *ast.Ident:
										b.WriteString(x.Name)
									}
									return true
								})
								contract = append(contract, value.Names[0].Name+":"+b.String())
							}
						}
					}
				}
			}
			return true
		})
	}
	sort.Strings(contract)
	digest := sha256.Sum256([]byte(strings.Join(contract, "\n")))
	path := filepath.Join("testdata", "schema-v"+strconv.Itoa(CurrentSchemaVersion)+".sha256")
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(expected)) != hex.EncodeToString(digest[:]) {
		t.Fatalf("memory format changed: bump CurrentSchemaVersion, register and test the migration, then pin %s = %x", path, digest)
	}
}
