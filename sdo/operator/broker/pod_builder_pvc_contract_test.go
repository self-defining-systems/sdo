package broker

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// All current pod builders select their repository claim from one configuration
// expression. Walk every operator module so a second Volume using that same
// claim cannot hide in an append, conditional, or another native package.
// Concrete builder tests additionally check the resulting mounts and subPaths.
func duplicatePVCClaims(file *ast.File, positions *token.FileSet) ([]string, int) {
	var problems []string
	builders := 0
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Body == nil {
			continue
		}
		claims := map[string]token.Pos{}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "PersistentVolumeClaimVolumeSource" {
				return true
			}
			for _, field := range literal.Elts {
				value, ok := field.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := value.Key.(*ast.Ident)
				if !ok || key.Name != "ClaimName" {
					continue
				}
				var expression bytes.Buffer
				if err := format.Node(&expression, positions, value.Value); err != nil {
					panic(err)
				}
				claim := expression.String()
				if previous, duplicate := claims[claim]; duplicate {
					problems = append(problems, fmt.Sprintf("%s redeclares PVC %s in %s (first at %s)", positions.Position(value.Pos()), claim, function.Name.Name, positions.Position(previous)))
				}
				claims[claim] = value.Pos()
			}
			return true
		})
		if len(claims) > 0 {
			builders++
		}
	}
	return problems, builders
}

func TestNativePodBuildersDeclareEachPVCOnce(t *testing.T) {
	// Debug filenames are module-relative under -trimpath; go test's package
	// working directory locates the checked-in source independently of them.
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	builders := 0
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "testdata" || entry.Name() == "vendor" || strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, nil, 0)
		if err != nil {
			return err
		}
		problems, count := duplicatePVCClaims(file, positions)
		builders += count
		for _, problem := range problems {
			t.Error(problem)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if builders < 5 {
		t.Fatalf("pod-builder inventory unexpectedly shrank: visited %d PVC builders", builders)
	}
}

func TestPVCBuilderContractRejectsConditionalAndAppendedAliases(t *testing.T) {
	for _, body := range []string{
		`volumes := []v1.Volume{{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName: e.RepositoryPVC}}}}; if session { volumes=append(volumes,v1.Volume{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName:e.RepositoryPVC}}}) }`,
		`pod := v1.PodSpec{Volumes:[]v1.Volume{{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName:"repository"}}},{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName:"repository"}}}}}`,
	} {
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, "fixture.go", "package fixture; func build(){"+body+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		problems, _ := duplicatePVCClaims(file, positions)
		if len(problems) != 1 {
			t.Fatalf("duplicate claim was not caught: %v", problems)
		}
	}
}

func TestPVCBuilderContractAllowsSharedSubpathMountsAndDistinctClaims(t *testing.T) {
	source := `package fixture; func build(){
        volumes:=[]v1.Volume{{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName:"repository"}}},{VolumeSource:v1.VolumeSource{PersistentVolumeClaim:&v1.PersistentVolumeClaimVolumeSource{ClaimName:"independent"}}}}
        mounts:=[]v1.VolumeMount{{Name:"repository",SubPath:"candidate"},{Name:"repository",SubPath:"healthy",ReadOnly:true}}
    }`
	positions := token.NewFileSet()
	file, err := parser.ParseFile(positions, "fixture.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	problems, count := duplicatePVCClaims(file, positions)
	if len(problems) > 0 || count != 1 {
		t.Fatalf("valid shared mounts rejected: %v, builders%d", problems, count)
	}
}
