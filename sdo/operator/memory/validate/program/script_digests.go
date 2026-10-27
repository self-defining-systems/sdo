package program

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/constant"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"strings"

	sdkprogram "sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Script bytes have provenance independent of whether a diagnosis matches a
// replay fixture. Application inputs belong in Env, keeping the script fixed.
func validateScriptDigests(base, corpusRoot string, item Spec) error {
	files, err := filepath.Glob(filepath.Join(base, "*.go"))
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	var parsed []*ast.File
	pkg := types.NewPackage("authored", "authored")
	var constants []*ast.ValueSpec
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		parsed = append(parsed, file)
		ast.Inspect(file, func(node ast.Node) bool {
			decl, ok := node.(*ast.GenDecl)
			if ok && decl.Tok == token.CONST {
				for _, spec := range decl.Specs {
					constants = append(constants, spec.(*ast.ValueSpec))
				}
			}
			return true
		})
	}
	eval := func(expr ast.Expr) (constant.Value, error) {
		var text bytes.Buffer
		if err := format.Node(&text, fset, expr); err != nil {
			return nil, err
		}
		value, err := types.Eval(fset, pkg, token.NoPos, text.String())
		if err != nil {
			return nil, err
		}
		if value.Value == nil {
			return nil, fmt.Errorf("script must be constant text")
		}
		return value.Value, nil
	}
	// Constant declarations may depend on later declarations in the package.
	for {
		resolved := false
		for _, decl := range constants {
			for index, name := range decl.Names {
				if index >= len(decl.Values) || pkg.Scope().Lookup(name.Name) != nil {
					continue
				}
				value, err := eval(decl.Values[index])
				if err == nil {
					kind := map[constant.Kind]types.BasicKind{constant.Bool: types.UntypedBool, constant.Int: types.UntypedInt, constant.Float: types.UntypedFloat, constant.Complex: types.UntypedComplex, constant.String: types.UntypedString}[value.Kind()]
					pkg.Scope().Insert(types.NewConst(name.Pos(), pkg, name.Name, types.Typ[kind], value))
					resolved = true
				}
			}
		}
		if !resolved {
			break
		}
	}
	aliases := map[string]bool{}
	for {
		discovered := false
		for _, file := range parsed {
			ast.Inspect(file, func(node ast.Node) bool {
				spec, ok := node.(*ast.TypeSpec)
				if !ok || !spec.Assign.IsValid() || aliases[spec.Name.Name] {
					return true
				}
				switch typ := spec.Type.(type) {
				case *ast.SelectorExpr:
					if typ.Sel.Name == "RunScript" {
						aliases[spec.Name.Name] = true
						discovered = true
					}
				case *ast.Ident:
					if aliases[typ.Name] {
						aliases[spec.Name.Name] = true
						discovered = true
					}
				}
				return true
			})
		}
		if !discovered {
			break
		}
	}
	var scripts []string
	for _, file := range parsed {
		var invalid error
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			scriptType := false
			switch typ := lit.Type.(type) {
			case *ast.SelectorExpr:
				scriptType = typ.Sel.Name == "RunScript"
			case *ast.Ident:
				scriptType = aliases[typ.Name]
			}
			if !scriptType {
				return true
			}
			var script ast.Expr
			for _, element := range lit.Elts {
				field, ok := element.(*ast.KeyValueExpr)
				if !ok {
					invalid = fmt.Errorf("RunScript requires named Script and Env fields")
					return false
				}
				name, ok := field.Key.(*ast.Ident)
				if ok && name.Name == "Script" {
					script = field.Value
				}
			}
			if script == nil {
				invalid = fmt.Errorf("RunScript requires fixed Script text")
				return false
			}
			value, err := eval(script)
			if err != nil || value.Kind() != constant.String {
				invalid = fmt.Errorf("RunScript requires constant script text with inputs in Env")
				return false
			}
			scripts = append(scripts, constant.StringVal(value))
			return true
		})
		if invalid != nil {
			return invalid
		}
	}
	for _, script := range scripts {
		digest := sdkprogram.ScriptDigest(script)
		linked := false
		for _, id := range item.Cases {
			corpus, err := sdktest.LoadCorpusMetadata(filepath.Join(corpusRoot, id))
			if err != nil {
				return err
			}
			if corpus.ScriptOrigins[digest] == "" {
				continue
			}
			if _, err := sdktest.ScriptOrigin(sdkprogram.RunScript{Script: script}, corpus); err != nil {
				return err
			}
			linked = true
		}
		if !linked {
			return fmt.Errorf("RunScript digest %s requires linkage to its original successful receipt", digest)
		}
	}
	return nil
}
