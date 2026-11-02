package capturefixture

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// boundaryName matches the files the controller writes through CaptureSnapshot.
var boundaryName = regexp.MustCompile(`(^|/)(open(@[a-z-]+)?|fixed|healthy)\.json$`)

// snapshotKeys are top-level sdktest.Snapshot JSON fields. A raw JSON object
// made only of them is a hand-built capture; object refs and Kubernetes objects
// carry kind/name and are not.
var snapshotKeys = map[string]bool{
	"namespace": true, "objects": true, "object_snapshot_version": true, "pod_admission": true,
	"configMaps": true, "services": true, "pods": true, "deployments": true, "statefulSets": true,
	"replicaSets": true, "horizontalPodAutoscalers": true, "podDisruptionBudgets": true,
	"endpoints": true, "endpointSlices": true, "networkPolicies": true, "events": true,
	"observedAt": true, "traffic": true, "metrics": true, "alerts": true, "readings": true,
	"reachability": true, "scoped_observations": true,
}

// ownerDirs own the projection or this helper; their tests feed raw input to
// the capture path itself.
var ownerDirs = []string{"sdk/sdktest", "internal/capturefixture"}

func stringLiteral(e ast.Expr) (string, bool) {
	if value, ok := e.(*ast.BasicLit); ok && value.Kind == token.STRING {
		text, err := strconv.Unquote(value.Value)
		return text, err == nil
	}
	if call, ok := e.(*ast.CallExpr); ok && len(call.Args) == 1 {
		if array, ok := call.Fun.(*ast.ArrayType); ok {
			if element, ok := array.Elt.(*ast.Ident); ok && element.Name == "byte" {
				return stringLiteral(call.Args[0])
			}
		}
	}
	return "", false
}

func selector(e ast.Expr, pkg, name string) bool {
	s, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	x, ok := s.X.(*ast.Ident)
	return ok && x.Name == pkg && s.Sel.Name == name
}

func isSnapshotType(e ast.Expr) bool {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	return selector(e, "sdktest", "Snapshot")
}

func mentionsBoundary(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if text, ok := stringLiteral(asExpr(n)); ok && boundaryName.MatchString(text) {
			found = true
		}
		return !found
	})
	return found
}

func asExpr(n ast.Node) ast.Expr {
	e, _ := n.(ast.Expr)
	return e
}

func rawSnapshotJSON(text string) bool {
	var object map[string]json.RawMessage
	if json.Unmarshal([]byte(strings.TrimSpace(text)), &object) != nil {
		return false
	}
	for key := range object {
		if !snapshotKeys[key] {
			return false
		}
	}
	return len(object) > 0
}

// handBuiltCaptures finds capture boundaries built outside the capture path:
// a marshalled sdktest.Snapshot value, raw snapshot JSON, or a literal written
// to a boundary file. capturefixture arguments are exempt: FromView captures
// its JSON view and Raw is the named opt-out.
func handBuiltCaptures(file *ast.File) []token.Pos {
	var found []token.Pos
	for _, decl := range file.Decls {
		found = append(found, handBuiltInDecl(decl)...)
	}
	return dedupe(found)
}

// handBuiltInDecl tracks snapshot names per top-level declaration, so a
// parameter named like a literal elsewhere in the file is not confused with it.
func handBuiltInDecl(decl ast.Decl) []token.Pos {
	var found []token.Pos
	snapshots := map[string]bool{}
	snapshotish := func(e ast.Expr) bool {
		if unary, ok := e.(*ast.UnaryExpr); ok && unary.Op == token.AND {
			e = unary.X
		}
		switch value := e.(type) {
		case *ast.CompositeLit:
			return isSnapshotType(value.Type) || mapSnapshot(value)
		case *ast.Ident:
			return snapshots[value.Name]
		}
		return false
	}
	// Pass 1: names bound to hand-built snapshots, including range values over
	// literal collections of them.
	ast.Inspect(decl, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			for i, rhs := range node.Rhs {
				if i < len(node.Lhs) && snapshotish(rhs) {
					if id, ok := node.Lhs[i].(*ast.Ident); ok {
						snapshots[id.Name] = true
					}
				}
			}
		case *ast.ValueSpec:
			literal := node.Type != nil && isSnapshotType(node.Type)
			for i, name := range node.Names {
				if literal || (i < len(node.Values) && snapshotish(node.Values[i])) {
					snapshots[name.Name] = true
				}
			}
		case *ast.RangeStmt:
			if collection, ok := node.X.(*ast.CompositeLit); ok {
				for _, element := range collection.Elts {
					if kv, ok := element.(*ast.KeyValueExpr); ok {
						element = kv.Value
					}
					if snapshotish(element) {
						if id, ok := node.Value.(*ast.Ident); ok {
							snapshots[id.Name] = true
						}
					}
				}
			}
		}
		return true
	})
	// Pass 2: serialisation and literal boundary writes.
	ast.Inspect(decl, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			if s, ok := node.Fun.(*ast.SelectorExpr); ok {
				if x, ok := s.X.(*ast.Ident); ok && x.Name == "capturefixture" {
					return false
				}
			}
			if (selector(node.Fun, "json", "Marshal") || selector(node.Fun, "json", "MarshalIndent")) && len(node.Args) > 0 && snapshotish(node.Args[0]) {
				found = append(found, node.Pos())
			}
			if s, ok := node.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Encode" && len(node.Args) == 1 && snapshotish(node.Args[0]) {
				found = append(found, node.Pos())
			}
			boundary := false
			for _, arg := range node.Args {
				if mentionsBoundary(arg) {
					boundary = true
				}
			}
			if boundary {
				for _, arg := range node.Args {
					if text, ok := stringLiteral(arg); ok && !boundaryName.MatchString(text) && strings.HasPrefix(strings.TrimSpace(text), "{") {
						found = append(found, arg.Pos())
					}
				}
			}
		case *ast.KeyValueExpr:
			key, keyOK := stringLiteral(node.Key)
			value, valueOK := stringLiteral(node.Value)
			if keyOK && valueOK && boundaryName.MatchString(key) && strings.HasPrefix(strings.TrimSpace(value), "{") {
				found = append(found, node.Value.Pos())
				return false
			}
		case *ast.BasicLit:
			if text, ok := stringLiteral(node); ok && rawSnapshotJSON(text) {
				found = append(found, node.Pos())
			}
		}
		return true
	})
	return found
}

func anyType(e ast.Expr) bool {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name == "any"
	}
	_, ok := e.(*ast.InterfaceType)
	return ok
}

// mapSnapshot reports a generic map literal shaped like snapshot JSON: string
// keys that are all snapshot fields, including the namespace.
func mapSnapshot(literal *ast.CompositeLit) bool {
	mapType, ok := literal.Type.(*ast.MapType)
	if !ok || len(literal.Elts) == 0 || !anyType(mapType.Value) {
		return false
	}
	namespace := false
	for _, element := range literal.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return false
		}
		key, ok := stringLiteral(kv.Key)
		if !ok || !snapshotKeys[key] {
			return false
		}
		namespace = namespace || key == "namespace"
	}
	return namespace
}

func dedupe(positions []token.Pos) []token.Pos {
	seen := map[token.Pos]bool{}
	var result []token.Pos
	for _, pos := range positions {
		if !seen[pos] {
			seen[pos] = true
			result = append(result, pos)
		}
	}
	return result
}

func TestCapturePolicyNegativeControls(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ raw, _ := json.Marshal(sdktest.Snapshot{NamespaceName: "demo"}); _ = raw }`,
		`package p; func f(){ view := sdktest.Snapshot{}; raw, _ := json.Marshal(view); _ = raw }`,
		`package p; func f(){ var view sdktest.Snapshot; raw, _ := json.Marshal(&view); _ = raw }`,
		`package p; func f(){ for _, v := range map[string]any{"open.json": sdktest.Snapshot{}} { json.Marshal(v) } }`,
		`package p; func f(){ os.WriteFile(filepath.Join(dir, "fixed.json"), []byte("{}"), 0o600) }`,
		`package p; func f(){ put(t, baseline, "healthy.json", "{\"services\":[]}", 0644) }`,
		`package p; var files = map[string]string{"open@detect.json": "{}"}`,
		"package p; const healthy = `{\"namespace\":\"shop\",\"services\":[]}`",
		`package p; func f(){ healthy := map[string]any{"namespace": "demo", "services": []any{}}; json.Marshal(healthy) }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "unsafe.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(handBuiltCaptures(file)) == 0 {
			t.Errorf("hand-built capture not detected: %s", source)
		}
	}
	safe := `package p
func f(){
	view := sdktest.Snapshot{NamespaceName: "demo"}
	capturefixture.WriteBoundary(t, filepath.Join(dir, "fixed.json"), view, "demo")
	raw := capturefixture.Boundary(t, view, "demo")
	healthy := capturefixture.FromView(t, "{\"namespace\":\"demo\",\"services\":[]}")
	view := map[string]any{"namespace": "demo"}
	capturefixture.FromView(t, view)
	labels := map[string]string{"namespace": "shop"}
	json.Marshal(labels)
	os.WriteFile(filepath.Join(dir, "open.json"), raw, 0o600)
	os.WriteFile(filepath.Join(dir, "fixed.json"), capturefixture.Raw(t, "malformed input", []byte("{\"namespace\":\"forged\"}")), 0o600)
	os.WriteFile(filepath.Join(dir, "recordings.json"), []byte("[]"), 0o600)
	detector.Evaluate(view)
	json.Marshal(outcome)
	_ = "{\"metadata\":{\"name\":\"x\",\"namespace\":\"demo\"}}"
}`
	file, err := parser.ParseFile(token.NewFileSet(), "safe.go", safe, 0)
	if err != nil {
		t.Fatal(err)
	}
	if found := handBuiltCaptures(file); len(found) != 0 {
		t.Fatalf("safe capture use flagged at %v", found)
	}
}

// TestCaptureFixturesUseRealProjection keeps capture boundary fixtures on the
// production path (CaptureSnapshot → ProjectCapturedObject).
func TestCaptureFixturesUseRealProjection(t *testing.T) {
	operator := filepath.Join("..", "..")
	err := filepath.WalkDir(operator, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(operator, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			for _, owner := range ownerDirs {
				if rel == owner {
					return filepath.SkipDir
				}
			}
			if rel != "." && strings.HasPrefix(entry.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		positions := token.NewFileSet()
		file, err := parser.ParseFile(positions, path, nil, 0)
		if err != nil {
			return err
		}
		for _, pos := range handBuiltCaptures(file) {
			t.Errorf("%s: capture fixtures must come from capturefixture.Boundary/WriteBoundary (the real CaptureSnapshot projection); use capturefixture.Raw(t, reason, data) for a deliberately raw shape", positions.Position(pos))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
