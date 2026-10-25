package validate

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/sdktest"
)

const RuleReflectionExecutableCause = "reflection.executable-cause"

// reflectionExecutableDiagnostics binds every changed executable to the
// accepted incident's verified causes. Coverage is a separate obligation: an
// SDK gap for one cause cannot authorize executable learning for a peer.
// Both memory check and publication call this with the committed outcome.
func reflectionExecutableDiagnostics(root string, outcome *contracts.OutcomeRecord, paths []string) ([]Diagnostic, error) {
	hasExecutable := false
	for _, p := range paths {
		kind, _, _ := layout.Classify(p)
		hasExecutable = hasExecutable || reflectionExecutableKind(kind)
	}
	if !hasExecutable {
		return nil, nil
	}
	causes := VerifiedReflectionCauses(outcome)
	bound := map[string]bool{}
	// Already settled programs carry controller-owned resource bindings.
	for _, cause := range causes {
		for _, item := range cause.GetItems() {
			if item.GetDisposition() == "covered" && item.GetReason() == "verified_equivalent_repair" && item.GetProgramId() != "" {
				bound[item.GetProgramId()] = true
			}
		}
	}
	// New programs/scripts must bind a positive replay case to a verified cause.
	entries, err := os.ReadDir(filepath.Join(root, layout.CorpusDir))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		c, err := sdktest.LoadCorpusCase(filepath.Join(root, layout.CorpusDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		matched := false
		for _, cause := range causes {
			matched = matched || bindingsCoverCause(c.Bindings.Objects, cause.GetResources())
		}
		// A fixture spanning a known unverified cause cannot launder that cause
		// through another resource's verification.
		for i, cause := range outcome.GetConfirmedRootCauses() {
			verified := false
			for _, eligible := range causes {
				verified = verified || eligible.GetCauseIndex() == uint32(i)
			}
			if !verified && bindingsCoverCause(c.Bindings.Objects, cause.GetResources()) {
				matched = false
				break
			}
		}
		if matched {
			for _, id := range c.RootCauseProgramIDs {
				bound[id] = true
			}
		}
	}
	detectors, err := layout.ReadDetectors(filepath.Join(root, layout.DiagnosticsDir))
	if err != nil {
		return nil, err
	}
	// Package roots and imports also bind shared libraries. A library used by an
	// unverified executable is not authorized by a verified consumer elsewhere.
	authorized, denied := map[string]bool{}, map[string]bool{}
	for _, d := range detectors {
		ok := len(d.Manifest.PossiblePlaybooks) > 0
		for _, id := range d.Manifest.PossiblePlaybooks {
			ok = ok && bound[id]
		}
		dir := filepath.ToSlash(filepath.Join(layout.DiagnosticsDir, d.Package()))
		if ok {
			authorized[dir] = true
		} else {
			denied[dir] = true
		}
	}
	tree, err := layout.ReadTree(root)
	if err != nil {
		return nil, err
	}
	for _, book := range tree.Playbooks {
		dir := layout.PlaybookDir(book.ID)
		if bound[book.ID] {
			authorized[dir] = true
		} else {
			denied[dir] = true
		}
	}
	module := ""
	data, err := os.ReadFile(filepath.Join(root, layout.DiagnosticsGoModFile))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			module = fields[1]
		}
	}
	imports := map[string][]string{}
	err = filepath.WalkDir(filepath.Join(root, layout.Dir), func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(p, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), p, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(p))
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(rel)
		for _, imp := range file.Imports {
			name, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			if module != "" && strings.HasPrefix(name, module+"/lib/") {
				imports[dir] = append(imports[dir], layout.LibrariesDir+strings.TrimPrefix(name, module+"/lib"))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, set := range []map[string]bool{authorized, denied} {
		var visit func(string)
		visit = func(dir string) {
			for _, dep := range imports[dir] {
				if !set[dep] {
					set[dep] = true
					visit(dep)
				}
			}
		}
		for dir := range set {
			visit(dir)
		}
	}
	var ds []Diagnostic
	for _, p := range paths {
		kind, id, _ := layout.Classify(p)
		executable := reflectionExecutableKind(kind)
		if !executable {
			continue
		}
		dir := filepath.ToSlash(filepath.Dir(p))
		if kind == layout.KindPlaybookScript {
			book, _, _ := strings.Cut(id, "/")
			dir = layout.PlaybookDir(book)
		}
		if !authorized[dir] || denied[dir] {
			ds = append(ds, Diagnostic{Rule: RuleReflectionExecutableCause, Path: p, Message: "executable change is not bound to an independently verified cause of this incident", Hint: "bind replay resources to an opening primary-resource finding cleared after its recorded repair; aggregate healthy closure and responder cause claims are insufficient; retain unproven causes as evidence gaps"})
		}
	}
	return ds, nil
}

func reflectionExecutableKind(kind layout.Kind) bool {
	return kind.IsDetectorSource() || kind.IsDetectorManifest() || slices.Contains([]layout.Kind{layout.KindPlaybookProgram, layout.KindPlaybookScript, layout.KindLibrary}, kind)
}
