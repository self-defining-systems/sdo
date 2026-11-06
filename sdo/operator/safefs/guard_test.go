package safefs_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// writerCalls are the os functions that create, replace, move or delete files.
// Each follows symlinks (or creates through them) and so must not run on a path
// below an agent-controlled checkout.
var writerCalls = map[string]bool{
	"WriteFile": true, "Create": true, "OpenFile": true, "Rename": true, "MkdirAll": true,
	"Mkdir": true, "Remove": true, "RemoveAll": true, "Symlink": true, "Link": true,
	"Chmod": true, "Truncate": true, "CreateTemp": true,
}

// guardedDirs are the packages that write into proposal, scratch or candidate
// checkouts (plan.yaml, patch capture, materialized views, evidence files).
var guardedDirs = []string{
	"planfile", "patchset", "memory", "memory/layout", "memory/validate", "lifecycle", "broker",
}

// operatorOwned lists the files in guardedDirs that still call the os writers
// directly, each with why that is not a write into an agent-controlled tree.
// Writing into an agent checkout is never a valid reason: use safefs. Adding a
// file here is a review decision.
var operatorOwned = map[string]string{
	"broker/advance.go":               "removes broker-owned proposal roots (whole directories, never a path inside a checkout)",
	"broker/advance_patches.go":       "removes a MkdirTemp scratch directory",
	"broker/subprocess_validator.go":  "removes a MkdirTemp scratch directory",
	"broker/attestation.go":           "materializes an attestation tree into a fresh broker-created scratch directory",
	"broker/broker.go":                "broker-owned workspaces and ledger; copyChange Lstats and rejects symlinks before it replaces a file",
	"broker/concurrent_publish.go":    "writes into clones and metadata the broker itself created from the accepted base",
	"broker/rejection.go":             "broker-owned rejection ledger",
	"broker/source_evidence.go":       "broker-owned ledger under the operator state directory",
	"broker/stage.go":                 "broker-owned candidate staging directory",
	"memory/api_history.go":           "operator-owned recovery state, not a checkout",
	"memory/commit_catalog.go":        "scratch directory created with MkdirTemp",
	"memory/corpus_capture.go":        "controller capture area under the repository mount, written by the controller",
	"memory/healthy_baseline.go":      "operator state directory",
	"memory/json_repository.go":       "outcome store: accepted-branch repository owned by the operator",
	"memory/reachability_baseline.go": "operator state directory",
	"memory/sdk_gaps.go":              "outcome store: accepted-branch repository owned by the operator",
	"memory/session.go":               "operator session storage",
	"memory/layout/migrate.go":        "migrates a bootstrap checkout or a base tree the operator materialized, before any agent turn",
	"memory/validate/git.go":          "materializes the base tree into a MkdirTemp scratch directory",
	"memory/validate/ownership.go":    "removes a MkdirTemp scratch directory",
	"memory/validate/preexisting.go":  "operator cache directory",
	"memory/validate/semantic.go":     "removes a MkdirTemp scratch directory",
	"memory/validate/whitespace.go":   "MkdirTemp scratch index",
	"memory/validate/publication.go":  "removes a MkdirTemp scratch directory",
	"patchset/checkout.go":            "pruneEmpty removes only empty directories after safefs.Remove checked the parents",
}

func TestCheckoutWritersGoThroughSafefs(t *testing.T) {
	var offenders []string
	seen := map[string]bool{}
	for _, dir := range guardedDirs {
		entries, err := os.ReadDir(filepath.Join("..", filepath.FromSlash(dir)))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			rel := dir + "/" + name
			file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("..", filepath.FromSlash(rel)), nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "os" && writerCalls[sel.Sel.Name] {
					seen[rel] = true
					if operatorOwned[rel] == "" {
						offenders = append(offenders, rel+": os."+sel.Sel.Name)
					}
				}
				return true
			})
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("direct os writers in packages that touch agent checkouts; use safefs (or list the file in operatorOwned with a reason it never sees agent-controlled paths):\n  %s", strings.Join(offenders, "\n  "))
	}
	for rel := range operatorOwned {
		if !seen[rel] {
			t.Errorf("%s no longer calls an os writer; delete its operatorOwned entry", rel)
		}
	}
}
