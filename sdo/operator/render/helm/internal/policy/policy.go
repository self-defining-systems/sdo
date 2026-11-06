// Package policy holds the rules a Helm chart must pass for the helm
// renderer. The structural rules apply to the processed release set (see
// Check), never to the raw chart: subcharts that dependency conditions or tags
// disable are not part of it. The function rule is enforced at execution (see
// Restrict). Every rule is a typed Error so callers and tests can tell which rule
// rejected a chart without matching text.
package policy

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"text/template/parse"

	"helm.sh/helm/v3/pkg/chart"
)

// Rule names one static rejection rule.
type Rule string

const (
	// RuleForbiddenFunction fails a render that executes a template function
	// reading the cluster, the environment, the clock, a random source or the
	// network. It is raised at execution, by the stubs of Restrict.
	RuleForbiddenFunction Rule = "forbidden-function"
	// RuleDependencyNotVendored rejects a Chart.yaml dependency that is not
	// present under charts/. Nothing is fetched from a repository.
	RuleDependencyNotVendored Rule = "dependency-not-vendored"
	// RuleCRDs rejects a crds/ directory with files (cluster-scoped objects).
	RuleCRDs Rule = "crds"
	// RuleTemplateSyntax rejects a template that does not parse.
	RuleTemplateSyntax Rule = "template-syntax"
	// RuleRenderDepth fails a render whose tpl or include calls nest deeper
	// than the engine allows (a template that expands itself). Unbounded
	// recursion would overflow the Go stack, which no recover() can catch.
	RuleRenderDepth Rule = "render-depth"
	// RuleRenderBudget fails a render that exceeds its CPU-time deadline, one
	// output buffer's byte cap, or asks a template function for an
	// amplification (until, repeat, indent, ...) beyond the engine's cap.
	RuleRenderBudget Rule = "render-budget"
)

// Error is one violation of a Rule.
type Error struct {
	Rule   Rule
	Chart  string // chart name, "parent/child" for subcharts
	File   string // chart-relative file, when the rule is about one
	Detail string
}

func (e *Error) Error() string {
	where := e.Chart
	if e.File != "" {
		where += " " + e.File
	}
	return fmt.Sprintf("helm policy %s: %s: %s", e.Rule, where, e.Detail)
}

// Is matches any *Error with the same Rule, so errors.Is(err,
// &policy.Error{Rule: r}) tests for a rule.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Rule == e.Rule
}

// Violates reports whether err (possibly a joined error) holds a violation of rule.
func Violates(err error, rule Rule) bool {
	return errors.Is(err, &Error{Rule: rule})
}

// Check applies the structural rules to the processed chart (Helm's
// dependency processing has already removed disabled subcharts): a dependency
// that is enabled but not vendored, crds/ files, and templates that do not
// parse. They come back joined. What templates do when they run is not judged
// here: the engine's function map is restricted (Restrict) and a forbidden
// function fails the render when, and only when, it is executed.
func Check(ch *chart.Chart) error {
	var errs []error
	checkChart(ch, ch.Name(), &errs)
	return errors.Join(errs...)
}

func checkChart(ch *chart.Chart, name string, errs *[]error) {
	add := func(rule Rule, file, format string, args ...any) {
		*errs = append(*errs, &Error{Rule: rule, Chart: name, File: file, Detail: fmt.Sprintf(format, args...)})
	}
	vendored := map[string]bool{}
	for _, dep := range ch.Dependencies() {
		vendored[dep.Name()] = true
	}
	if ch.Metadata != nil {
		for _, dep := range ch.Metadata.Dependencies {
			if !vendored[dep.Name] {
				add(RuleDependencyNotVendored, "Chart.yaml", "dependency %q is enabled but not vendored under charts/ (repository %q is never fetched)", dep.Name, dep.Repository)
			}
		}
	}
	for _, f := range ch.Files {
		if strings.HasPrefix(f.Name, "crds/") {
			add(RuleCRDs, f.Name, "crds/ is not supported (CRDs are cluster-scoped)")
		}
	}
	for _, t := range ch.Templates {
		// Function existence is the engine's concern: every identifier is kept.
		tree := parse.New(path.Base(t.Name))
		tree.Mode = parse.SkipFuncCheck
		if _, err := tree.Parse(string(t.Data), "{{", "}}", map[string]*parse.Tree{}); err != nil {
			add(RuleTemplateSyntax, t.Name, "%v", err)
		}
	}
	for _, sub := range ch.Dependencies() {
		checkChart(sub, name+"/"+sub.Name(), errs)
	}
}

// Operation is the Helm operation a render stands for. Hooks are selected by
// the events of that operation, as Helm does.
type Operation string

const (
	// OperationInstall runs pre-install and post-install hooks.
	OperationInstall Operation = "install"
	// OperationUpgrade runs pre-upgrade and post-upgrade hooks (release replay).
	OperationUpgrade Operation = "upgrade"
)

// HookPhases maps a hook's events to the phase names it joins in operation op:
// the operation's pre and post events become phases of the same name. Every
// other event (the other operations' events, delete, rollback, test) belongs
// to a different operation and contributes none, so a hook with no event of
// op yields no phase and is left out of the render, as Helm leaves it out.
func HookPhases(op Operation, events []string) []string {
	var phases []string
	for _, e := range events {
		if e == "pre-"+string(op) || e == "post-"+string(op) {
			phases = append(phases, e)
		}
	}
	return phases
}
