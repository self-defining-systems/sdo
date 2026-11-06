// Package policy holds the rules a Helm chart must pass for the helm
// renderer. They apply to the processed release set (see Check), never to the
// raw chart: subcharts that dependency conditions or tags disable are not part
// of it. Every rule is a typed Error so callers and tests can tell which rule
// rejected a chart without matching text.
package policy

import (
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"text/template/parse"

	"helm.sh/helm/v3/pkg/chart"
)

// Rule names one static rejection rule.
type Rule string

const (
	// RuleForbiddenFunction rejects template functions that read the cluster,
	// the clock, a random source or the network.
	RuleForbiddenFunction Rule = "forbidden-function"
	// RuleDependencyNotVendored rejects a Chart.yaml dependency that is not
	// present under charts/. Nothing is fetched from a repository.
	RuleDependencyNotVendored Rule = "dependency-not-vendored"
	// RuleCRDs rejects a crds/ directory with files (cluster-scoped objects).
	RuleCRDs Rule = "crds"
	// RuleTemplateSyntax rejects a template that does not parse.
	RuleTemplateSyntax Rule = "template-syntax"
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

// forbiddenFunctions are Helm and sprig template functions whose result is
// not a pure function of the chart, values and release.
// The date family is rejected outright: date and its siblings fall back to the
// current time for any argument that is not a time or an integer, which a
// static check cannot rule out.
var forbiddenFunctions = map[string]string{
	"lookup": "reads the live cluster", "getHostByName": "resolves names over the network",
	"now": "reads the clock", "date": "may read the clock", "dateInZone": "may read the clock",
	"htmlDate": "may read the clock", "htmlDateInZone": "may read the clock", "ago": "reads the clock",
	"randAlpha": "is random", "randAlphaNum": "is random", "randNumeric": "is random", "randAscii": "is random",
	"randInt": "is random", "randBytes": "is random", "uuidv4": "is random", "shuffle": "is random",
	"genCA": "is random", "genCAWithKey": "is random", "genPrivateKey": "is random",
	"htpasswd": "is salted randomly", "bcrypt": "is salted randomly", "encryptAES": "may use a random IV",
}

func forbidden(name string) (string, bool) {
	if why, ok := forbiddenFunctions[name]; ok {
		return why, true
	}
	if strings.HasPrefix(name, "genSelfSignedCert") || strings.HasPrefix(name, "genSignedCert") {
		return "is random", true
	}
	return "", false
}

// Conditional holds forbidden-function findings in template files whose
// rendered output decides whether they matter, keyed by the template's path in
// the render output (the engine's file name, "chart/charts/sub/templates/x.yaml").
type Conditional map[string][]*Error

// Resolve returns the findings of every file whose output belongs to the
// release set (kept), joined; nil when no kept file holds a finding. A file
// that rendered nothing or only hooks the operation does not run produces no
// object whose content depends on the forbidden function, so its findings do
// not apply.
func (c Conditional) Resolve(kept func(file string) bool) error {
	files := make([]string, 0, len(c))
	for file := range c {
		files = append(files, file)
	}
	sort.Strings(files)
	var errs []error
	for _, file := range files {
		if !kept(file) {
			continue
		}
		for _, e := range c[file] {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

// Check applies the structural rules to the processed chart (Helm's
// dependency processing has already removed disabled subcharts) and returns
// the forbidden-function findings that depend on rendering. Structural errors
// come back joined: a dependency that is enabled but not vendored, crds/
// files, templates that do not parse, and forbidden functions in helper
// templates (files named _*, and files that define named templates), which any
// template may include. Findings in an output file are returned as
// Conditional, to be resolved against what that file rendered. NOTES.txt is
// rendered and discarded, so it is exempt.
func Check(ch *chart.Chart) (Conditional, error) {
	var errs []error
	conditional := Conditional{}
	checkChart(ch, ch.Name(), &errs, conditional)
	return conditional, errors.Join(errs...)
}

func checkChart(ch *chart.Chart, name string, errs *[]error, conditional Conditional) {
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
		var found []*Error
		defines, err := checkTemplate(t.Name, string(t.Data), func(fn, why string) {
			found = append(found, &Error{Rule: RuleForbiddenFunction, Chart: name, File: t.Name,
				Detail: fmt.Sprintf("template function %q %s", fn, why)})
		})
		if err != nil {
			add(RuleTemplateSyntax, t.Name, "%v", err)
			continue
		}
		base := path.Base(t.Name)
		switch {
		case base == "NOTES.txt":
		case strings.HasPrefix(base, "_") || defines:
			for _, e := range found {
				*errs = append(*errs, e)
			}
		case len(found) > 0:
			conditional[path.Join(ch.ChartFullPath(), t.Name)] = found
		}
	}
	for _, sub := range ch.Dependencies() {
		checkChart(sub, name+"/"+sub.Name(), errs, conditional)
	}
}

// checkTemplate reports every forbidden function the template mentions and
// whether it defines named templates besides its own.
func checkTemplate(name, text string, report func(fn, why string)) (defines bool, err error) {
	trees := map[string]*parse.Tree{}
	tree := parse.New(path.Base(name))
	// Function existence is the renderer's concern; here every identifier is
	// kept so a forbidden one cannot hide behind an unknown-function error.
	tree.Mode = parse.SkipFuncCheck
	if _, err := tree.Parse(text, "{{", "}}", trees); err != nil {
		return false, err
	}
	seen := map[string]bool{}
	names := make([]string, 0, len(trees))
	for n := range trees {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		walk(trees[n].Root, func(id string) {
			if why, bad := forbidden(id); bad && !seen[id] {
				seen[id] = true
				report(id, why)
			}
		})
	}
	for n := range trees {
		if n != path.Base(name) {
			defines = true
		}
	}
	return defines, nil
}

func walk(n parse.Node, visit func(string)) {
	switch n := n.(type) {
	case nil:
	case *parse.ListNode:
		if n != nil {
			for _, c := range n.Nodes {
				walk(c, visit)
			}
		}
	case *parse.ActionNode:
		walk(n.Pipe, visit)
	case *parse.PipeNode:
		if n != nil {
			for _, c := range n.Cmds {
				walk(c, visit)
			}
		}
	case *parse.CommandNode:
		for _, a := range n.Args {
			walk(a, visit)
		}
	case *parse.IdentifierNode:
		visit(n.Ident)
	case *parse.ChainNode:
		walk(n.Node, visit)
	case *parse.IfNode:
		walkBranch(&n.BranchNode, visit)
	case *parse.RangeNode:
		walkBranch(&n.BranchNode, visit)
	case *parse.WithNode:
		walkBranch(&n.BranchNode, visit)
	case *parse.TemplateNode:
		walk(n.Pipe, visit)
	}
}

func walkBranch(b *parse.BranchNode, visit func(string)) {
	walk(b.Pipe, visit)
	walk(b.List, visit)
	walk(b.ElseList, visit)
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
