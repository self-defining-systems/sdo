// Package policy holds the static rules a Helm chart must pass before the
// helm renderer executes it. Every rule is a typed Error so callers and tests
// can tell which rule rejected a chart without matching text.
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
	// RuleUnsupportedHook rejects a hook event other than pre-install and
	// post-install.
	RuleUnsupportedHook Rule = "unsupported-hook"
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

// Check applies every static rule to the loaded chart and its vendored
// subcharts. It returns all violations joined; nil means the chart may render.
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
				add(RuleDependencyNotVendored, "Chart.yaml", "dependency %q is not vendored under charts/ (repository %q is never fetched)", dep.Name, dep.Repository)
			}
		}
	}
	for _, f := range ch.Files {
		if strings.HasPrefix(f.Name, "crds/") {
			add(RuleCRDs, f.Name, "crds/ is not supported (CRDs are cluster-scoped)")
		}
	}
	for _, t := range ch.Templates {
		if err := checkTemplate(t.Name, string(t.Data), func(fn, why string) {
			add(RuleForbiddenFunction, t.Name, "template function %q %s", fn, why)
		}); err != nil {
			add(RuleTemplateSyntax, t.Name, "%v", err)
		}
	}
	for _, sub := range ch.Dependencies() {
		checkChart(sub, name+"/"+sub.Name(), errs)
	}
}

func checkTemplate(name, text string, report func(fn, why string)) error {
	trees := map[string]*parse.Tree{}
	tree := parse.New(path.Base(name))
	// Function existence is the renderer's concern; here every identifier is
	// kept so a forbidden one cannot hide behind an unknown-function error.
	tree.Mode = parse.SkipFuncCheck
	if _, err := tree.Parse(text, "{{", "}}", trees); err != nil {
		return err
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
	return nil
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

// HookPhases maps a hook's events to the phase names it joins. pre-install and
// post-install are phases; every other event is a typed rejection.
func HookPhases(chartName, hook string, events []string) ([]string, error) {
	var phases []string
	for _, e := range events {
		switch e {
		case "pre-install", "post-install":
			phases = append(phases, e)
		default:
			return nil, &Error{Rule: RuleUnsupportedHook, Chart: chartName, File: hook,
				Detail: fmt.Sprintf("hook event %q is not supported; only pre-install and post-install map to phases (upgrade, delete, rollback and test hooks need the later phases work)", e)}
		}
	}
	return phases, nil
}
