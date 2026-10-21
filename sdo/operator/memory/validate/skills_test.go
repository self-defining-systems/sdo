package validate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"sdo.dev/operator/memory/layout"
)

var skillsRoot = filepath.Join("..", "..", "..", "skills")

// internalRules are validator rules no agent skill teaches, each with the reason.
var internalRules = map[string]string{
	RuleLegacyManifest:       "Upkeep migrates schema versions; authoring agents never see a legacy manifest",
	RuleSchemaVersion:        "the schema-version file is controller metadata, changed only by Upkeep",
	RuleGoalMissing:          "the human goal is installed before any agent runs",
	RuleArchitectureMissing:  "the deployer's architecture stage is not an authoring skill flow",
	RuleGoalOwner:            "goal.md is human-owned and no skill flow edits it",
	RuleArchitectureCommit:   "architecture belongs to the deployer lifecycle stage",
	RuleArchitectureBaseline: "architecture belongs to the deployer lifecycle stage",
	RuleUnknownActor:         "the controller supplies the actor; an agent cannot choose it",
	RuleUnknownPhase:         "the controller supplies the phase; an agent cannot choose it",
	RuleUnsafePath:           "a path-traversal guard against malformed diffs, not an authoring decision",
	RuleUpkeepMigration:      "only the Upkeep actor migrates schema versions",
	RuleArchitectureAlone:    "architecture belongs to the deployer lifecycle stage",
}

// ruleIDs returns every rule constant declared in rules.go.
func ruleIDs(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "rules.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok {
			return true
		}
		for _, value := range spec.Values {
			if literal, ok := value.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				id, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
			}
		}
		return true
	})
	if len(ids) == 0 {
		t.Fatal("no rules found in rules.go")
	}
	return ids
}

var (
	citation = regexp.MustCompile(`Enforced by: (.+?)\.?$`)
	backtick = regexp.MustCompile("`([^`]+)`")
)

// citedRules maps each rule ID cited by a skill's references/rules.md to the files citing it.
func citedRules(t *testing.T) map[string][]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(skillsRoot, "*", "references", "rules.md"))
	if err != nil || len(files) != 3 {
		t.Fatalf("want the three skills' rules.md, got %v (%v)", files, err)
	}
	cited := map[string][]string{}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		// A rule is one bullet, possibly wrapped over several lines.
		for _, bullet := range strings.Split("\n"+string(data), "\n- ")[1:] {
			lines := strings.Split(bullet, "\n")
			end := 1
			for end < len(lines) && strings.HasPrefix(lines[end], "  ") {
				end++
			}
			text := strings.Join(strings.Fields(strings.Join(lines[:end], " ")), " ")
			match := citation.FindStringSubmatch(strings.TrimSpace(text))
			if match == nil {
				t.Errorf("%s: rule without an \"Enforced by:\" citation: %.80s", file, text)
				continue
			}
			for _, id := range backtick.FindAllStringSubmatch(match[1], -1) {
				cited[id[1]] = append(cited[id[1]], filepath.Base(filepath.Dir(filepath.Dir(file))))
			}
		}
	}
	return cited
}

func TestSkillRulesCiteExistingValidatorRules(t *testing.T) {
	known := map[string]bool{}
	for _, id := range ruleIDs(t) {
		known[id] = true
	}
	for id, skills := range citedRules(t) {
		if !known[id] {
			t.Errorf("%v cite %q, which is not a validator rule", skills, id)
		}
	}
}

func TestEveryValidatorRuleIsTaughtOrExplicitlyInternal(t *testing.T) {
	cited := citedRules(t)
	for _, id := range ruleIDs(t) {
		_, internal := internalRules[id]
		switch {
		case cited[id] == nil && !internal:
			t.Errorf("rule %s is cited by no skill: teach it in a references/rules.md or list it in internalRules with a reason", id)
		case cited[id] != nil && internal:
			t.Errorf("rule %s is both cited by %v and listed as internal", id, cited[id])
		}
	}
	known := map[string]bool{}
	for _, id := range ruleIDs(t) {
		known[id] = true
	}
	for id := range internalRules {
		if !known[id] {
			t.Errorf("internalRules lists unknown rule %s", id)
		}
	}
}

// exampleTree assembles the skills' examples into a complete memory tree.
func exampleTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	memory := filepath.Join(root, ".sdo")
	copyTree(t, filepath.Join(skillsRoot, "sdo-detector-authoring", "assets", "restart-loop"),
		filepath.Join(memory, "diagnostics", "detectors", "incidents", "restart-loop"))
	copyTree(t, filepath.Join(skillsRoot, "sdo-playbook-authoring", "assets", "restart-loop"),
		filepath.Join(memory, "playbooks", "restart-loop"))
	write(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\napplication: demo\n---\nKeep the application healthy.\n")
	write(t, root, ".sdo/arch.md", "---\nschema_version: 1\ngenerated_at_commit: base\napplication: demo\n---\nA small application.\n")
	write(t, root, ".sdo/schema-version", strconv.Itoa(layout.CurrentSchemaVersion)+"\n")
	write(t, root, ".sdo/playbooks/README.md", "# Playbooks\n\n- [restart-loop](restart-loop/README.md)\n")
	return root
}

func TestSkillExamplesPassMemoryCheckAndGoTest(t *testing.T) {
	root := exampleTree(t)
	got := diagnose(t, Request{Root: root, Layers: Structural | Semantic | Compile, OperatorRoot: filepath.Join("..", "..")})
	if len(got) != 0 {
		t.Fatalf("skill examples are invalid:\n%v", got)
	}
}

func TestSkillsShipTheirDocumentedFiles(t *testing.T) {
	var missing []string
	for _, rel := range []string{
		"sdo-memory/SKILL.md", "sdo-memory/references/rules.md",
		"sdo-detector-authoring/SKILL.md", "sdo-detector-authoring/references/sdk.md",
		"sdo-detector-authoring/references/rules.md", "sdo-detector-authoring/assets/restart-loop/detector.go",
		"sdo-playbook-authoring/SKILL.md", "sdo-playbook-authoring/references/rules.md",
		"sdo-playbook-authoring/assets/restart-loop/README.md",
	} {
		if _, err := os.Stat(filepath.Join(skillsRoot, filepath.FromSlash(rel))); err != nil {
			missing = append(missing, rel)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("missing skill files: %v", missing)
	}
}
