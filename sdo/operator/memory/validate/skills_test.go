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

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

var skillsRoot = filepath.Join("..", "..", "..", "skills")

// internalRules are validator rules no agent skill teaches, each with the reason.
var internalRules = map[string]string{
	RuleUpkeepMigration:         "controller-owned registered schema migrations; workers cannot request them",
	RuleDetectorHealthyBaseline: "missing controller healthy capture is informational",
	RuleReplayObservation:       "informational replay observations, never an acceptance gate",
	RuleLegacyManifest:          "only the current schema is supported; authoring agents never see a legacy manifest",
	RuleSchemaVersion:           "the schema-version file is controller metadata, changed only by Upkeep",
	RuleArchitectureMissing:     "Go generates the manifest inventory; no authoring skill edits it",
	RuleGoalMissing:             "a non-blocking note; the goal is human-owned and install supplies the standard one",
	RuleGoalOwner:               "goal.md is human-owned and no skill flow edits it",
	RuleSourcesInvalid:          "sources.yaml is human goal-class input written by the controller; no skill flow edits it",
	RuleUnknownActor:            "the controller supplies the actor; an agent cannot choose it",
	RuleUnknownPhase:            "the controller supplies the phase; an agent cannot choose it",
	RuleUnsafePath:              "a path-traversal guard against malformed diffs, not an authoring decision",
	RuleDeploymentInvalid:       "the optional override is decoded at deployment; the serving skill documents its fields",

	RuleTrafficDefaultLatency: "a coverage-gap name, recorded and never refused; the detector-authoring skill teaches latency bounds",
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
	files, err := filepath.Glob(filepath.Join(skillsRoot, "*", "references", "*rules.md"))
	if err != nil || len(files) != 5 {
		t.Fatalf("want the five skills' rules.md, got %v (%v)", files, err)
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
	copyTree(t, filepath.Join(skillsRoot, "sdo-detectors", "assets", "restart-loop"),
		filepath.Join(memory, "diagnostics", "detectors", "learned", "restart-loop"))
	copyTree(t, filepath.Join(skillsRoot, "sdo-playbook-authoring", "assets", "restart-loop"),
		filepath.Join(memory, "playbooks", "restart-loop"))
	write(t, root, ".sdo/goal.md", "---\nschema_version: 1\nowner: human\napplication: demo\n---\nKeep the application healthy.\n")
	write(t, root, ".sdo/arch.md", "---\nschema_version: 1\ngenerated_at_commit: base\napplication: demo\n---\nA small application.\n")
	write(t, root, ".sdo/schema-version", strconv.Itoa(layout.CurrentSchemaVersion)+"\n")
	generateIndex(t, root)
	return root
}

func TestSkillsShipTheirDocumentedFiles(t *testing.T) {
	var missing []string
	for _, rel := range []string{
		"sdo-memory/SKILL.md", "sdo-memory/references/rules.md",
		"sdo-detectors/SKILL.md", "sdo-detectors/references/sdk.md",
		"sdo-detectors/references/traffic.md",
		"sdo-detectors/references/rules.md", "sdo-detectors/assets/restart-loop/detector.go",
		"sdo-playbook-authoring/SKILL.md", "sdo-playbook-authoring/references/rules.md",
		"sdo-playbook-authoring/assets/restart-loop/README.md",
		"sdo-operations-map/SKILL.md", "sdo-detectors/references/cause-catalog.md",
		"sdo-detectors/references/methods.md", "sdo-operations-map/references/map.md",
		"sdo-operations-map/references/rules.md", "sdo-detectors/references/probes.md",
		"sdo-serving/SKILL.md", "sdo-serving/references/catalog.md", "sdo-serving/references/rules.md",
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

// map.uncovered-operation accepts only a call probe: a write-read probe that
// also calls the operation does not cover it. Text that offers either kind
// sends the designer into a refused commit.
func TestCoverageTextRequiresACallProbeForEveryOperation(t *testing.T) {
	either := regexp.MustCompile("(?i)\\bcall`? (or|and/or) `?write-read")
	operationsMap := filepath.Join(skillsRoot, "sdo-operations-map", "references")
	for _, file := range []string{
		filepath.Join(operationsMap, "rules.md"),
		filepath.Join(operationsMap, "map.md"),
		filepath.Join(skillsRoot, "sdo-detectors", "SKILL.md"),
		filepath.Join(skillsRoot, "..", "..", "docs", "design", "application-map.md"),
	} {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.Join(strings.Fields(string(data)), " ")
		if found := either.FindString(text); found != "" {
			t.Errorf("%s says %q covers an operation; only a call probe does", file, found)
		}
		if strings.Contains(text, "| read operation |") {
			t.Errorf("%s limits call probes to read operations; every operation needs one", file)
		}
	}
}

// The example map a designer copies passes the operation coverage rules.
func TestSkillExampleMapCoversItsOperations(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(skillsRoot, "sdo-operations-map", "references", "map.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, example, found := strings.Cut(string(data), "<!-- sdo:application-map:begin -->\n```yaml\n")
	if !found {
		t.Fatal("map.md has no example map")
	}
	example, _, _ = strings.Cut(example, "\n```")
	parsed, err := appmap.Parse(example)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range parsed.Check(appmap.ParseInventory(""), nil) {
		if finding.Rule == appmap.RuleUncoveredOp || finding.Rule == appmap.RuleUncoveredWrite {
			t.Errorf("the example map is refused: %s", finding.Message)
		}
	}
}
