package runtime

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"sdo.dev/operator/sdk/program"
)

// runPrintedAction takes a printed `sdo mitigate ACTION --bind NAME=VALUE ...`
// through the binding and validation the responder's command goes through. A
// `...` placeholder gets the menu's example value.
func runPrintedAction(t *testing.T, command string) (program.Plan, error) {
	t.Helper()
	words := strings.Fields(command)
	if len(words) < 3 || words[0] != "sdo" || words[1] != "mitigate" {
		t.Fatalf("not a mitigate command: %q", command)
	}
	name := words[2]
	entry, ok := rawMenuActions()[name]
	if !ok {
		t.Fatalf("%q names an unknown action", command)
	}
	parameters := map[string]BindValues{}
	for i := 3; i < len(words); i++ {
		parameter, value, _ := strings.Cut(strings.TrimPrefix(words[i], "--bind="), "=")
		if words[i] == "--bind" {
			continue
		}
		if value == "..." {
			example, found := menuActionExamples()[name][parameter]
			if !found {
				t.Errorf("%s.%s: required parameter needs a menu example", name, parameter)
			}
			value = exampleText(t, example)
		}
		parameters[parameter] = append(parameters[parameter], value)
	}
	texts, err := actionBindings(entry.action, parameters)
	if err != nil {
		return program.Plan{}, err
	}
	return menuRawPlan(entry.action, texts, &program.ValueInputs{})
}

// Every command the menu prints, with its placeholders filled and the optional
// parameters its example chooses, passes validation; a reason it does not print
// is never needed.
func TestEveryMenuCommandIsRunnableWithoutAReasonItDoesNotPrint(t *testing.T) {
	for name, entry := range rawMenuActions() {
		examples, ok := menuActionExamples()[name]
		if !ok {
			continue
		}
		command := menuEntryCommand(name, entry)
		for parameter := range examples {
			if parameter != "reason" && !strings.Contains(command, "--bind "+parameter+"=") {
				command += " --bind " + parameter + "=..."
			}
		}
		if _, err := runPrintedAction(t, command); err != nil {
			t.Errorf("%s: %v", command, err)
		}
	}
}

// Every restore command a source departure prints runs as printed.
func TestEveryDepartureCommandIsRunnableAsPrinted(t *testing.T) {
	for _, kind := range []DepartureKind{DepartureChanged, DepartureMissing, DepartureExtra} {
		command := (SourceDeparture{Ref: dref("Service", "api"), Kind: kind}).Command()
		if command == "" {
			continue
		}
		if _, err := runPrintedAction(t, command); err != nil {
			t.Errorf("%s: %v", command, err)
		}
	}
}

var printedAction = regexp.MustCompile(`sdo mitigate ([A-Z][A-Za-z]+)((?:\s+--bind\s+[A-Za-z_]+=[^\s"` + "`" + `]*)+)`)
var printedParameter = regexp.MustCompile(`--bind\s+([A-Za-z_]+)=`)

// Every built-in action command written in production code, prompts or skills
// binds every parameter the menu requires.
//
//ci:whole-tree
func TestEveryWrittenActionCommandBindsItsRequiredParameters(t *testing.T) {
	found := map[string]bool{}
	err := filepath.WalkDir("../..", func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		switch extension := filepath.Ext(path); {
		case strings.HasSuffix(path, "_test.go"), extension != ".go" && extension != ".py" && extension != ".md":
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range printedAction.FindAllStringSubmatch(string(body), -1) {
			entry, ok := rawMenuActions()[match[1]]
			if !ok {
				continue // a playbook placeholder
			}
			found[match[1]] = true
			bound := map[string]bool{}
			for _, parameter := range printedParameter.FindAllStringSubmatch(match[2], -1) {
				bound[parameter[1]] = true
			}
			for _, parameter := range menuActionParameters(entry.action) {
				if parameter.Required && !bound[parameter.Name] {
					t.Errorf("%s: %q lacks the required %s", path, match[0], parameter.Name)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"DeleteObject", "RetireOldRevision", "ReconcileFromSource"} {
		if !found[name] {
			t.Errorf("the scan did not find the %s command", name)
		}
	}
}

// An omitted reason is recorded as such instead of refusing the fix; a given
// one is kept. An escalation and a source proposal have no default: their
// reason is the content.
func TestAnOmittedReasonIsDefaulted(t *testing.T) {
	for name, want := range map[string]string{"": defaultActionReason, "leftover of the outage": "leftover of the outage"} {
		parameters := map[string]BindValues{"ref": {"Service/app/api"}}
		if name != "" {
			parameters["reason"] = BindValues{name}
		}
		plan, err := bindAction(t, "DeleteObject", parameters)
		if err != nil {
			t.Fatal(err)
		}
		if got := plan.Actions[0].(program.DeleteObject).Reason; got != want {
			t.Fatalf("reason = %q, want %q", got, want)
		}
	}
	for _, name := range []string{"Escalate", "ProposeSourceChange"} {
		for _, parameter := range menuActionParameters(rawMenuActions()[name].action) {
			if parameter.Name == "reason" && (!parameter.Required || parameter.Default != nil) {
				t.Errorf("%s: reason must stay required", name)
			}
		}
	}
}
