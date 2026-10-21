package operatorcli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type repeatedFlags []string

func (v *repeatedFlags) String() string         { return strings.Join(*v, ",") }
func (v *repeatedFlags) Set(value string) error { *v = append(*v, value); return nil }

type ProgramParam struct{ Name, Kind string }
type ProgramScaffoldConfig struct {
	FaultClass string
	Params     []ProgramParam
	Mitigate   bool
	Out        string
}

var programID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
var paramName = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*$`)
var paramKind = regexp.MustCompile(`^[A-Z][a-zA-Z0-9]*$`)

func ParseProgramScaffold(args []string) (ProgramScaffoldConfig, error) {
	if len(args) < 2 || args[0] != "program" || args[1] != "scaffold" {
		return ProgramScaffoldConfig{}, errors.New("expected program scaffold")
	}
	flags := flag.NewFlagSet("program scaffold", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var params repeatedFlags
	flags.Var(&params, "param", "required name=Kind binding")
	faultClass := flags.String("fault-class", "", "fault class and program ID")
	mitigate := flags.Bool("mitigate", false, "include mitigation skeleton")
	out := flags.String("out", "", "output playbook directory")
	if err := flags.Parse(args[2:]); err != nil {
		return ProgramScaffoldConfig{}, err
	}
	if flags.NArg() != 0 {
		return ProgramScaffoldConfig{}, errors.New("unexpected scaffold arguments")
	}
	config := ProgramScaffoldConfig{FaultClass: *faultClass, Mitigate: *mitigate, Out: *out}
	for _, raw := range params {
		name, kind, ok := strings.Cut(raw, "=")
		if !ok {
			return ProgramScaffoldConfig{}, fmt.Errorf("invalid parameter %q: expected name=Kind", raw)
		}
		config.Params = append(config.Params, ProgramParam{Name: name, Kind: kind})
	}
	return config, nil
}

func RunProgramScaffold(config ProgramScaffoldConfig) error {
	if !programID.MatchString(config.FaultClass) || config.Out == "" || len(config.Params) == 0 {
		return errors.New("scaffold requires a valid fault class, parameter, and output directory")
	}
	names := map[string]bool{}
	for _, param := range config.Params {
		if !paramName.MatchString(param.Name) || !paramKind.MatchString(param.Kind) || names[param.Name] {
			return fmt.Errorf("invalid or duplicate parameter %q=%q", param.Name, param.Kind)
		}
		names[param.Name] = true
	}
	if err := os.Mkdir(config.Out, 0o755); err != nil {
		return fmt.Errorf("create playbook directory: %w", err)
	}
	pkg := strings.ReplaceAll(config.FaultClass, "-", "_")
	var diagnose strings.Builder
	fmt.Fprintf(&diagnose, "package %s\n\nimport (\n\t\"context\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\ntype Program struct{}\n\nfunc (Program) Spec() program.ProgramSpec {\n\treturn program.ProgramSpec{ID: %q, Params: []program.ParamSpec{\n", pkg, config.FaultClass)
	for _, param := range config.Params {
		fmt.Fprintf(&diagnose, "\t\t{Name: %q, Kind: %q, Required: true},\n", param.Name, param.Kind)
	}
	diagnose.WriteString("\t}}\n}\n\nfunc (Program) Diagnose(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Verdict, error) {\n\treturn program.Verdict{Result: program.Unknown}, nil\n}\n")
	var test strings.Builder
	fmt.Fprintf(&test, "package %s\n\nimport (\n\t\"testing\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\nfunc TestRequiredBindings(t *testing.T) {\n\tif err := program.ValidateBindings(Program{}.Spec(), nil); err == nil {\n\t\tt.Fatal(\"unbound program accepted\")\n\t}\n}\n", pkg)
	readme := "---\nschema_version: 1\nowner: responder\noriginating_incident: " + os.Getenv("SDO_INCIDENT_ID") + "\nfault_class: " + config.FaultClass + "\n---\n\n# " + config.FaultClass + "\n\nDescribe the judgement needed after diagnosis.\n"
	files := map[string]string{"README.md": readme, "diagnose.go": diagnose.String(), "diagnose_test.go": test.String()}
	if config.Mitigate {
		files["mitigate.go"] = fmt.Sprintf("package %s\n\nimport (\n\t\"context\"\n\t\"sdo.dev/operator/sdk/program\"\n)\n\nfunc (Program) Mitigate(_ context.Context, _ program.ReadView, _ program.Bindings) (program.Plan, error) {\n\treturn program.Plan{}, nil\n}\n", pkg)
	}
	for name, content := range files {
		if err := writeNew(filepath.Join(config.Out, name), content); err != nil {
			return err
		}
	}
	return nil
}

func writeNew(path, content string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.WriteString(file, content)
	return err
}
