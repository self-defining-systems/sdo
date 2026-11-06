package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// MitigateTarget is one playbook or built-in action named on the command line.
// Parameters are the raw --bind values; Go types them against the playbook's
// declared parameters, so the responder never sees an option hash.
type MitigateTarget struct {
	Name                string            `json:"name"`
	Parameters          map[string]string `json:"parameters,omitempty"`
	FindingFingerprints []string          `json:"finding_fingerprints,omitempty"`
}

func (l *ResponderLoop) programSpec(id string) (program.ProgramSpec, bool) {
	if l.Programs == nil {
		return program.ProgramSpec{}, false
	}
	for _, spec := range l.Programs.List() {
		if spec.ID == id {
			return spec, true
		}
	}
	return program.ProgramSpec{}, false
}

// programUsage spells out what a playbook takes, in the form --bind accepts.
func programUsage(spec program.ProgramSpec) string {
	if len(spec.Params) == 0 {
		return "no parameters"
	}
	parts := make([]string, 0, len(spec.Params))
	for _, p := range spec.Params {
		part := p.Name + "=VALUE"
		if p.ValueKind == "" {
			part = p.Name + "=" + string(p.Kind) + "/NAMESPACE/NAME"
		}
		if !p.Required || p.Default != nil {
			part = "[" + part + "]"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, " ")
}

// bindNamedParameters turns the responder's NAME=VALUE arguments into bindings
// and says what the playbook takes when a name is wrong or a role is missing.
func bindNamedParameters(spec program.ProgramSpec, parameters map[string]string) (program.Bindings, error) {
	usage := programUsage(spec)
	declared := map[string]bool{}
	for _, p := range spec.Params {
		declared[p.Name] = true
	}
	names := make([]string, 0, len(parameters))
	for name := range parameters {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !declared[name] {
			return program.Bindings{}, fmt.Errorf("%s has no parameter '%s'; it takes %s", spec.ID, name, usage)
		}
	}
	bound, err := menuBindProgram(menuEntry{spec: spec}, parameters)
	if err == nil {
		err = program.ValidateBindings(spec, bound)
	}
	if err != nil {
		return program.Bindings{}, fmt.Errorf("%s: %v; it takes %s", spec.ID, err, usage)
	}
	return cloneProgramBindings(bound), nil
}

// mitigationCandidates are the bindings SDO already knows for a playbook: its
// recorded coverage matches and the bindings its open findings imply.
func (l *ResponderLoop) mitigationCandidates(spec program.ProgramSpec) []program.Bindings {
	candidates := coverageMatchBindings(l.Request.PlaybookCoverage, spec.ID, program.Bindings{})
	for _, finding := range l.Request.Findings {
		if bound, err := program.BindFinding(spec, finding); err == nil {
			candidates = append(candidates, bound)
		}
	}
	var unique []program.Bindings
	for _, candidate := range candidates {
		duplicate := false
		for _, seen := range unique {
			duplicate = duplicate || seen.Equal(candidate)
		}
		if !duplicate {
			unique = append(unique, cloneProgramBindings(candidate))
		}
	}
	return unique
}

// bindForMitigation uses the given --bind values, or the one recorded match.
func (l *ResponderLoop) bindForMitigation(spec program.ProgramSpec, parameters map[string]string) (program.Bindings, error) {
	if len(parameters) > 0 || len(spec.Params) == 0 {
		return bindNamedParameters(spec, parameters)
	}
	candidates := l.mitigationCandidates(spec)
	switch len(candidates) {
	case 0:
		return program.Bindings{}, fmt.Errorf("%s has no recorded match; it takes %s; run: sdo mitigate %s --bind ...", spec.ID, programUsage(spec), spec.ID)
	case 1:
		return candidates[0], nil
	}
	lines := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		lines = append(lines, "  "+mitigateCommand(spec.ID, candidate))
	}
	sort.Strings(lines)
	return program.Bindings{}, fmt.Errorf("%s matches more than one binding; pick one:\n%s", spec.ID, strings.Join(lines, "\n"))
}

// mitigateCommand is the copy-pasteable command that applies a playbook with
// exactly these bindings.
func mitigateCommand(programID string, bindings program.Bindings) string {
	parts := []string{"sdo mitigate " + shellWord(programID)}
	names := make([]string, 0, len(bindings.Objects)+len(bindings.Values))
	for name := range bindings.Objects {
		names = append(names, name)
	}
	for name := range bindings.Values {
		if _, object := bindings.Objects[name]; !object {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		value := ""
		if ref, ok := bindings.Objects[name]; ok {
			value = objectRefText(ref)
		} else if text, ok := bindings.Values[name].(string); ok {
			value = text
		} else {
			encoded, _ := json.Marshal(bindings.Values[name])
			value = string(encoded)
		}
		parts = append(parts, "--bind "+shellWord(name+"="+value))
	}
	return strings.Join(parts, " ")
}

func objectRefText(ref sdk.ObjectRef) string {
	if ref.Namespace == "" {
		return ref.Kind + "/" + ref.Name
	}
	return ref.Kind + "/" + ref.Namespace + "/" + ref.Name
}

func shellWord(text string) string {
	if text != "" && strings.Trim(text, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./=:@%+,") == "" {
		return text
	}
	return "'" + strings.ReplaceAll(text, "'", `'\''`) + "'"
}

// resolveMitigateTargets maps named targets onto the session catalog's exact
// selections: a built-in action keeps its name; a playbook with its bindings
// becomes "program-"+digest, advertised here so the live precondition recheck
// and overlapping-write refusal run on the exact program and bindings.
func (l *ResponderLoop) resolveMitigateTargets(request *ToolRequest) error {
	if len(request.Targets) == 0 {
		return nil
	}
	if len(request.Targets) > 1 && request.Rounds > 1 {
		return fmt.Errorf("--rounds applies to one fix; run it as a separate command")
	}
	actions := rawMenuActions()
	options := make([]*contracts.MitigationOptionSelection, 0, len(request.Targets))
	for _, target := range request.Targets {
		option := &contracts.MitigationOptionSelection{FindingFingerprints: target.FindingFingerprints}
		if action, ok := actions[target.Name]; ok {
			l.menuEntries.advertise(map[string]menuEntry{target.Name: action})
			option.OptionId = target.Name
			option.Parameters = target.Parameters
			options = append(options, option)
			continue
		}
		spec, ok := l.programSpec(target.Name)
		if !ok {
			return fmt.Errorf("unknown playbook or action '%s'; see sdo menu", target.Name)
		}
		if _, ok := l.Programs.Mitigator(spec.ID); !ok {
			return fmt.Errorf("%s only diagnoses; it has no fix to apply", spec.ID)
		}
		bindings, err := l.bindForMitigation(spec, target.Parameters)
		if err != nil {
			return err
		}
		option.OptionId = "program-" + menuDigest([]any{spec.ID, bindings})
		l.menuEntries.advertise(map[string]menuEntry{option.OptionId: {spec: spec, programID: spec.ID, bindings: cloneProgramBindings(bindings)}})
		options = append(options, option)
	}
	if len(options) == 1 {
		request.Selection = &contracts.MitigationSelection{Version: 1, OptionId: options[0].OptionId, RoundLimit: request.Rounds}
		request.Parameters = options[0].Parameters
		request.FindingFingerprints = options[0].FindingFingerprints
	} else {
		request.SelectionSet = &contracts.MitigationSelectionSet{Version: 1, Options: options}
	}
	return nil
}

// loadTargetPrograms compiles the working copy of each named playbook; built-in
// actions have no source to load.
func (l *ResponderLoop) loadTargetPrograms(ctx context.Context, targets []MitigateTarget) error {
	actions := rawMenuActions()
	for _, target := range targets {
		if _, builtin := actions[target.Name]; builtin {
			continue
		}
		if err := l.loadWorkingPrograms(ctx, target.Name); err != nil {
			return err
		}
	}
	return nil
}

// diagnoseBindings binds a diagnose call the way mitigate does: --bind values
// are typed against the playbook's parameters, and a wrong or missing role is
// an error naming what the playbook takes, never an Unknown verdict.
func diagnoseBindings(spec program.ProgramSpec, request ToolRequest) (program.Bindings, error) {
	if len(request.Parameters) > 0 {
		return bindNamedParameters(spec, request.Parameters)
	}
	if err := program.ValidateBindings(spec, request.Bindings); err != nil {
		return program.Bindings{}, fmt.Errorf("%s: %v; it takes %s", spec.ID, err, programUsage(spec))
	}
	return request.Bindings, nil
}
