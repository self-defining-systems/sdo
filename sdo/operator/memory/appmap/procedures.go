package appmap

import (
	"fmt"
	"strings"
	"time"

	"sdo.dev/operator/sdk/program"
)

// Procedure is one operations procedure the application ships, as the map
// records it: where it runs (a component and container, or a Job of the
// source it re-creates), its command with typed parameters, its effect class,
// how to tell it worked and where it is declared.
type Procedure struct {
	ID        string           `yaml:"id"`
	Component string           `yaml:"component,omitempty"`
	Container string           `yaml:"container,omitempty"`
	Job       string           `yaml:"job,omitempty"`
	Command   []string         `yaml:"command,omitempty"`
	Params    []ProcedureParam `yaml:"params,omitempty"`
	Effect    string           `yaml:"effect"`
	Verify    string           `yaml:"verify,omitempty"`
	Timeout   string           `yaml:"timeout,omitempty"`
	Source    string           `yaml:"source"`
	// ProbeIdentity marks the procedure that provisions probe users; see
	// program.Procedure.ProbeIdentity.
	ProbeIdentity bool `yaml:"probe_identity,omitempty"`
}

// ProcedureParam is one typed parameter of a procedure.
type ProcedureParam struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Pattern  string `yaml:"pattern,omitempty"`
	Required bool   `yaml:"required,omitempty"`
	Default  any    `yaml:"default,omitempty"`
}

// Program converts the entry to the SDK's form. A timeout that does not parse
// is an error.
func (p Procedure) Program() (program.Procedure, error) {
	out := program.Procedure{ID: p.ID, Component: p.Component, Container: p.Container, Job: p.Job, Command: append([]string(nil), p.Command...), Effect: p.Effect, Verify: p.Verify, Source: p.Source, ProbeIdentity: p.ProbeIdentity}
	for _, param := range p.Params {
		out.Params = append(out.Params, program.ProcedureParam{Name: param.Name, Type: param.Type, Pattern: param.Pattern, Required: param.Required, Default: param.Default})
	}
	if p.Timeout != "" {
		timeout, err := time.ParseDuration(p.Timeout)
		if err != nil {
			return out, fmt.Errorf("procedure %s: timeout %q is not a duration such as 90s or 5m", p.ID, p.Timeout)
		}
		out.Timeout = timeout
	}
	return out, nil
}

const procedureHint = "a procedure is {id, component, container, command, params?, effect: check|repair|data|long, verify?, timeout?, probe_identity?, source} or {id, job: Job/<name>, effect, source}; a probe_identity procedure takes only a required string username and reads the password from stdin"

// procedures checks each procedure's fields and that its component is
// declared and its Job is in the source inventory.
func (c *checker) procedures(inv Inventory) {
	seen := map[string]bool{}
	identities := 0
	for _, p := range c.m.Procedures {
		if p.ProbeIdentity {
			identities++
			if identities == 2 {
				c.add(RuleSchema, "mark exactly one procedure probe_identity: the one that provisions every probe user", "procedure %s is a second probe_identity procedure", p.ID)
			}
		}
		if seen[p.ID] {
			c.add(RuleSchema, "procedure IDs are unique", "procedure %q is declared more than once", p.ID)
			continue
		}
		seen[p.ID] = true
		converted, err := p.Program()
		if err == nil {
			err = converted.Validate()
		}
		if err != nil {
			c.add(RuleSchema, procedureHint, "%v", err)
			continue
		}
		if p.Component != "" {
			if _, ok := c.byName[p.Component]; !ok {
				c.add(RuleUnknownName, "component names a declared component of the map", "procedure %s: component %q is not a component", p.ID, p.Component)
			}
		}
		if p.Job != "" && !inv.Workloads[p.Job] {
			c.add(RuleUnknownName, "job is Job/<name> as listed in the manifest inventory above the map", "procedure %s: job %q is not in the accepted source inventory", p.ID, p.Job)
		}
	}
}

// ProgramProcedures converts the map's procedures, skipping any entry that does
// not convert; the memory check rejects those before they are accepted.
func (m Map) ProgramProcedures() []program.Procedure {
	var out []program.Procedure
	for _, p := range m.Procedures {
		if converted, err := p.Program(); err == nil {
			out = append(out, converted)
		}
	}
	return out
}

// ProbeIdentity returns the procedure marked probe_identity, converted to the
// SDK's form, and its component's workload (Kind/name). It reports false when
// the map has none, or it does not convert or names no declared component;
// the memory check rejects the latter before acceptance.
func (m Map) ProbeIdentity() (program.Procedure, string, bool) {
	for _, p := range m.Procedures {
		if !p.ProbeIdentity {
			continue
		}
		converted, err := p.Program()
		if err != nil || converted.Validate() != nil {
			return program.Procedure{}, "", false
		}
		for _, c := range m.Components {
			if c.Name == p.Component {
				return converted, c.Workload, true
			}
		}
		return program.Procedure{}, "", false
	}
	return program.Procedure{}, "", false
}

// checkProbeUsers reports scenario users the map cannot provision: no
// probe-identity procedure, or a name its username parameter rejects.
func (c *checker) checkProbeUsers(scenarios []Scenario) {
	identity, _, ok := c.m.ProbeIdentity()
	for _, s := range scenarios {
		if len(s.Users) == 0 {
			continue
		}
		if !ok {
			c.add(RuleSchema, "declare the application's own admin command that creates or approves a non-admin user as a procedure with probe_identity: true, or sign the probe up through self-service sign-up in Setup", "scenario %s signs in as probe users %s but the map has no probe_identity procedure", s.ID, strings.Join(s.Users, ", "))
			continue
		}
		for _, user := range s.Users {
			if _, err := identity.Bind(map[string]any{program.ProbeIdentityParam: user}); err != nil {
				c.add(RuleSchema, "name the probe user so the identity procedure's username parameter accepts it", "scenario %s: probe user %s: %v", s.ID, user, err)
			}
		}
	}
}
