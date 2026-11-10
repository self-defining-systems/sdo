package datacheck

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
)

// Rule IDs of data-check findings.
const (
	RuleShape       = "data-check.shape"
	RuleBounds      = "data-check.bounds"
	RuleReference   = "data-check.reference"
	RuleEquivalence = "data-check.equivalence"
	RuleKeyType     = "data-check.key-type"
	RuleTooFew      = "data-check.too-few-records"
)

// Watch is the input every data check consumes: its reads target Pods.
var Watch = sdk.WatchKind{APIVersion: "v1", Kind: "Pod"}

// maxExamples bounds the records a finding names.
const maxExamples = 5

// QueryID is the pod-read ID of a check's read; LookupQueryID of its lookup.
func QueryID(id string) string       { return "data-check-" + id }
func LookupQueryID(id string) string { return "data-check-" + id + "-lookup" }

type detector struct {
	spec   sdk.DetectorSpec
	id     string
	check  Check
	reads  []podread.Query
	broken error
}

// NewDetector returns the deterministic detector of the data check id, whose
// YAML text is source. The detector declares the check's reads and judges the
// frozen readings: one finding per violated assertion and store Pod, naming at
// most five records and why, never a stored value beyond a key. A missing,
// failed, truncated or unparsable read is a detector error, never a clear. A
// check that does not validate yields a detector that always errors.
func NewDetector(spec sdk.DetectorSpec, id, source string) sdk.Detector {
	if !slices.Contains(spec.Watches, Watch) {
		spec.Watches = append(slices.Clone(spec.Watches), Watch)
	}
	d := detector{spec: spec, id: id}
	check, err := Parse([]byte(source))
	if err == nil {
		err = check.Validate(id)
	}
	if err != nil {
		d.broken = fmt.Errorf("data check %s: %w", id, err)
		return d
	}
	d.check = check
	d.reads = check.Reads(id)
	return d
}

// Reads returns the pod reads of the check: its read, then its lookup.
func (c Check) Reads(id string) []podread.Query {
	interval, _ := c.interval()
	keys, first := c.Read.IDs, c.Read.First
	if r := c.Read.Range; r != nil {
		keys = nil
		for n := r.From; n <= r.To; n++ {
			keys = append(keys, fmt.Sprint(n))
		}
	}
	if keys == nil && first == 0 {
		keys = []string{}
	}
	keyPath, keyIDs := "", false
	for _, a := range c.Assert {
		if a.Reference != nil {
			keyPath = a.Reference.Field
		}
		if len(a.Equivalence) > 0 {
			keyIDs = true
		}
	}
	main := podread.Query{
		ID: QueryID(id), Interval: interval,
		Target: podread.Target{Labels: c.Target.Labels, Image: c.Target.Image},
		Args:   c.Store.command(c.Store.fields(c.pathsOf(c.Store)), keys, first, keyPath, keyIDs, false),
	}
	reads := []podread.Query{main}
	if c.Lookup != nil {
		l := *c.Lookup
		reads = append(reads, podread.Query{
			ID: LookupQueryID(id), Interval: interval,
			Target: podread.Target{Labels: l.Target.Labels, Image: l.Target.Image},
			Args:   l.command(l.fields(c.pathsOf(l)), nil, 0, "", false, true),
			Keys:   &podread.Keys{From: main.ID, Prefix: keyPrefix, Max: podread.MaxKeys},
		})
	}
	return reads
}

func (d detector) Spec() sdk.DetectorSpec { return d.spec }

// PodReads implements podread.Consumer.
func (d detector) PodReads() []podread.Query { return slices.Clone(d.reads) }

// violation is one record that breaks an assertion, and why.
type violation struct{ id, reason string }

func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	if d.broken != nil {
		return nil, d.broken
	}
	result, err := podread.Require(ctx, QueryID(d.id))
	if err != nil {
		return nil, fmt.Errorf("data check %s: %w", d.id, err)
	}
	var lookup *readout
	if d.check.Lookup != nil {
		lookupResult, err := podread.Require(ctx, LookupQueryID(d.id))
		if err != nil {
			return nil, fmt.Errorf("data check %s lookup: %w", d.id, err)
		}
		merged := readout{WrongType: map[string]string{}, Queried: map[string]bool{}}
		for _, reading := range lookupResult.Readings {
			got, err := parseOutput(*d.check.Lookup, reading.Stdout)
			if err != nil {
				return nil, fmt.Errorf("data check %s lookup Pod %s: %w", d.id, reading.Pod, err)
			}
			merged.Records = append(merged.Records, got.Records...)
			for k := range got.Queried {
				merged.Queried[k] = true
			}
		}
		lookup = &merged
	}
	var findings []sdk.Finding
	for _, reading := range result.Readings {
		got, err := parseOutput(d.check.Store, reading.Stdout)
		if err != nil {
			return nil, fmt.Errorf("data check %s Pod %s: %w", d.id, reading.Pod, err)
		}
		findings = append(findings, d.judge(reading.Ref(), got, lookup)...)
	}
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}

// judge evaluates every assertion over one read.
func (d detector) judge(pod sdk.ObjectRef, got readout, lookup *readout) []sdk.Finding {
	var findings []sdk.Finding
	sampled := len(got.Records) + len(got.WrongType)
	if sampled < d.check.minRecords() {
		findings = append(findings, d.finding(pod, RuleTooFew, -1, sampled, nil,
			fmt.Sprintf("data check %s: store %s holds %d of the at least %d sampled records it should", d.id, d.check.Component, sampled, d.check.minRecords())))
	}
	if len(got.WrongType) > 0 {
		var v []violation
		for id, t := range got.WrongType {
			v = append(v, violation{id, fmt.Sprintf("holds a %s, not a %s", t, d.check.Read.Type)})
		}
		findings = append(findings, d.violations(pod, RuleKeyType, -1, sampled, v, "keys hold the wrong type"))
	}
	for i, a := range d.check.Assert {
		var v []violation
		var rule, what string
		switch a.Kind() {
		case KindShape:
			rule, what = RuleShape, "the expected shape"
			v = shapeViolations(got.Records, a.Shape)
		case KindBounds:
			rule, what = RuleBounds, "bounds on "+a.Bounds.Field
			v = boundsViolations(got.Records, *a.Bounds)
		case KindReference:
			rule, what = RuleReference, a.Reference.Field+" resolving in "+d.check.Lookup.Component
			v = referenceViolations(got.Records, a.Reference.Field, *lookup, d.check.Lookup.Component)
		case KindEquivalence:
			rule, what = RuleEquivalence, "equality with its source in "+d.check.Lookup.Component
			v = equivalenceViolations(got.Records, a.Equivalence, *lookup, d.check.Lookup.Component)
		}
		if len(v) > 0 {
			findings = append(findings, d.violations(pod, rule, i, sampled, v, what))
		}
	}
	return findings
}

func shapeViolations(records []record, shape map[string]string) []violation {
	paths := make([]string, 0, len(shape))
	for p := range shape {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var out []violation
	for _, r := range records {
		if r.Undecodable {
			out = append(out, violation{r.ID, "value is not JSON"})
			continue
		}
		if object, ok := r.Value.(map[string]any); ok && marker(object) != "" {
			continue // a capped document is unchecked
		}
		for _, p := range paths {
			want := shape[p]
			parent, each := strings.CutSuffix(p, "[]")
			found := values(r.Value, parent)
			if len(found) == 0 {
				out = append(out, violation{r.ID, p + " is missing"})
				break
			}
			if each {
				found = values(r.Value, p)
			}
			if bad := slices.IndexFunc(found, func(v any) bool { return !matches(v, want) }); bad >= 0 {
				out = append(out, violation{r.ID, fmt.Sprintf("%s is %s, not %s", p, typeOf(found[bad]), want)})
				break
			}
		}
	}
	return out
}

func boundsViolations(records []record, b Bounds) []violation {
	var out []violation
	for _, r := range records {
		for _, v := range values(r.Value, b.Field) {
			var n float64
			ok := true
			if b.Count {
				switch x := v.(type) {
				case []any:
					n = float64(len(x))
				case map[string]any:
					n, ok = number(x["$size"])
				default:
					ok = false
				}
				if object, isObject := r.Value.(map[string]any); isObject && b.Field == "members" {
					if size, has := number(object["size"]); has {
						n, ok = size, true
					}
				}
			} else {
				n, ok = number(v)
			}
			switch {
			case !ok:
				out = append(out, violation{r.ID, b.Field + " is not a number"})
			case b.Min != nil && n < *b.Min:
				out = append(out, violation{r.ID, fmt.Sprintf("%s is below %g", b.Field, *b.Min)})
			case b.Max != nil && n > *b.Max:
				out = append(out, violation{r.ID, fmt.Sprintf("%s is above %g", b.Field, *b.Max)})
			default:
				continue
			}
			break
		}
	}
	return out
}

func found(lookup readout) map[string]any {
	out := map[string]any{}
	for _, r := range lookup.Records {
		out[r.ID] = r.Value
	}
	return out
}

func referenceViolations(records []record, field string, lookup readout, component string) []violation {
	present := found(lookup)
	var out []violation
	for _, r := range records {
		for _, v := range values(r.Value, field) {
			key, ok := scalar(v)
			if !ok || !lookup.Queried[key] {
				continue // not looked up this cycle: unchecked
			}
			if _, ok := present[key]; !ok {
				out = append(out, violation{r.ID, fmt.Sprintf("%s %s does not resolve in %s", field, key, component)})
				break
			}
		}
	}
	return out
}

func equivalenceViolations(records []record, pairs []FieldPair, lookup readout, component string) []violation {
	source := found(lookup)
	var out []violation
	for _, r := range records {
		if !lookup.Queried[r.ID] {
			continue
		}
		s, ok := source[r.ID]
		if !ok {
			out = append(out, violation{r.ID, "has no source record in " + component})
			continue
		}
		for _, pair := range pairs {
			if reason := differs(r.Value, pair.Field, s, pair.Source); reason != "" {
				out = append(out, violation{r.ID, reason})
				break
			}
		}
	}
	return out
}

// differs compares one field pair and says how they differ, without values:
// sets by the count of members missing and extra, single values by equality.
func differs(record any, field string, source any, sourceField string) string {
	if strings.Contains(field, "[]") {
		a, okA := texts(record, field)
		b, okB := texts(source, sourceField)
		if !okA || !okB {
			return ""
		}
		missing, extra := 0, 0
		for _, x := range b {
			if _, found := slices.BinarySearch(a, x); !found {
				missing++
			}
		}
		for _, x := range a {
			if _, found := slices.BinarySearch(b, x); !found {
				extra++
			}
		}
		if missing+extra == 0 {
			return ""
		}
		return fmt.Sprintf("%s differs from %s in the source: %d missing, %d extra", field, sourceField, missing, extra)
	}
	a, b := values(record, field), values(source, sourceField)
	if partial(record, field) || partial(source, sourceField) {
		return ""
	}
	switch {
	case len(a) == 0 && len(b) == 0:
		return ""
	case len(a) == 0:
		return field + " is missing; the source has " + sourceField
	case len(b) == 0:
		return sourceField + " is missing in the source"
	}
	textA, okA := scalar(a[0])
	textB, okB := scalar(b[0])
	if okA && okB {
		if textA != textB {
			return fmt.Sprintf("%s differs from %s in the source", field, sourceField)
		}
		return ""
	}
	if canonical(a[0]) != canonical(b[0]) {
		return fmt.Sprintf("%s differs from %s in the source", field, sourceField)
	}
	return ""
}

func (d detector) violations(pod sdk.ObjectRef, rule string, index, sampled int, v []violation, what string) sdk.Finding {
	sort.Slice(v, func(i, j int) bool { return v[i].id < v[j].id })
	ids := map[string]bool{}
	for _, x := range v {
		ids[x.id] = true
	}
	summary := fmt.Sprintf("data check %s: %d of %d sampled records in %s break %s", d.id, len(ids), sampled, d.check.Component, what)
	return d.finding(pod, rule, index, sampled, v, summary)
}

func (d detector) finding(pod sdk.ObjectRef, rule string, index, sampled int, v []violation, summary string) sdk.Finding {
	examples := make([]string, 0, maxExamples)
	keys := make([]string, 0, len(v))
	for i, x := range v {
		if i < maxExamples {
			examples = append(examples, x.id+": "+x.reason)
		}
		keys = append(keys, x.id)
	}
	evidence := fmt.Sprintf("Pod %s, %d records sampled", pod.Name, sampled)
	if len(examples) > 0 {
		evidence += "; " + strings.Join(examples, "; ")
	}
	if len(v) > maxExamples {
		evidence += fmt.Sprintf("; and %d more", len(v)-maxExamples)
	}
	return sdk.Finding{
		DetectorID: d.spec.ID, RuleID: rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
		Summary: summary, Evidence: evidence, PrimaryResource: pod,
		Playbooks:   slices.Clone(d.spec.Playbooks),
		Fingerprint: fmt.Sprintf("%s/%s/%d/%s/%s", d.spec.ID, rule, index, pod.Namespace, pod.Name),
		Metadata:    map[string]any{"check": d.id, "component": d.check.Component, "engine": string(d.check.Engine), "assertion": index, "records": slices.Compact(keys), "sampled": sampled},
	}
}
