package datacheck_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/datacheck"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
)

// Synthetic stores: an authoritative document store of items and a derived
// cache of item groups. The names are invented for these tests.
const itemShape = `component: item-store
engine: mongodb
target: {labels: {app: item-store}}
read: {database: catalog, collection: items, key: item_id, key_type: number, first: 4}
assert:
  - shape: {item_id: integer, owner.user_id: integer, title: string, "tags[]": string}
  - bounds: {field: stock, min: 0}
`

const groupReference = `component: group-cache
engine: redis
target: {labels: {app: group-cache}}
read: {pattern: "group:{id}", type: zset, first: 8}
lookup:
  component: item-store
  engine: mongodb
  target: {labels: {app: item-store}}
  read: {database: catalog, collection: items, key: item_id, key_type: number}
assert:
  - shape: {"members[]": digits}
  - reference: {field: "members[]"}
`

const memberEquivalence = `component: member-cache
engine: redis
target: {labels: {app: member-cache}}
read: {pattern: "{id}:members", type: zset, first: 8}
min_records: 0
lookup:
  component: member-store
  engine: mongodb
  target: {labels: {app: member-store}}
  read: {database: graph, collection: members, key: owner_id, key_type: number}
assert:
  - equivalence: [{field: "members[]", source: "members[].user_id"}]
`

func spec(id string) sdk.DetectorSpec {
	return sdk.DetectorSpec{ID: "data-" + id, Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: time.Minute, Playbooks: []string{"health-objective"}}
}

func detector(t *testing.T, id, source string) sdk.Detector {
	t.Helper()
	check, err := datacheck.Parse([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	if err := check.Validate(id); err != nil {
		t.Fatal(err)
	}
	d := datacheck.NewDetector(spec(id), id, source)
	if err := podread.ValidateDetector(d); err != nil {
		t.Fatalf("pod reads: %v", err)
	}
	return d
}

func reading(pod, stdout string) podread.Reading {
	return podread.Reading{Namespace: "shop", Pod: pod, Container: "store", ObservedAt: time.Unix(100, 0).UTC(), Stdout: stdout}
}

// view freezes one reading per declared read, in declaration order.
func view(t *testing.T, d sdk.Detector, stdout ...string) sdktest.Snapshot {
	t.Helper()
	reads := d.(podread.Consumer).PodReads()
	if len(reads) != len(stdout) {
		t.Fatalf("detector declares %d reads, test gives %d answers", len(reads), len(stdout))
	}
	results := map[string]podread.Result{}
	for i, q := range reads {
		results[q.ID] = podread.Result{Query: q, Readings: []podread.Reading{reading(q.ID+"-0", stdout[i])}}
	}
	return sdktest.Snapshot{PodReads: results}
}

func detect(t *testing.T, d sdk.Detector, stdout ...string) []sdk.Finding {
	t.Helper()
	findings, err := d.Detect(context.Background(), view(t, d, stdout...))
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func rules(findings []sdk.Finding) string {
	var out []string
	for _, f := range findings {
		out = append(out, f.RuleID)
	}
	return strings.Join(out, ",")
}

const healthyItems = "R\t1\t{\"item_id\":1,\"owner\":{\"user_id\":7},\"title\":\"a\",\"tags\":[\"x\"],\"stock\":3}\n" +
	"R\t2\t{\"item_id\":2,\"owner\":{\"user_id\":8},\"title\":\"b\",\"tags\":[],\"stock\":0}\nE\n"

func TestHealthySampleClears(t *testing.T) {
	if findings := detect(t, detector(t, "items", itemShape), healthyItems); len(findings) != 0 {
		t.Fatalf("healthy sample: %+v", findings)
	}
}

func TestShapeNamesRenamedAndRetypedFieldsWithoutValues(t *testing.T) {
	stdout := "R\t1\t{\"item_id\":1,\"owner_legacy\":{\"user_id\":7},\"title\":\"a\",\"tags\":[],\"stock\":1}\n" +
		"R\t2\t{\"item_id\":2,\"owner\":{\"user_id\":8},\"title\":12,\"tags\":[],\"stock\":1}\n" +
		"R\t3\t{\"item_id\":3,\"owner\":{\"user_id\":9},\"title\":\"secret text\",\"tags\":[\"x\",5],\"stock\":1}\nE\n"
	findings := detect(t, detector(t, "items", itemShape), stdout)
	if rules(findings) != datacheck.RuleShape {
		t.Fatalf("findings = %s", rules(findings))
	}
	f := findings[0]
	for _, want := range []string{"1: owner.user_id is missing", "2: title is integer, not string", "3: tags[] is integer, not string", "3 records sampled"} {
		if !strings.Contains(f.Evidence, want) {
			t.Fatalf("evidence %q lacks %q", f.Evidence, want)
		}
	}
	if strings.Contains(f.Evidence, "secret text") || f.PrimaryResource.Kind != "Pod" || f.Severity != sdk.SeverityCritical {
		t.Fatalf("finding = %+v", f)
	}
	if !strings.Contains(f.Summary, "3 of 3 sampled records in item-store") {
		t.Fatalf("summary = %q", f.Summary)
	}
}

func TestBoundsAndEmptySamples(t *testing.T) {
	d := detector(t, "items", itemShape)
	negative := strings.Replace(healthyItems, `"stock":0`, `"stock":-4`, 1)
	if got := rules(detect(t, d, negative)); got != datacheck.RuleBounds {
		t.Fatalf("negative stock: %s", got)
	}
	// A renamed collection reads as empty: fewer records than the check needs.
	if got := rules(detect(t, d, "E\n")); got != datacheck.RuleTooFew {
		t.Fatalf("empty sample: %s", got)
	}
}

func TestReferenceFindsDanglingMembers(t *testing.T) {
	d := detector(t, "groups", groupReference)
	groups := "R\t1\t{\"members\":[\"9999\",\"1\"],\"size\":2}\nR\t2\t{\"members\":[\"2\"],\"size\":2}\nK\t9999\nK\t2\nK\t1\nE\n"
	items := "Q\t9999\nQ\t2\nQ\t1\nR\t1\t{\"item_id\":1}\nR\t2\t{\"item_id\":2}\nE\n"
	findings := detect(t, d, groups, items)
	if rules(findings) != datacheck.RuleReference || !strings.Contains(findings[0].Evidence, "1: members[] 9999 does not resolve in item-store") {
		t.Fatalf("findings = %+v", findings)
	}
	// A key the lookup was not asked for this cycle is unchecked, not missing.
	partial := "Q\t2\nQ\t1\nR\t1\t{\"item_id\":1}\nR\t2\t{\"item_id\":2}\nE\n"
	if findings := detect(t, d, groups, partial); len(findings) != 0 {
		t.Fatalf("unchecked key reported: %+v", findings)
	}
	poisoned := "R\t1\t{\"members\":[\"x-1\"],\"size\":1}\nK\tx-1\nE\n"
	if got := rules(detect(t, d, poisoned, "Q\tx-1\nE\n")); got != datacheck.RuleReference+","+datacheck.RuleShape {
		t.Fatalf("poisoned member: %s", got)
	}
}

func TestEquivalenceComparesADerivedCopyWithItsSource(t *testing.T) {
	d := detector(t, "members", memberEquivalence)
	cache := "R\t1\t{\"members\":[\"3\",\"999\"],\"size\":2}\nR\t2\t{\"members\":[\"4\"],\"size\":1}\nR\t3\t{\"members\":[\"5\"],\"size\":9}\nK\t1\nK\t2\nK\t3\nE\n"
	source := "Q\t1\nQ\t2\nQ\t3\nR\t1\t{\"owner_id\":1,\"members\":[{\"user_id\":2},{\"user_id\":3}]}\nR\t2\t{\"owner_id\":2,\"members\":[{\"user_id\":4}]}\nR\t3\t{\"owner_id\":3,\"members\":[]}\nE\n"
	findings := detect(t, d, cache, source)
	if rules(findings) != datacheck.RuleEquivalence {
		t.Fatalf("findings = %+v", findings)
	}
	// Record 3 lists fewer members than its size: unchecked, not stale.
	if f := findings[0]; !strings.Contains(f.Evidence, "1: members[] differs from members[].user_id in the source: 1 missing, 1 extra") || strings.Contains(f.Evidence, "3:") {
		t.Fatalf("evidence = %q", f.Evidence)
	}
	orphan := "Q\t1\nQ\t2\nQ\t3\nR\t2\t{\"owner_id\":2,\"members\":[{\"user_id\":4}]}\nE\n"
	if f := detect(t, d, cache, orphan); len(f) != 1 || !strings.Contains(f[0].Evidence, "1: has no source record in member-store") {
		t.Fatalf("orphan cache entry: %+v", f)
	}
	// An empty cache is allowed by min_records: 0.
	if f := detect(t, d, "E\n", "E\n"); len(f) != 0 {
		t.Fatalf("empty cache: %+v", f)
	}
}

func TestCachedDocumentsDecodeOrFailTheirShape(t *testing.T) {
	const source = `component: profile-cache
engine: memcached
target: {image: memcached}
read: {pattern: "{id}", decode: json, range: {from: 1, to: 3}}
min_records: 0
assert:
  - shape: {address.city: string}
`
	d := detector(t, "profiles", source)
	good := `{"address":{"city":"a"}}`
	missing := `{"name":"b"}`
	garbage := `garbage`
	out := "VALUE 1 0 " + itoa(len(good)) + "\r\n" + good + "\r\n" +
		"VALUE 2 0 " + itoa(len(missing)) + "\r\n" + missing + "\r\n" +
		"VALUE 3 0 " + itoa(len(garbage)) + "\r\n" + garbage + "\r\nEND\r\n\nE\n"
	findings := detect(t, d, out)
	if rules(findings) != datacheck.RuleShape {
		t.Fatalf("findings = %+v", findings)
	}
	if e := findings[0].Evidence; !strings.Contains(e, "2: address.city is missing") || !strings.Contains(e, "3: value is not JSON") {
		t.Fatalf("evidence = %q", e)
	}
	if _, err := d.Detect(context.Background(), view(t, d, "VALUE 1 0 99\r\n{\"addr\n")); err == nil {
		t.Fatal("a value cut at the read bound cleared")
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

func TestWrongKeyTypeIsAFinding(t *testing.T) {
	d := detector(t, "groups", groupReference)
	findings := detect(t, d, "W\t1\thash\nE\n", "E\n")
	if rules(findings) != datacheck.RuleKeyType || !strings.Contains(findings[0].Evidence, "1: holds a hash, not a zset") {
		t.Fatalf("findings = %+v", findings)
	}
}

func TestUnavailableReadsNeverClear(t *testing.T) {
	d := detector(t, "groups", groupReference)
	for name, answers := range map[string][2]string{
		"no end marker":    {"R\t1\t{\"members\":[],\"size\":0}\n", "E\n"},
		"store refused":    {"ERR NOAUTH Authentication required.\n", "E\n"},
		"cli error":        {"(error) ERR unknown command\n", "E\n"},
		"lookup refused":   {"E\n", "ERR not authorized\n"},
		"malformed record": {"R\t1\t{broken\nE\n", "E\n"},
	} {
		if _, err := d.Detect(context.Background(), view(t, d, answers[0], answers[1])); err == nil {
			t.Fatalf("%s: cleared", name)
		}
	}
	if _, err := d.Detect(context.Background(), sdktest.Snapshot{}); err == nil {
		t.Fatal("a missing read cleared")
	}
	failed := view(t, d, "E\n", "E\n")
	lookup := failed.PodReads[datacheck.LookupQueryID("groups")]
	lookup.Error, lookup.Readings = "keys from data-check-groups: no readings", nil
	failed.PodReads[datacheck.LookupQueryID("groups")] = lookup
	if _, err := d.Detect(context.Background(), failed); err == nil {
		t.Fatal("a missing lookup cleared")
	}
	broken := datacheck.NewDetector(spec("broken"), "broken", "component: x\n")
	if _, err := broken.Detect(context.Background(), sdktest.Snapshot{}); err == nil {
		t.Fatal("an invalid check cleared")
	}
}
