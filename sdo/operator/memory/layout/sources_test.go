package layout

import (
	"strings"
	"testing"
)

const goodSources = "sources:\n- path: .\n  kind: git\n  config: {repository: \"git://synthetic.invalid/app.git\", ref: main, poll_interval: 30s}\n"

func TestClassifySourcesFile(t *testing.T) {
	kind, id, ok := Classify(SourcesFile)
	if !ok || kind != KindSources || id != "" {
		t.Fatalf("Classify(%s) = %v %q %v", SourcesFile, kind, id, ok)
	}
	for _, p := range []string{Dir + "/goal/other.yaml", Dir + "/goal/sources.yaml/x", Dir + "/goal", Dir + "/goal/a/sources.yaml"} {
		if _, _, ok := Classify(p); ok {
			t.Errorf("%s must not classify", p)
		}
	}
	if RequiresValidatorJob([]string{SourcesFile}) {
		t.Error("a sources change is inert and needs no validator Job")
	}
	if RequiresControllerRollout([]string{SourcesFile}) {
		t.Error("a sources change does not reload detectors")
	}
}

func TestParseSources(t *testing.T) {
	sources, err := ParseSources([]byte(goodSources))
	if err != nil {
		t.Fatal(err)
	}
	if len(sources.Sources) != 1 || sources.Sources[0].Kind != "git" || sources.Sources[0].Config["ref"] != "main" {
		t.Fatalf("parsed %+v", sources)
	}
	encoded, err := MarshalSources(sources)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseSources(encoded)
	if err != nil {
		t.Fatalf("round trip: %v\n%s", err, encoded)
	}
	if again.Sources[0].Config["poll_interval"] != "30s" {
		t.Fatalf("round trip lost config: %+v", again)
	}
}

func TestParseSourcesRejects(t *testing.T) {
	cfg := "  config: {repository: r, ref: main}\n"
	for name, tc := range map[string]struct{ text, want string }{
		"empty":          {"sources: []\n", "at least one"},
		"two sources":    {"sources:\n- {path: ., kind: git, config: {a: b}}\n- {path: ., kind: git, config: {a: b}}\n", "only one source"},
		"subtree mount":  {"sources:\n- path: sub\n  kind: git\n" + cfg, "only \".\""},
		"bad kind":       {"sources:\n- path: .\n  kind: Git!\n" + cfg, "kind"},
		"missing config": {"sources:\n- path: .\n  kind: git\n", "config is required"},
		"unknown field":  {"sources:\n- path: .\n  kind: git\n  extra: 1\n" + cfg, "extra"},
		"unknown top":    {"sources:\n- path: .\n  kind: git\n" + cfg + "other: 1\n", "other"},
		"duplicate key":  {"sources:\n- path: .\n  kind: git\n  kind: git\n" + cfg, "duplicate"},
		"not yaml":       {"sources: [\n", "YAML"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSources([]byte(tc.text))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseSourceConfigJSON(t *testing.T) {
	if _, err := ParseSourceConfigJSON(`{"repository":"r"}`); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{``, `[]`, `{}`, `{"a":1} {"b":2}`, `"x"`} {
		if _, err := ParseSourceConfigJSON(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
