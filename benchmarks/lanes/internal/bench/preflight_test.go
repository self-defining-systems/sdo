//go:build integration

package bench

import (
	"errors"
	"strings"
	"testing"
)

func TestParseCapacityAndJudge(t *testing.T) {
	out := "load5 20.5\ncores 40\nmemavail_kb 83886080\ndisk_staging_kb 1073741824\ndisk_docker_kb 20971520\ncontainer ph2-s1-control-plane\n"
	c, err := parseCapacity(out)
	must(t, err)
	if !c.Running["ph2-s1-control-plane"] || c.Cores != 40 {
		t.Fatalf("%+v", c)
	}
	if ok, d := c.judge(0, 1.0); !ok {
		t.Fatalf("no lanes to build must pass: %s", d)
	}
	// 80 GB memory fits 4 lanes of 16 GB; the docker disk has 20 GB free, below one lane's 40 GB.
	ok, d := c.judge(1, 1.0)
	if ok || !strings.Contains(d, "free disk") || strings.Contains(d, "MemAvailable 80 GB <") {
		t.Fatalf("want a disk failure only: %v %s", ok, d)
	}
	c.DiskDocker = -1
	if ok, d := c.judge(6, 1.0); ok || !strings.Contains(d, "MemAvailable") {
		t.Fatalf("6 lanes need 96 GB memory: %v %s", ok, d)
	}
	c.Load5 = 60
	if ok, d := c.judge(1, 1.0); ok || !strings.Contains(d, "load5 per core 1.50") {
		t.Fatalf("load: %v %s", ok, d)
	}
	if _, err := parseCapacity("load5 1\n"); err == nil {
		t.Fatal("an incomplete probe must fail")
	}
}

func TestCompareImages(t *testing.T) {
	v := compareImages(map[string]map[string]string{
		"a": {"x:1": "sha256:aaaaaaaaaaaa", "y:1": "sha256:bbbbbbbbbbbb"},
		"b": {"x:1": "sha256:aaaaaaaaaaaa", "y:1": "MISSING"},
		"c": {"x:1": "sha256:cccccccccccc", "y:1": "sha256:bbbbbbbbbbbb"},
	})
	got := map[string]hostVerdict{}
	for _, r := range v {
		got[r.host] = r
	}
	// x:1 has two ids over the hosts: every host that has it is told; y:1 is missing on b only.
	for _, h := range []string{"a", "b", "c"} {
		if got[h].ok || !strings.Contains(got[h].detail, "x:1 id") {
			t.Fatalf("%s: %+v", h, got[h])
		}
	}
	if !strings.Contains(got["b"].detail, "y:1 missing") || strings.Contains(got["a"].detail, "y:1") {
		t.Fatalf("%+v", got)
	}
}

func TestCompareImagesAllEqual(t *testing.T) {
	v := compareImages(map[string]map[string]string{"a": {"x": "i1"}, "b": {"x": "i1"}})
	if !v[0].ok || !v[1].ok {
		t.Fatalf("%+v", v)
	}
}

func TestKindImageMustMatchAcrossHosts(t *testing.T) {
	pinned := Host{Env: map[string]string{"KIND_NODE_IMAGE": "kindest/node:v1.35.0@sha256:x"}}
	if g := effectiveKindImage(pinned, "kind v0.30"); g != "kindest/node:v1.35.0@sha256:x" {
		t.Fatal(g)
	}
	same := compareKind(map[string]string{"a": "n1", "b": "n1"})
	if !same[0].ok || !same[1].ok {
		t.Fatalf("%+v", same)
	}
	diff := compareKind(map[string]string{"a": effectiveKindImage(Host{}, "kind v0.30"), "b": "kindest/node:v1.35.0"})
	if diff[0].ok || diff[1].ok || !strings.Contains(diff[0].detail, "kind-default (kind v0.30)") {
		t.Fatalf("%+v", diff)
	}
}

func TestPreloadKeepsTheBaseTheComparedImage(t *testing.T) {
	base := "kindest/node:v1.35.0@sha256:x"
	h := Host{Env: map[string]string{"KIND_NODE_IMAGE": base, "KIND_NODE_PRELOAD": "examples/gitlab"}}
	if g := effectiveKindImage(h, "kind v0.31"); g != base {
		t.Fatalf("preload changed the compared image: %s", g)
	}
	v := preloadNote(h, hostVerdict{"a", base, true})
	if !v.ok || !strings.Contains(v.detail, base) || !strings.Contains(v.detail, "preloaded") || !strings.Contains(v.detail, "unchanged") {
		t.Fatalf("%+v", v)
	}
	if v := preloadNote(Host{}, hostVerdict{"a", base, true}); v.detail != base {
		t.Fatalf("no preload must not change the verdict: %+v", v)
	}
	if v := preloadNote(Host{Env: map[string]string{"KIND_NODE_PRELOAD": "examples/gitlab"}}, hostVerdict{"a", "kind-default (x)", true}); v.ok {
		t.Fatalf("a preload without a pinned base must fail: %+v", v)
	}
}

func TestComparePack(t *testing.T) {
	same := "HEAD abcdef1234\nCOMMITTED\nb1 examples/g/a.yaml\nb2 examples/g/b.yaml\nWORKING\nb1 examples/g/a.yaml\nb2 examples/g/b.yaml\n"
	if ok, d := comparePack(same, "9b2e0ed"); !ok || !strings.Contains(d, "2 files") {
		t.Fatalf("%v %s", ok, d)
	}
	edited := strings.Replace(same, "WORKING\nb1", "WORKING\nzz", 1) + "b9 examples/g/c.yaml\n"
	ok, d := comparePack(edited, "9b2e0ed")
	if ok || !strings.Contains(d, "modified examples/g/a.yaml") || !strings.Contains(d, "extra examples/g/c.yaml") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, d := comparePack("NOCOMMIT\n", "deadbee"); ok || !strings.Contains(d, "deadbee") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, _ := comparePack("HEAD x\nCOMMITTED\nWORKING\n", "r"); ok {
		t.Fatal("an empty committed pack must fail")
	}
}

func TestJudgeCall(t *testing.T) {
	if ok, _ := judgeCall("warning: model catalog\nok\n", nil); !ok {
		t.Fatal("ok answer")
	}
	ok, d := judgeCall("You've hit your weekly limit - resets 8pm (UTC)\n", nil)
	if ok || !strings.Contains(d, "weekly limit") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, d := judgeCall("", errors.New("exit status 124")); ok || !strings.Contains(d, "124") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, _ := judgeCall("token expired\nok\ntoken expired", nil); ok {
		t.Fatal("the last line decides")
	}
}

func TestCredentialChecksDedupe(t *testing.T) {
	mk := func(name, provider, tag string) specFile {
		return specFile{Spec: Spec{Name: name, Operator: Operator{Hooks: "sdo", Revision: "1da0aac6", Settings: map[string]any{
			"provider": provider, "model": "m", "effort": "medium", "image_tag": tag}}}}
	}
	cs := credentialChecks([]specFile{mk("a", "claude", "t"), mk("b", "claude", "t"), mk("c", "codex", "t"),
		{Spec: Spec{Name: "n", Operator: Operator{Builtin: "noop"}}}})
	if len(cs) != 2 || cs[0].label() != "a,b" || cs[0].Image != "sdo-responder:t" || cs[0].AuthFile != "~/.claude/.credentials.json" ||
		cs[1].AuthFile != "~/.codex/auth.json" {
		t.Fatalf("%+v", cs)
	}
	s := cs[0].script()
	if !strings.Contains(s, `-v "$HOME/.claude/.credentials.json":/sdo/credentials/.credentials.json:ro`) || !strings.Contains(s, "--model ") || !strings.Contains(s, "sdo-responder:t") {
		t.Fatal(s)
	}
	if strings.Contains(s, "cat ") {
		t.Fatal("the credential file must never be read out")
	}
}

// A provider reached through another backend (Vertex: the agent's config.agent.extra_env, with the credential file at
// the path the agent pod mounts it) is checked with that environment and mount, not as a plain subscription login.
func TestCredentialCheckUsesTheAgentsExtraEnv(t *testing.T) {
	vertex := []any{"CLAUDE_CODE_USE_VERTEX=1", "CLOUD_ML_REGION=global", "ANTHROPIC_VERTEX_PROJECT_ID=p-1",
		"GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json"}
	mk := func(name string, env any) specFile {
		set := map[string]any{"provider": "claude", "model": "m", "image_tag": "t", "auth_file": "/home/u/.config/gcloud/adc.json"}
		if env != nil {
			set["config"] = map[string]any{"agent": map[string]any{"extra_env": env}}
		}
		return specFile{Spec: Spec{Name: name, Operator: Operator{Hooks: "sdo", Revision: "1da0aac6", Settings: set}}}
	}
	cs := credentialChecks([]specFile{mk("v", vertex), mk("plain", nil), mk("m", map[string]any{"CLAUDE_CODE_USE_VERTEX": "1"})})
	if len(cs) != 3 || len(cs[0].Env) != 4 || len(cs[1].Env) != 0 || len(cs[2].Env) != 1 || cs[2].Env[0] != "CLAUDE_CODE_USE_VERTEX=1" {
		t.Fatalf("checks with different agent env are distinct: %+v", cs)
	}
	s := cs[0].script()
	for _, want := range []string{
		`-v '/home/u/.config/gcloud/adc.json':/sdo/credentials/.credentials.json:ro`,
		`-e 'CLAUDE_CODE_USE_VERTEX=1'`, `-e 'ANTHROPIC_VERTEX_PROJECT_ID=p-1'`,
		`-e 'GOOGLE_APPLICATION_CREDENTIALS=/sdo/credentials/.credentials.json'`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("script lacks %s:\n%s", want, s)
		}
	}
	if strings.Contains(cs[1].script(), "-e 'CLAUDE_CODE_USE_VERTEX") {
		t.Error("a spec without extra_env got another spec's env")
	}
}

func TestRequiredImagesDefaultTag(t *testing.T) {
	h := Host{Env: map[string]string{"KIND_NODE_IMAGE": "kindest/node:v1"}}
	got := requiredImages(h, []specFile{{Spec: Spec{Operator: Operator{Hooks: "sdo", Revision: "1da0aac6f00"}}}}, legacySubject{})
	want := "kindest/node:v1 sdo-controller:fz-1da0aac6 sdo-responder:fz-1da0aac6 sdo-script-tools:fz-1da0aac6"
	if strings.Join(got, " ") != want {
		t.Fatal(got)
	}
}

func TestJudgeSnippetAndMemory(t *testing.T) {
	if ok, d := judgeSnippet("noise\nPREFLIGHT ok: 5 binding sources valid\n", nil); !ok || !strings.HasPrefix(d, "ok") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, d := judgeSnippet("PREFLIGHT rejected binding sources: suspect (x)\n", errors.New("exit status 1")); ok || !strings.Contains(d, "suspect") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, _ := judgeSnippet("Traceback\n", nil); ok {
		t.Fatal("no result line must fail")
	}
	if ok, _ := judgeMemory("HEAD abc123456789\nEXIT 0\n", nil, "abc123456789", nil); !ok {
		t.Fatal("clean check")
	}
	ok, d := judgeMemory("HEAD abc123456789\nCHECK .sdo/schema-version:1: schema.version: 7\nEXIT 1\n", nil, "abc123456789", nil)
	if ok || !strings.Contains(d, "schema.version") {
		t.Fatalf("%v %s", ok, d)
	}
	allowed := "HEAD abc123456789\nCHECK .sdo/diagnostics: telemetry.unbound[metrics-alerts]: x\nEXIT 1\n"
	if ok, d := judgeMemory(allowed, nil, "abc123456789", []string{"telemetry.unbound[metrics-alerts]"}); !ok || !strings.Contains(d, "1 allowed") {
		t.Fatalf("%v %s", ok, d)
	}
	if ok, _ := judgeMemory(allowed+"CHECK other\n", nil, "abc123456789", []string{"telemetry.unbound[metrics-alerts]"}); ok {
		t.Fatal("another diagnostic must still fail")
	}
	if ok, _ := judgeMemory("HEAD abc123456789\nEXIT 2\n", nil, "abc123456789", nil); ok {
		t.Fatal("a failure with no diagnostic must fail")
	}
	if ok, d := judgeMemory("HEAD def\nEXIT 0\n", nil, "abc123456789", nil); ok || !strings.Contains(d, "baseline.json says") {
		t.Fatalf("%v %s", ok, d)
	}
}

func TestPlannedLanesCapsAtClusters(t *testing.T) {
	inv := Inventory{Hosts: map[string]Host{"h": {Clusters: []string{"a", "b", "c"}}}}
	sp := []specFile{{Spec: Spec{Lanes: 2}}, {Spec: Spec{Lanes: 5}}}
	if n := plannedLanes(inv, "h", sp); n != 3 {
		t.Fatal(n)
	}
}

func TestJudgeSharedCluster(t *testing.T) {
	ok, d := judgeSharedCluster("node a Ready\nnode b NotReady\nnode c Ready\n", nil, 3)
	if !ok || !strings.Contains(d, "2 Ready") || !strings.Contains(d, "NotReady (lanes avoid them): b") {
		t.Fatalf("%v %q", ok, d)
	}
	if ok, _ := judgeSharedCluster("node a NotReady\n", nil, 1); ok {
		t.Fatal("no Ready node must fail")
	}
	if ok, _ := judgeSharedCluster("", nil, 1); ok {
		t.Fatal("an empty probe (unreadable kubeconfig) must fail")
	}
}
