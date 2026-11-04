package deployment

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		full := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func sourcePaths(dockerfile string) []string {
	var out []string
	for _, source := range parseCopySources(dockerfile) {
		out = append(out, source.Path)
	}
	return out
}

func TestParseCopySourcesKeepsOnlyDecidableContextSources(t *testing.T) {
	dockerfile := "# syntax=docker/dockerfile:1\n" +
		"FROM base AS build\n" +
		"COPY ./lua-thrift /tmp/lua-thrift\n" +
		"COPY nginx.conf \\\n  # comment inside a continuation\n  /etc/nginx/\n" +
		"COPY --chown=1:1 [\"a b/file\", \"/b.txt\", \"/dst/\"]\n" +
		"ADD https://example.com/x.tgz /x\n" +
		"COPY *.go ./\n" +
		"COPY pkg/*/gen.go ./\n" +
		"COPY ${SRC}/app /app\n" +
		"COPY --from=build /out /out\n" +
		"COPY --from=0 /out /out\n" +
		"RUN cat <<EOF\nCOPY inside-heredoc /x\nEOF\n" +
		"COPY <<EOT /etc/conf\ncontent\nEOT\n" +
		"COPY single-arg\n" +
		"COPY . .\n"
	want := []string{"lua-thrift", "nginx.conf", "a b/file", "b.txt"}
	if got := sourcePaths(dockerfile); !reflect.DeepEqual(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}
}

func TestParseCopySourcesHonoursEscapeDirective(t *testing.T) {
	got := sourcePaths("# escape=`\nFROM x\nCOPY a `\n  b /dst/\n")
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("sources = %v", got)
	}
}

func socialNetworkCheckout(t *testing.T) string {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"docker/openresty-thrift/xenial/Dockerfile":     "FROM ubuntu\nCOPY ./lua-thrift /tmp/lua-thrift\nCOPY nginx.vh.default.conf /etc/nginx/conf.d/\nCOPY --from=build /x /y\n",
		"docker/openresty-thrift/lua-thrift/a.lua":      "x",
		"docker/openresty-thrift/nginx.vh.default.conf": "x",
		"manifests/app.yaml":                            "x",
	})
	return dir
}

// The class behind L1b/L2b: a Dockerfile whose COPY sources are not under the
// chosen context. The deployer is told which directories hold them, before
// any build starts.
func TestPlanAdmissionRejectsMissingCopySourcesAndNamesTheContainingDirectory(t *testing.T) {
	checkout := socialNetworkCheckout(t)
	backend := &recordingBuilds{}
	_, err := (RawStrategy{Namespace: "demo", Backend: backend}).ApplyAndVerify(context.Background(), checkout, "tree", buildPlan(
		imageBuild("proxy", "docker/openresty-thrift/xenial", "docker/openresty-thrift/xenial/Dockerfile"),
	))
	if err == nil {
		t.Fatal("a context lacking the COPY sources must be rejected")
	}
	if len(backend.contexts) != 0 {
		t.Fatalf("no build may start: %v", backend.contexts)
	}
	for _, want := range []string{`"lua-thrift"`, `"nginx.vh.default.conf"`, "line 2", `"docker/openresty-thrift"`, "image_builds[0]", "no build was started"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("rejection %q lacks %q", err, want)
		}
	}
	if strings.Contains(err.Error(), `"/x"`) {
		t.Fatalf("--from sources must not be checked: %v", err)
	}
}

func TestPlanAdmissionAcceptsTheContextHoldingTheCopySources(t *testing.T) {
	checkout := socialNetworkCheckout(t)
	plan := buildPlan(imageBuild("proxy", "docker/openresty-thrift", "docker/openresty-thrift/xenial/Dockerfile"))
	if _, err := planImageBuilds(checkout, plan.ImageBuilds); err != nil {
		t.Fatalf("correct context rejected: %v", err)
	}
}

func TestCopySourceAdmissionAllowsWhatItCannotDecide(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"svc/Dockerfile":    "FROM x\nARG DIR\nCOPY ${DIR}/app /app\nCOPY *.missing /x/\nCOPY --from=builder /o /o\nADD https://example.com/a /a\nCOPY present.txt /p\nCOPY ignored.txt /i\n",
		"svc/present.txt":   "x",
		"svc/ignored.txt":   "x",
		"svc/.dockerignore": "ignored.txt\n",
	})
	if _, err := planImageBuilds(dir, buildPlan(imageBuild("a", "svc", "svc/Dockerfile")).ImageBuilds); err != nil {
		t.Fatalf("undecidable sources must be allowed: %v", err)
	}
}

func TestCopySourceAdmissionWithNoContainingDirectorySaysSo(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"svc/Dockerfile": "FROM x\nCOPY nowhere.txt /x\n"})
	_, err := planImageBuilds(dir, buildPlan(imageBuild("a", "svc", "svc/Dockerfile")).ImageBuilds)
	if err == nil || !strings.Contains(err.Error(), "No directory in the checkout contains all of them") {
		t.Fatalf("got %v", err)
	}
}
