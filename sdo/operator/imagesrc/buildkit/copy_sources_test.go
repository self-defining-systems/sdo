package buildkit

import (
	"os"
	"path/filepath"
	"reflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	_, err := PlanBuilds(checkout, []*contracts.ImageBuild{
		imageBuild("proxy", "docker/openresty-thrift/xenial", "docker/openresty-thrift/xenial/Dockerfile"),
	})
	if err == nil {
		t.Fatal("a context lacking the COPY sources must be rejected")
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
	plan := []*contracts.ImageBuild{imageBuild("proxy", "docker/openresty-thrift", "docker/openresty-thrift/xenial/Dockerfile")}
	if _, err := PlanBuilds(checkout, plan); err != nil {
		t.Fatalf("correct context rejected: %v", err)
	}
}

func TestCopySourceAdmissionAllowsWhatItCannotDecide(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"svc/Dockerfile":    "FROM x\nARG DIR\nCOPY ${DIR}/app /app\nCOPY *.missing /x/\nCOPY --from=builder /o /o\nADD https://example.com/a /a\nCOPY present.txt /p\n",
		"svc/present.txt":   "x",
		"svc/.dockerignore": "ignored.txt\n",
	})
	if _, err := PlanBuilds(dir, []*contracts.ImageBuild{imageBuild("a", "svc", "svc/Dockerfile")}); err != nil {
		t.Fatalf("undecidable sources must be allowed: %v", err)
	}
}

func TestCopySourceAdmissionWithNoContainingDirectorySaysSo(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"svc/Dockerfile": "FROM x\nCOPY nowhere.txt /x\n"})
	_, err := PlanBuilds(dir, []*contracts.ImageBuild{imageBuild("a", "svc", "svc/Dockerfile")})
	if err == nil || !strings.Contains(err.Error(), "No directory in the checkout contains all of them") {
		t.Fatalf("got %v", err)
	}
}

func TestPlanAdmissionFlagsCopySourcesExcludedByDockerignore(t *testing.T) {
	cases := []struct {
		name, ignore, dockerfileIgnore string
		reject                         bool
	}{
		{"exact file", "conf/app.conf\n", "", true},
		{"glob", "conf/*.conf\n", "", true},
		{"parent directory", "conf\n", "", true},
		{"double star", "**/app.conf\n", "", true},
		{"negated exception keeps it", "conf\n!conf/app.conf\n", "", false},
		{"unrelated pattern", "*.log\nnode_modules\n# conf\n", "", false},
		{"dockerfile-specific ignore wins", "conf\n", "node_modules\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string]string{"svc/Dockerfile": "FROM x\nCOPY conf/app.conf /etc/app.conf\n", "svc/conf/app.conf": "x", "svc/.dockerignore": tc.ignore}
			if tc.dockerfileIgnore != "" {
				files["svc/Dockerfile.dockerignore"] = tc.dockerfileIgnore
			}
			writeFiles(t, dir, files)
			_, err := PlanBuilds(dir, []*contracts.ImageBuild{imageBuild("a", "svc", "svc/Dockerfile")})
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%t, got %v", tc.reject, err)
			}
			if err != nil {
				for _, want := range []string{"excluded by", ".dockerignore", `"conf/app.conf"`, "do not change context_path"} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("rejection lacks %q: %v", want, err)
					}
				}
				if strings.Contains(err.Error(), "do not exist") {
					t.Fatalf("an existing file is excluded, not missing: %v", err)
				}
			}
		})
	}
}
