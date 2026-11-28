package buildkit

import (
	"slices"
	"testing"
	"testing/fstest"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// A build reads its Dockerfile, the context's ignore files and the context
// paths its COPY and ADD instructions name; a change anywhere else in the
// context cannot change the image.
func TestBuildInputsAreTheDockerfileAndItsCopySources(t *testing.T) {
	fsys := fstest.MapFS{
		"src/currency/Dockerfile": {Data: []byte("FROM alpine AS b\nCOPY ./src/currency /currency\nCOPY pb/demo.proto \\\n  /currency/proto/\nCOPY --from=b /x /y\nADD https://example.invalid/x /z\nRUN make\n")},
		"src/currency/main.cc":    {Data: []byte("int main(){}")},
		"pb/demo.proto":           {Data: []byte("syntax")},
		"src/cart/x":              {Data: []byte("x")},
	}
	got, err := Builder{}.BuildInputs(fsys, &contracts.BuildKitImageConfig{ContextPath: ".", DockerfilePath: "src/currency/Dockerfile"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".dockerignore", "pb/demo.proto", "src/currency", "src/currency/Dockerfile", "src/currency/Dockerfile.dockerignore"}
	if !slices.Equal(got.Paths, want) {
		t.Fatalf("inputs %v, want %v", got.Paths, want)
	}
	for p, covered := range map[string]bool{"pb/demo.proto": true, "src/currency/main.cc": true, "src/cart/x": false, "pb/other.proto": false} {
		if got.Covers(p) != covered {
			t.Fatalf("Covers(%q) = %v, want %v", p, !covered, covered)
		}
	}
}

// Whatever the parser cannot decide (a copy of the whole context, a variable
// or wildcard source, a bind mount of the context) makes the whole context an
// input, so a change is never missed.
func TestBuildInputsFallBackToTheWholeContext(t *testing.T) {
	for name, dockerfile := range map[string]string{
		"whole context": "FROM alpine\nCOPY . /app\n",
		"wildcard":      "FROM alpine\nCOPY *.json /app/\n",
		"variable":      "FROM alpine\nARG D=svc\nCOPY $D /app\n",
		"bind mount":    "FROM alpine\nRUN --mount=type=bind,target=/src make -C /src\n",
		"default mount": "FROM alpine\nRUN --mount=target=/src make -C /src\n",
		"heredoc copy":  "FROM alpine\nCOPY <<EOF /app/x\nhello\nEOF\nCOPY svc /app\n",
	} {
		fsys := fstest.MapFS{"svc/Dockerfile": {Data: []byte(dockerfile)}, "svc/a.json": {Data: []byte("{}")}}
		got, err := Builder{}.BuildInputs(fsys, &contracts.BuildKitImageConfig{ContextPath: "svc", DockerfilePath: "svc/Dockerfile"})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Contains(got.Paths, "svc") || !got.Covers("svc/anything") {
			t.Fatalf("%s: inputs %v must cover the whole context svc", name, got.Paths)
		}
	}
	// A Dockerfile outside the context is an input too.
	fsys := fstest.MapFS{"build/api.Dockerfile": {Data: []byte("FROM alpine\nCOPY main.go /\n")}, "api/main.go": {Data: []byte("package main")}}
	got, err := Builder{}.BuildInputs(fsys, &contracts.BuildKitImageConfig{ContextPath: "api", DockerfilePath: "build/api.Dockerfile"})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Covers("build/api.Dockerfile") || !got.Covers("api/main.go") || got.Covers("api/other.go") {
		t.Fatalf("inputs %v", got.Paths)
	}
}
