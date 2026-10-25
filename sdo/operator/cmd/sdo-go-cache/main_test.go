package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOverlayReadsSeedAndWritesOnlyScratch(t *testing.T) {
	seed, scratch := t.TempDir(), t.TempDir()
	action := sha256.Sum256([]byte("action"))
	body := []byte("compiled SDK")
	output := sha256.Sum256(body)
	req := request{ID: 1, Command: "put", ActionID: action[:], OutputID: output[:], BodySize: int64(len(body))}
	c := cache{writable: seed}
	if _, err := c.put(req, body); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(cachePath(seed, action[:], "a"))
	if err != nil {
		t.Fatal(err)
	}
	c = cache{seed: seed, writable: scratch}
	got := c.get(action[:])
	if got.Miss || got.DiskPath != cachePath(seed, output[:], "d") {
		t.Fatalf("seed was not used in place: %+v", got)
	}
	if entries, _ := os.ReadDir(scratch); len(entries) != 0 {
		t.Fatal("seed read wrote scratch")
	}
	newBody := []byte("changed app")
	newOutput := sha256.Sum256(newBody)
	req.OutputID, req.BodySize = newOutput[:], int64(len(newBody))
	if _, err := c.put(req, newBody); err != nil {
		t.Fatal(err)
	}
	got = c.get(action[:])
	if got.Miss || got.DiskPath != cachePath(scratch, newOutput[:], "d") {
		t.Fatalf("private write not visible: %+v", got)
	}
	after, _ := os.ReadFile(cachePath(seed, action[:], "a"))
	if !bytes.Equal(before, after) {
		t.Fatal("immutable seed changed")
	}
	if _, err := os.Stat(cachePath(seed, newOutput[:], "d")); !os.IsNotExist(err) {
		t.Fatal("private output leaked into seed")
	}
}

func TestProtocolEmptyBodyAndMiss(t *testing.T) {
	action, output := sha256.Sum256([]byte("empty")), sha256.Sum256(nil)
	var input, result bytes.Buffer
	enc := json.NewEncoder(&input)
	for _, r := range []request{{ID: 1, Command: "get", ActionID: action[:]}, {ID: 2, Command: "put", ActionID: action[:], OutputID: output[:]}, {ID: 3, Command: "get", ActionID: action[:]}, {ID: 4, Command: "close"}} {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := serve(&input, &result, cache{writable: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&result)
	var responses []response
	for dec.More() {
		var r response
		if err := dec.Decode(&r); err != nil {
			t.Fatal(err)
		}
		responses = append(responses, r)
	}
	if len(responses) != 5 || !responses[1].Miss || responses[3].Miss || responses[3].DiskPath == "" {
		t.Fatalf("bad protocol: %+v", responses)
	}
}

func TestCorruptOrAbsentSeedEntriesMiss(t *testing.T) {
	root := t.TempDir()
	action := sha256.Sum256([]byte("action"))
	p := cachePath(root, action[:], "a")
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"", "v2 bogus", "v1 ../../escape invalid 3 1\n"} {
		if err := os.WriteFile(p, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if got := (cache{seed: root}).get(action[:]); !got.Miss {
			t.Fatalf("corruption hit: %+v", got)
		}
	}
}

// Real go commands verify compatibility with the pinned toolchain's cache, not
// just our own encoder. An app source edit must reuse immutable dependencies.
func TestRealGoReusesReadOnlySeedWithoutCopying(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	helper := filepath.Join(t.TempDir(), "sdo-go-cache")
	command := exec.CommandContext(ctx, "go", "build", "-o", helper, ".")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build helper: %v: %s", err, output)
	}
	app, seed, scratch := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(app, "go.mod"), []byte("module fixture\n\ngo 1.26\n"), 0600); err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(app, "main.go")
	if err := os.WriteFile(main, []byte("package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"before\")}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	run := func(cacheRoot, cacheProg string) string {
		t.Helper()
		command := exec.CommandContext(ctx, "go", "build", "-x", "-buildvcs=false", "-o", filepath.Join(app, "app"), ".")
		command.Dir = app
		command.Env = append(os.Environ(), "GOCACHE="+cacheRoot, "GOCACHEPROG="+cacheProg, "SDO_GO_CACHE_SEED="+seed, "GOPROXY=off", "GOSUMDB=off", "CGO_ENABLED=0")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("real Go build: %v: %s", err, output)
		}
		return string(output)
	}
	cold := run(seed, "")
	if !strings.Contains(cold, "/compile ") {
		t.Fatal("negative control did not compile cold dependencies")
	}
	before := treeDigest(t, seed)
	if err := os.WriteFile(main, []byte("package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"changed\")}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	warm := run(scratch, helper)
	if strings.Contains(warm, "-p fmt ") || strings.Contains(warm, "-p runtime ") {
		t.Fatalf("immutable standard-library dependencies recompiled: %s", warm)
	}
	if !strings.Contains(warm, "-p main ") {
		t.Fatal("changed application was not compiled")
	}
	if before != treeDigest(t, seed) {
		t.Fatal("read-through build changed immutable seed")
	}
	output, err := exec.CommandContext(ctx, filepath.Join(app, "app")).CombinedOutput()
	if err != nil || string(output) != "changed\n" {
		t.Fatalf("built stale app: %v: %s", err, output)
	}
	// Repeated Go invocations use the private delta as well as the seed.
	again := run(scratch, helper)
	if strings.Contains(again, "/compile ") {
		t.Fatalf("private delta was not reused: %s", again)
	}
}

func treeDigest(t *testing.T, root string) string {
	t.Helper()
	hash := sha256.New()
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fmt.Fprintf(hash, "%s:%x\n", path, sha256.Sum256(data))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}
