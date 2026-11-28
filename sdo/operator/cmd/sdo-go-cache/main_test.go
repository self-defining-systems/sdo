package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// id is a synthetic 32-byte Go cache identity.
func id(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

// seedEntry writes one Go disk-cache v1 entry (action index file and output) into root.
func seedEntry(t *testing.T, root string, action, output []byte, body string) {
	t.Helper()
	for path, data := range map[string]string{
		cachePath(root, output, "d"): body,
		cachePath(root, action, "a"): fmt.Sprintf("v1 %x %x %20d %20d\n", action, output, len(body), 1),
	} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// A worker's Go reads the private cache first, then every seed in the order given, and finds an entry in whichever
// holds it; an entry in no cache is a miss.
func TestGetReadsTheWritableCacheThenEachSeedInOrder(t *testing.T) {
	writable, controller, image := t.TempDir(), t.TempDir(), t.TempDir()
	seedEntry(t, image, id(1), id(11), "from image")
	seedEntry(t, controller, id(2), id(12), "from controller")
	seedEntry(t, image, id(2), id(22), "older image copy")
	c, err := newCache(writable, controller+string(filepath.ListSeparator)+image)
	if err != nil {
		t.Fatal(err)
	}
	for action, want := range map[byte]string{1: filepath.Join(image), 2: filepath.Join(controller)} {
		got := c.get(id(action))
		if got.Miss || !strings.HasPrefix(got.DiskPath, want+string(filepath.Separator)) {
			t.Fatalf("action %d: %+v, want an entry under %s", action, got, want)
		}
	}
	if got := c.get(id(3)); !got.Miss {
		t.Fatalf("an entry in no cache must miss, got %+v", got)
	}
}

// Seeds are read only: a put lands in the private cache and is read back from there.
func TestPutWritesOnlyTheWritableCache(t *testing.T) {
	writable, seed := t.TempDir(), t.TempDir()
	c, err := newCache(writable, seed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.put(request{ActionID: id(4), OutputID: id(14), BodySize: 3}, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(seed); len(entries) != 0 {
		t.Fatalf("put wrote into a seed: %v", entries)
	}
	if got := c.get(id(4)); got.Miss || !strings.HasPrefix(got.DiskPath, writable) {
		t.Fatalf("put entry not read back from GOCACHE: %+v", got)
	}
}

// A truncated or foreign entry (an index whose output has another size, as a seed still being written can hold) is a
// miss, never a wrong output.
func TestGetMissesAnEntryWhoseOutputDoesNotMatchItsIndex(t *testing.T) {
	writable, seed := t.TempDir(), t.TempDir()
	seedEntry(t, seed, id(5), id(15), "complete")
	if err := os.WriteFile(cachePath(seed, id(15), "d"), []byte("trunc"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := newCache(writable, seed)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.get(id(5)); !got.Miss {
		t.Fatalf("a size mismatch must miss, got %+v", got)
	}
}

func TestNewCacheRefusesARelativeOrSharedWritableCache(t *testing.T) {
	dir := t.TempDir()
	for name, c := range map[string]struct{ writable, seeds string }{
		"relative GOCACHE":     {"go-cache", ""},
		"GOCACHE is a seed":    {dir, "/go/build-cache" + string(filepath.ListSeparator) + dir},
		"GOCACHE is the seed/": {dir, dir + "/"},
	} {
		if _, err := newCache(c.writable, c.seeds); err == nil {
			t.Errorf("%s: accepted %+v", name, c)
		}
	}
	if c, err := newCache(dir, ""); err != nil || len(c.seeds) != 0 {
		t.Fatalf("no seed is a plain private cache: %+v %v", c, err)
	}
}

// The protocol round trip Go speaks: a handshake naming the commands, then one response per request with its ID.
func TestServeAnswersEachRequestWithItsID(t *testing.T) {
	writable := t.TempDir()
	var input bytes.Buffer
	enc := json.NewEncoder(&input)
	for _, r := range []any{
		request{ID: 1, Command: "put", ActionID: id(6), OutputID: id(16), BodySize: 2}, []byte("hi"),
		request{ID: 2, Command: "get", ActionID: id(6)},
		request{ID: 3, Command: "close"},
	} {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	var output bytes.Buffer
	if err := serve(&input, &output, cache{writable: writable}); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(&output)
	var handshake response
	if err := dec.Decode(&handshake); err != nil || strings.Join(handshake.KnownCommands, ",") != "get,put,close" {
		t.Fatalf("handshake %+v %v", handshake, err)
	}
	for want := int64(1); want <= 3; want++ {
		var got response
		if err := dec.Decode(&got); err != nil || got.ID != want || got.Err != "" {
			t.Fatalf("response %d: %+v %v", want, got, err)
		}
		if want == 2 && (got.Miss || got.Size != 2) {
			t.Fatalf("get after put: %+v", got)
		}
	}
}
