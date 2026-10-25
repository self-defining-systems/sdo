// sdo-go-cache implements Go's GOCACHEPROG protocol as a read-through overlay.
// The pinned Go 1.26 disk-cache v1 format is shared with image-build warming.
// Seed files are never opened for writing; misses are stored only in GOCACHE.
package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

type request struct {
	ID                 int64
	Command            string
	ActionID, OutputID []byte
	BodySize           int64
}
type response struct {
	ID            int64
	Err           string   `json:",omitempty"`
	KnownCommands []string `json:",omitempty"`
	Miss          bool     `json:",omitempty"`
	OutputID      []byte   `json:",omitempty"`
	Size          int64    `json:",omitempty"`
	DiskPath      string   `json:",omitempty"`
}
type cache struct{ seed, writable string }

func cachePath(root string, id []byte, suffix string) string {
	key := hex.EncodeToString(id)
	return filepath.Join(root, key[:2], key+"-"+suffix)
}

func (c cache) get(action []byte) response {
	if len(action) != 32 {
		return response{Miss: true}
	}
	for _, root := range []string{c.writable, c.seed} {
		if root == "" {
			continue
		}
		data, err := os.ReadFile(cachePath(root, action, "a"))
		if err != nil {
			continue
		}
		var version, key, output string
		var size, stamp int64
		if n, err := fmt.Sscanf(string(data), "%s %s %s %d %d", &version, &key, &output, &size, &stamp); err != nil || n != 5 || version != "v1" || key != hex.EncodeToString(action) || size < 0 {
			continue
		}
		id, err := hex.DecodeString(output)
		if err != nil || len(id) != 32 {
			continue
		}
		path := cachePath(root, id, "d")
		stat, err := os.Stat(path)
		if err != nil || !stat.Mode().IsRegular() || stat.Size() != size {
			continue
		}
		return response{OutputID: id, Size: size, DiskPath: path}
	}
	return response{Miss: true}
}

func atomicWrite(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".cache-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}

func (c cache) put(r request, body []byte) (response, error) {
	if len(r.ActionID) != 32 || len(r.OutputID) != 32 || r.BodySize != int64(len(body)) {
		return response{}, errors.New("invalid Go cache put identity or size")
	}
	path := cachePath(c.writable, r.OutputID, "d")
	if err := atomicWrite(path, body); err != nil {
		return response{}, err
	}
	entry := fmt.Sprintf("v1 %x %x %20d %20d\n", r.ActionID, r.OutputID, r.BodySize, time.Now().UnixNano())
	if err := atomicWrite(cachePath(c.writable, r.ActionID, "a"), []byte(entry)); err != nil {
		return response{}, err
	}
	return response{DiskPath: path}, nil
}

func serve(input io.Reader, output io.Writer, c cache) error {
	dec, enc := json.NewDecoder(input), json.NewEncoder(output)
	if err := enc.Encode(response{KnownCommands: []string{"get", "put", "close"}}); err != nil {
		return err
	}
	for {
		var r request
		if err := dec.Decode(&r); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		var res response
		switch r.Command {
		case "get":
			res = c.get(r.ActionID)
		case "put":
			var body []byte
			if r.BodySize > 0 {
				if err := dec.Decode(&body); err != nil {
					return err
				}
			}
			var err error
			res, err = c.put(r, body)
			if err != nil {
				res.Err = err.Error()
			}
		case "close":
		default:
			res.Err = "unsupported Go cache command"
		}
		res.ID = r.ID
		if err := enc.Encode(res); err != nil {
			return err
		}
		if r.Command == "close" {
			return nil
		}
	}
}

func main() {
	writable := os.Getenv("GOCACHE")
	if !filepath.IsAbs(writable) || writable == os.Getenv("SDO_GO_CACHE_SEED") {
		fmt.Fprintln(os.Stderr, "GOCACHE must be an absolute private writable directory distinct from SDO_GO_CACHE_SEED")
		os.Exit(1)
	}
	if err := serve(os.Stdin, os.Stdout, cache{seed: os.Getenv("SDO_GO_CACHE_SEED"), writable: writable}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
