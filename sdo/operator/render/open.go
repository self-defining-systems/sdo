package render

import (
	"bytes"
	"io"
	"io/fs"
)

// openVia implements fs.FS.Open on top of ReadFile, ReadDir and Stat so that
// wrappers observe every access.
func openVia(f FS, name string) (fs.File, error) {
	if !fs.ValidPath(name) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	info, err := f.Stat(name)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		entries, err := f.ReadDir(name)
		if err != nil {
			return nil, err
		}
		return &dirFile{info: info, entries: entries}, nil
	}
	data, err := f.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return &memFile{info: info, Reader: bytes.NewReader(data)}, nil
}

type memFile struct {
	info fs.FileInfo
	*bytes.Reader
}

func (m *memFile) Stat() (fs.FileInfo, error) { return m.info, nil }
func (m *memFile) Close() error               { return nil }

type dirFile struct {
	info    fs.FileInfo
	entries []fs.DirEntry
}

func (d *dirFile) Stat() (fs.FileInfo, error) { return d.info, nil }
func (d *dirFile) Close() error               { return nil }
func (d *dirFile) Read([]byte) (int, error) {
	return 0, &fs.PathError{Op: "read", Path: d.info.Name(), Err: fs.ErrInvalid}
}

func (d *dirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		out := d.entries
		d.entries = nil
		return out, nil
	}
	if len(d.entries) == 0 {
		return nil, io.EOF
	}
	n = min(n, len(d.entries))
	out := d.entries[:n]
	d.entries = d.entries[n:]
	return out, nil
}
