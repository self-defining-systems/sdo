package patchset

import (
	"crypto/sha1" //nolint:gosec // Git object IDs are SHA-1; this is not a security hash.
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

var patchName = regexp.MustCompile(`^(\d{4})-[a-z0-9][a-z0-9-]{0,63}\.patch$`)

// File is one patch file as read from the repository.
type File struct {
	Name string
	Data []byte
}

// Series is an ordered, validated set of patches. The zero Series is empty.
type Series struct {
	patches []Patch
}

// ParseSeries validates a whole directory of patch files: name format
// (NNNN-slug.patch), unique numbers, the per-series caps, and every patch.
// The result is ordered by file name. All failures are *PatchError.
func ParseSeries(files []File) (Series, error) {
	if len(files) > MaxPatches {
		return Series{}, perr(KindForbidden, "", nil, "%d patches exceed the limit of %d", len(files), MaxPatches)
	}
	sorted := append([]File(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	total := 0
	numbers := map[int]string{}
	var s Series
	for _, f := range sorted {
		m := patchName.FindStringSubmatch(f.Name)
		if m == nil {
			return Series{}, perr(KindMalformed, f.Name, nil, "file name must be NNNN-slug.patch (lowercase letters, digits and dashes)")
		}
		n, _ := strconv.Atoi(m[1])
		if prev, dup := numbers[n]; dup {
			return Series{}, perr(KindMalformed, f.Name, nil, "number %04d is already used by %s", n, prev)
		}
		numbers[n] = f.Name
		total += len(f.Data)
		if total > MaxTotalBytes {
			return Series{}, perr(KindForbidden, f.Name, nil, "series exceeds %d bytes", MaxTotalBytes)
		}
		p, err := Parse(f.Name, f.Data)
		if err != nil {
			return Series{}, err
		}
		s.patches = append(s.patches, p)
	}
	return s, nil
}

// Patches returns the series in application order.
func (s Series) Patches() []Patch { return append([]Patch(nil), s.patches...) }

// Len is the number of patches.
func (s Series) Len() int { return len(s.patches) }

// Digest binds the exact series: sha256 over `name NUL blob-id LF` per patch in
// name order, where blob-id is the Git blob ID of the whole patch file. It
// equals what a tree listing of the patches directory would give, with no git
// process needed.
func (s Series) Digest() string {
	h := sha256.New()
	for _, p := range s.patches {
		fmt.Fprintf(h, "%s\x00%s\n", p.Name, BlobID(p.raw))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// BlobID is the Git blob object ID of data.
func BlobID(data []byte) string {
	h := sha1.New() //nolint:gosec // see import
	fmt.Fprintf(h, "blob %d\x00", len(data))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}
