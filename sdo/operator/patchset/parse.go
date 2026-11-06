package patchset

import (
	"bytes"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	hex40       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	indexLine   = regexp.MustCompile(`^index [0-9a-f]{7,64}\.\.[0-9a-f]{7,64}( [0-7]{6})?$`)
	hunkHeader  = regexp.MustCompile(`^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@`)
	similarity  = regexp.MustCompile(`^(dis)?similarity index \d{1,3}%$`)
	headerKey   = regexp.MustCompile(`^([A-Za-z][A-Za-z-]*):(?: (.*))?$`)
	pathLikeTok = regexp.MustCompile(`^[\w.@+-]+\.[A-Za-z0-9]+$`)
)

const diffStart = "diff --git "

// Parse reads one patch file. name is used only in errors. Every failure is a
// *PatchError (malformed or forbidden).
func Parse(name string, data []byte) (Patch, error) {
	if len(data) > MaxPatchBytes {
		return Patch{}, perr(KindForbidden, name, nil, "%d bytes exceeds the %d byte limit per patch", len(data), MaxPatchBytes)
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return Patch{}, perr(KindForbidden, name, nil, "binary content (NUL byte)")
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return Patch{}, perr(KindMalformed, name, nil, "file must end with a newline")
	}
	lines := strings.SplitAfter(string(data), "\n")
	lines = lines[:len(lines)-1] // SplitAfter leaves one empty tail after the final newline
	start := -1
	for i, l := range lines {
		if strings.HasPrefix(l, diffStart) {
			start = i
			break
		}
	}
	if start < 0 {
		return Patch{}, perr(KindMalformed, name, nil, "no diff --git section")
	}
	hdr, err := parseHeader(name, lines[:start])
	if err != nil {
		return Patch{}, err
	}
	files, err := parseDiff(name, lines[start:])
	if err != nil {
		return Patch{}, err
	}
	p := Patch{Name: name, Header: hdr, Files: files, raw: append([]byte(nil), data...)}
	p.Diff = []byte(strings.Join(lines[start:], ""))
	if e := checkReason(name, hdr.Reason, p.Paths()); e != nil {
		return Patch{}, e
	}
	return p, nil
}

func parseHeader(name string, lines []string) (Header, error) {
	var h Header
	seen := map[string]bool{}
	first := true
	for i := 0; i < len(lines); i++ {
		line := strings.TrimSuffix(lines[i], "\n")
		if line == "" {
			continue
		}
		if !utf8.ValidString(line) || strings.ContainsFunc(line, func(r rune) bool { return r < ' ' && r != '\t' }) {
			return h, perr(KindMalformed, name, nil, "preamble line %d has control or invalid characters", i+1)
		}
		m := headerKey.FindStringSubmatch(line)
		if m == nil {
			return h, perr(KindMalformed, name, nil, "preamble line %d is not `Key: value`", i+1)
		}
		key, val := m[1], m[2]
		if first {
			if key != "SDO-Patch" || val != "1" {
				return h, perr(KindMalformed, name, nil, "first line must be `SDO-Patch: 1`")
			}
			first = false
		}
		if seen[key] {
			return h, perr(KindMalformed, name, nil, "duplicate header key %s", key)
		}
		seen[key] = true
		if key == "Reason" && val == "|" {
			var block []string
			for i+1 < len(lines) {
				next := strings.TrimSuffix(lines[i+1], "\n")
				if next == "" {
					block = append(block, "")
				} else if strings.HasPrefix(next, "  ") {
					block = append(block, next[2:])
				} else {
					break
				}
				i++
			}
			val = strings.TrimSpace(strings.Join(block, "\n"))
		} else if strings.TrimSpace(val) != val {
			return h, perr(KindMalformed, name, nil, "header %s has surrounding whitespace", key)
		}
		if val == "" && key != "Reason" {
			return h, perr(KindMalformed, name, nil, "header %s is empty", key)
		}
		switch key {
		case "SDO-Patch":
		case "Owner":
			h.Owner = Owner(val)
		case "Reason":
			h.Reason = val
		case "Incident":
			h.Incident = val
		case "Upstream-Base":
			h.UpstreamBase = val
		case "Upstream-Fix":
			h.UpstreamFix = val
		default:
			return h, perr(KindMalformed, name, nil, "unknown header key %s", key)
		}
	}
	if first {
		return h, perr(KindMalformed, name, nil, "missing `SDO-Patch: 1` header")
	}
	switch h.Owner {
	case OwnerDeployer, OwnerResponder:
	default:
		return h, perr(KindMalformed, name, nil, "Owner must be deployer or responder, got %q", h.Owner)
	}
	if (h.Owner == OwnerResponder) != (h.Incident != "") {
		return h, perr(KindMalformed, name, nil, "Incident is required for, and only for, owner responder")
	}
	if len(h.Incident) > 256 {
		return h, perr(KindMalformed, name, nil, "Incident longer than 256 bytes")
	}
	if h.UpstreamBase != "" && !hex40.MatchString(h.UpstreamBase) {
		return h, perr(KindMalformed, name, nil, "Upstream-Base must be a 40-hex commit ID")
	}
	if !seen["Reason"] {
		return h, perr(KindMalformed, name, nil, "missing Reason header")
	}
	return h, nil
}

func checkReason(name, reason string, paths []string) *PatchError {
	n := utf8.RuneCountInString(reason)
	if n < MinReasonRunes || n > MaxReasonRunes {
		return perr(KindMalformed, name, nil, "Reason must be %d..%d characters, got %d", MinReasonRunes, MaxReasonRunes, n)
	}
	if strings.ContainsFunc(reason, func(r rune) bool { return unicode.IsControl(r) && r != '\n' && r != '\t' }) {
		return perr(KindMalformed, name, nil, "Reason has control characters")
	}
	isPath := map[string]bool{}
	for _, p := range paths {
		isPath[p] = true
	}
	words := strings.FieldsFunc(reason, func(r rune) bool { return unicode.IsSpace(r) || r == ',' || r == ';' })
	for _, w := range words {
		if !isPath[w] && !strings.Contains(w, "/") && !pathLikeTok.MatchString(w) {
			return nil
		}
	}
	return perr(KindMalformed, name, nil, "Reason is only a list of paths; say why the file must differ")
}

type section struct {
	a, b                                    string
	newMode, delMode, oldMode, mod, idxMode string
	renameFrom, renameTo                    string
	copyFrom, copyTo                        string
	minus, plus                             string
	hunks                                   int
}

func parseDiff(name string, lines []string) ([]FileChange, error) {
	var files []FileChange
	i := 0
	for i < len(lines) {
		sec, next, err := parseSection(name, lines, i)
		if err != nil {
			return nil, err
		}
		fc, err := sec.change(name)
		if err != nil {
			return nil, err
		}
		files = append(files, fc)
		i = next
	}
	if len(files) == 0 {
		return nil, perr(KindMalformed, name, nil, "patch touches no file")
	}
	return files, nil
}

func parseSection(name string, lines []string, i int) (section, int, error) {
	var s section
	head := strings.TrimSuffix(lines[i], "\n")
	fields := strings.Split(strings.TrimPrefix(head, diffStart), " ")
	if len(fields) != 2 || !strings.HasPrefix(fields[0], "a/") || !strings.HasPrefix(fields[1], "b/") {
		return s, 0, perr(KindForbidden, name, nil, "unsupported diff header %q (paths with spaces or quotes are not allowed)", head)
	}
	s.a, s.b = fields[0][2:], fields[1][2:]
	i++
	for ; i < len(lines); i++ {
		line := strings.TrimSuffix(lines[i], "\n")
		switch {
		case strings.HasPrefix(line, diffStart), strings.HasPrefix(line, "@@ "):
			goto body
		case strings.HasPrefix(line, "--- "):
			if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "+++ ") {
				return s, 0, perr(KindMalformed, name, nil, "--- line without +++ line")
			}
			s.minus = line[4:]
			s.plus = strings.TrimSuffix(lines[i+1], "\n")[4:]
			i += 2
			goto body
		case strings.HasPrefix(line, "new file mode "):
			s.newMode = line[len("new file mode "):]
		case strings.HasPrefix(line, "deleted file mode "):
			s.delMode = line[len("deleted file mode "):]
		case strings.HasPrefix(line, "old mode "):
			s.oldMode = line[len("old mode "):]
		case strings.HasPrefix(line, "new mode "):
			s.mod = line[len("new mode "):]
		case strings.HasPrefix(line, "rename from "):
			s.renameFrom = line[len("rename from "):]
		case strings.HasPrefix(line, "rename to "):
			s.renameTo = line[len("rename to "):]
		case strings.HasPrefix(line, "copy from "):
			s.copyFrom = line[len("copy from "):]
		case strings.HasPrefix(line, "copy to "):
			s.copyTo = line[len("copy to "):]
		case strings.HasPrefix(line, "GIT binary patch"), strings.HasPrefix(line, "Binary files "):
			return s, 0, perr(KindForbidden, name, []string{s.b}, "binary hunk")
		case indexLine.MatchString(line):
			if f := strings.Fields(line); len(f) == 3 {
				s.idxMode = f[2]
			}
		case similarity.MatchString(line):
		default:
			return s, 0, perr(KindMalformed, name, []string{s.b}, "unrecognized diff header line %q", line)
		}
	}
body:
	for i < len(lines) && strings.HasPrefix(lines[i], "@@ ") {
		next, err := parseHunk(name, lines, i)
		if err != nil {
			return s, 0, err
		}
		s.hunks++
		i = next
	}
	if i < len(lines) && !strings.HasPrefix(lines[i], diffStart) {
		return s, 0, perr(KindMalformed, name, []string{s.b}, "unexpected line after hunk: %q", strings.TrimSuffix(lines[i], "\n"))
	}
	return s, i, nil
}

// parseHunk consumes exactly the lines the hunk header promises, so content
// that looks like a diff header inside a hunk is never reinterpreted.
func parseHunk(name string, lines []string, i int) (int, error) {
	m := hunkHeader.FindStringSubmatch(strings.TrimSuffix(lines[i], "\n"))
	if m == nil {
		return 0, perr(KindMalformed, name, nil, "bad hunk header %q", strings.TrimSuffix(lines[i], "\n"))
	}
	count := func(s string) int {
		if s == "" {
			return 1
		}
		n, _ := strconv.Atoi(s)
		return n
	}
	old, nw := count(m[2]), count(m[4])
	if old > MaxPatchBytes || nw > MaxPatchBytes {
		return 0, perr(KindMalformed, name, nil, "hunk length out of range")
	}
	i++
	for old > 0 || nw > 0 {
		if i >= len(lines) {
			return 0, perr(KindMalformed, name, nil, "hunk is shorter than its header says")
		}
		switch lines[i][0] {
		case ' ', '\n':
			old--
			nw--
		case '-':
			old--
		case '+':
			nw--
		case '\\':
		default:
			return 0, perr(KindMalformed, name, nil, "bad hunk line %q", strings.TrimSuffix(lines[i], "\n"))
		}
		if old < 0 || nw < 0 {
			return 0, perr(KindMalformed, name, nil, "hunk is longer than its header says")
		}
		i++
	}
	for i < len(lines) && strings.HasPrefix(lines[i], "\\") {
		i++
	}
	return i, nil
}

func (s section) change(name string) (FileChange, error) {
	var fc FileChange
	bad := func(kind Kind, f string, args ...any) (FileChange, error) {
		return FileChange{}, perr(kind, name, []string{s.b}, f, args...)
	}
	switch {
	case s.renameFrom != "" || s.renameTo != "":
		if s.renameFrom != s.a || s.renameTo != s.b || s.a == s.b {
			return bad(KindMalformed, "rename lines disagree with the diff header")
		}
		fc = FileChange{Op: OpRename, OldPath: s.a, Path: s.b}
	case s.copyFrom != "" || s.copyTo != "":
		if s.copyFrom != s.a || s.copyTo != s.b || s.a == s.b {
			return bad(KindMalformed, "copy lines disagree with the diff header")
		}
		fc = FileChange{Op: OpCopy, OldPath: s.a, Path: s.b}
	case s.newMode != "":
		if s.a != s.b || s.delMode != "" || (s.minus != "" && (s.minus != "/dev/null" || s.plus != "b/"+s.b)) {
			return bad(KindMalformed, "inconsistent new file section")
		}
		fc = FileChange{Op: OpAdd, Path: s.b, Mode: s.newMode}
	case s.delMode != "":
		if s.a != s.b || (s.minus != "" && (s.minus != "a/"+s.a || s.plus != "/dev/null")) {
			return bad(KindMalformed, "inconsistent deleted file section")
		}
		fc = FileChange{Op: OpDelete, Path: s.a, Mode: s.delMode}
	default:
		if s.a != s.b || (s.minus != "" && (s.minus != "a/"+s.a || s.plus != "b/"+s.b)) {
			return bad(KindMalformed, "inconsistent file section")
		}
		if s.hunks == 0 && s.mod == "" && s.oldMode == "" {
			return bad(KindMalformed, "section changes nothing")
		}
		fc = FileChange{Op: OpModify, Path: s.a}
	}
	if fc.Op != OpAdd && fc.Op != OpDelete && s.mod != "" {
		fc.Mode = s.mod
	}
	for _, m := range []string{s.newMode, s.delMode, s.oldMode, s.mod, s.idxMode} {
		if m != "" && m != "100644" && m != "100755" {
			return bad(KindForbidden, "mode %s is not allowed (regular files only: 100644, 100755)", m)
		}
	}
	for _, p := range []string{fc.OldPath, fc.Path} {
		if p == "" {
			continue
		}
		if why := badPath(p); why != "" {
			return FileChange{}, perr(KindForbidden, name, []string{p}, "path %q %s", p, why)
		}
	}
	return fc, nil
}

// badPath explains why a repository path may not be touched, or "".
func badPath(p string) string {
	if p == "" || strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return "is not a clean relative file path"
	}
	if !utf8.ValidString(p) || strings.ContainsFunc(p, func(r rune) bool { return unicode.IsControl(r) || r == '\\' || r == '"' || r == ' ' }) {
		return "has characters that are not allowed"
	}
	for i, seg := range strings.Split(p, "/") {
		switch low := strings.ToLower(seg); {
		case seg == "" || seg == "." || seg == "..":
			return "is not a clean relative file path"
		case i == 0 && low == ".sdo":
			return "is inside .sdo/ (patches change source files only)"
		case low == ".git":
			return "is inside .git"
		}
	}
	return ""
}
