package evidencesink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
)

// Redacted replaces a credential value in run evidence.
const Redacted = "[redacted]"

const (
	// minSecretBytes keeps short words out of the replacement set.
	minSecretBytes = 8
	// maxCredentialBytes bounds one credential file.
	maxCredentialBytes = 1 << 20
	redactChunk        = 256 << 10
)

// MountedCredentialRoots are the credential Secret mounts of SDO's own Pods.
func MountedCredentialRoots() []string { return []string{"/sdo/credentials", "/sdo/claude-accounts"} }

// agentLogins are the login files an agent's CLI home holds. The CLIs refresh them in place, so they can hold values
// the mounted Secret does not.
var agentLogins = []string{"auth.json", ".credentials.json", "api_key.credential"}

// Credentials is the set of exact credential values known to SDO. It only grows: a value replaced by a refresh is
// still in earlier evidence. It is the one redactor of everything that leaves the runtime root.
type Credentials struct {
	values   map[string]bool
	patterns [][]byte
	first    [256][]int // pattern indices by first byte, longest first
	longest  int
}

// Len is the number of known value forms; it changes exactly when the set grows.
func (c *Credentials) Len() int { return len(c.values) }

// Load adds the values of every file in the mounted credential roots and of the login files in the agent homes below
// runtimeRoot. A missing root is an unconfigured one; a root or file that cannot be read is an error, so evidence is
// never written with an incomplete set.
func (c *Credentials) Load(mounted []string, runtimeRoot string) error {
	var files []string
	for _, root := range mounted {
		entries, err := os.ReadDir(root)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read credential directory: %w", err)
		}
		for _, entry := range entries {
			// Secret volumes keep their data in ..data and link each key to it.
			if !strings.HasPrefix(entry.Name(), "..") {
				files = append(files, filepath.Join(root, entry.Name()))
			}
		}
	}
	for _, file := range files {
		if err := c.loadFile(file, os.O_RDONLY); err != nil {
			return err
		}
	}
	for _, home := range []string{"codex", "claude", "sessions/*/codex", "sessions/*/claude", "tasks/*/codex", "tasks/*/claude"} {
		for _, login := range agentLogins {
			matches, err := filepath.Glob(filepath.Join(runtimeRoot, filepath.FromSlash(home), login))
			if err != nil {
				return err
			}
			for _, file := range matches {
				// Agents can write here: never follow a link.
				if err := c.loadFile(file, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK); err != nil {
					return err
				}
			}
		}
	}
	c.index()
	return nil
}

func (c *Credentials) loadFile(name string, flags int) error {
	file, err := os.OpenFile(name, flags, 0)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ELOOP) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read credential file: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxCredentialBytes))
	if err != nil {
		return fmt.Errorf("read credential file: %w", err)
	}
	c.addDocument(data)
	return nil
}

// addDocument adds the secret fields of a JSON document, or the whole content of any other file (a bare key or token).
func (c *Credentials) addDocument(data []byte) {
	var document any
	if err := json.Unmarshal(data, &document); err != nil {
		c.add(string(data))
		return
	}
	if text, ok := document.(string); ok {
		c.add(text)
		return
	}
	c.addFields(document, false)
}

var secretField = regexp.MustCompile(`(token|secret|password|passphrase|privatekey|apikey|authorization)$`)

func (c *Credentials) addFields(value any, secret bool) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			name := strings.ToLower(strings.NewReplacer("_", "", "-", "", ".", "").Replace(key))
			c.addFields(child, secretField.MatchString(name))
		}
	case []any:
		for _, child := range value {
			c.addFields(child, secret)
		}
	case string:
		if secret {
			c.add(value)
		}
	}
}

// add records a value one line at a time, so a multi-line key is found however its line breaks are escaped, and in the
// forms a line takes inside JSON text, nested up to three levels (a transcript line quoting a tool result that is JSON).
func (c *Credentials) add(value string) {
	if c.values == nil {
		c.values = map[string]bool{}
	}
	for _, line := range strings.FieldsFunc(value, func(r rune) bool { return r == '\n' || r == '\r' }) {
		line = strings.TrimSpace(line)
		if len(line) < minSecretBytes || strings.HasPrefix(line, "-----") {
			continue
		}
		for _, escapeHTML := range []bool{false, true} {
			form := line
			for range 4 {
				c.values[form] = true
				form = jsonText(form, escapeHTML)
			}
		}
	}
}

func jsonText(value string, escapeHTML bool) string {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(escapeHTML)
	_ = encoder.Encode(value) // a string always encodes
	return strings.TrimSuffix(strings.TrimPrefix(strings.TrimSuffix(out.String(), "\n"), `"`), `"`)
}

func (c *Credentials) index() {
	if len(c.patterns) == len(c.values) {
		return
	}
	c.patterns, c.first, c.longest = nil, [256][]int{}, 0
	for value := range c.values {
		c.patterns = append(c.patterns, []byte(value))
		c.longest = max(c.longest, len(value))
	}
	sort.Slice(c.patterns, func(i, j int) bool {
		if len(c.patterns[i]) != len(c.patterns[j]) {
			return len(c.patterns[i]) > len(c.patterns[j])
		}
		return bytes.Compare(c.patterns[i], c.patterns[j]) < 0
	})
	for i, pattern := range c.patterns {
		c.first[pattern[0]] = append(c.first[pattern[0]], i)
	}
}

// Reader streams source with every known value replaced. A source cut inside a value (a transcript still being
// appended, or one over the size cap) has the partial value at its end replaced too.
func (c *Credentials) Reader(source io.Reader) io.Reader {
	if len(c.patterns) == 0 {
		return source
	}
	return &redactReader{credentials: c, source: source}
}

type redactReader struct {
	credentials *Credentials
	source      io.Reader
	in, out     []byte
	err         error
}

func (r *redactReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		r.fill()
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

// fill reads one chunk and redacts every position whose longest possible match is wholly in memory.
func (r *redactReader) fill() {
	c := r.credentials
	have := len(r.in)
	r.in = append(r.in, make([]byte, redactChunk)...)
	n, err := io.ReadFull(r.source, r.in[have:])
	r.in = r.in[:have+n]
	ended := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
	if err != nil && !ended {
		r.err = err
		return
	}
	limit := len(r.in) - c.longest + 1
	if ended {
		r.err, limit = io.EOF, len(r.in)
	}
	out := make([]byte, 0, len(r.in))
	at := 0
	for at < limit {
		length := c.match(r.in[at:], ended)
		if length == 0 {
			out = append(out, r.in[at])
			at++
			continue
		}
		out = append(out, Redacted...)
		at += length
	}
	r.in = append([]byte(nil), r.in[at:]...)
	r.out = out
}

func (c *Credentials) match(text []byte, ended bool) int {
	for _, index := range c.first[text[0]] {
		pattern := c.patterns[index]
		if bytes.HasPrefix(text, pattern) {
			return len(pattern)
		}
		if ended && len(text) >= minSecretBytes && bytes.HasPrefix(pattern, text) {
			return len(text)
		}
	}
	return 0
}
