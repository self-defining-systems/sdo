package apihistory

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// MaxChangedPaths and MaxChangedPathBytes bound one write's recorded field paths.
	MaxChangedPaths     = 32
	MaxChangedPathBytes = 1024
	// MaxRequestBodyBytes bounds the request prefix the proxy parses for field paths.
	MaxRequestBodyBytes = 256 << 10
	// Opaque replaces a path segment that fails every identifier grammar.
	Opaque = "<opaque>"
	// Withheld replaces a command argument outside the safe-token grammar.
	Withheld = "<withheld>"
)

// ChangedPaths returns the field paths a write request changes: keys only, never values. A path segment that is
// not a Kubernetes identifier is recorded as "<opaque>". Nothing below data, stringData or binaryData of a Secret
// or ConfigMap (or below a TokenRequest body) is recorded. It reports false when the body cannot be parsed or no
// path is recoverable, so a reader knows the paths are unknown rather than empty.
func ChangedPaths(resource, contentType string, body []byte) ([]string, bool) {
	var document any
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, false
	}
	hidden := resource == "secrets" || resource == "configmaps" || resource == "serviceaccounts"
	collected := map[string]bool{}
	if operations, ok := document.([]any); ok && strings.Contains(contentType, "json-patch") {
		for _, item := range operations {
			operation, ok := item.(map[string]any)
			if !ok {
				return nil, false
			}
			verb, _ := operation["op"].(string)
			path, _ := operation["path"].(string)
			if !safeOperation(verb) {
				verb = Opaque
			}
			collected[verb+" "+pointerPath(path, hidden)] = true
		}
	} else if object, ok := document.(map[string]any); ok {
		walkPaths(object, "", hidden, collected)
	} else {
		return nil, false
	}
	paths := make([]string, 0, len(collected))
	for path := range collected {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var kept []string
	size := 0
	for _, path := range paths {
		if len(kept) == MaxChangedPaths || size+len(path) > MaxChangedPathBytes {
			break
		}
		kept = append(kept, path)
		size += len(path)
	}
	return kept, len(kept) > 0
}

func safeOperation(op string) bool {
	switch op {
	case "add", "remove", "replace", "move", "copy", "test":
		return true
	}
	return false
}

func hiddenKey(key string) bool {
	return key == "data" || key == "stringData" || key == "binaryData"
}

func walkPaths(object map[string]any, prefix string, hidden bool, out map[string]bool) {
	for key, value := range object {
		path := segment(key)
		if prefix != "" {
			path = prefix + "." + path
		}
		if hidden && prefix == "" && hiddenKey(key) {
			out[path] = true
			continue
		}
		switch typed := value.(type) {
		case map[string]any:
			if len(typed) == 0 {
				out[path] = true
				continue
			}
			walkPaths(typed, path, hidden, out)
		case []any:
			if len(typed) == 0 {
				out[path] = true
			}
			for index, item := range typed {
				element, ok := item.(map[string]any)
				if !ok {
					out[path+"[]"] = true
					continue
				}
				label := strconv.Itoa(index)
				if name, ok := element["name"].(string); ok && identifier(name) {
					label = name
					rest := make(map[string]any, len(element))
					for key, value := range element {
						if key != "name" {
							rest[key] = value
						}
					}
					element = rest
				}
				if len(element) == 0 {
					out[path+"["+label+"]"] = true
					continue
				}
				walkPaths(element, path+"["+label+"]", hidden, out)
			}
		default:
			out[path] = true
		}
	}
}

func pointerPath(pointer string, hidden bool) string {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	for i, part := range parts {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		if hidden && i == 0 && hiddenKey(part) {
			return "/" + part
		}
		parts[i] = segment(part)
	}
	return "/" + strings.Join(parts, "/")
}

func identifier(s string) bool {
	if s == "" || len(s) > 253 {
		return false
	}
	return len(validation.IsQualifiedName(s)) == 0 || len(validation.IsDNS1123Subdomain(s)) == 0 ||
		len(validation.IsCIdentifier(s)) == 0 || decimalIdentifier(s) == nil
}

func segment(s string) string {
	if identifier(s) {
		return s
	}
	return Opaque
}

var safeWords = map[string]bool{"reload": true, "stop": true, "quit": true, "reopen": true, "status": true, "start": true, "restart": true}

var shells = map[string]bool{"sh": true, "bash": true, "dash": true, "zsh": true, "ash": true}

const maxCommandArguments = 32
const maxSafeArgumentBytes = 64

// RedactCommand keeps the program's base name, the argument count and only arguments that match a safe-token
// grammar: no "=", ":", "@", quote or whitespace; not a short flag with an attached value (-pSECRET); not the
// value after a short flag unless it is a known verb (nginx -s reload); not longer than a token; and nothing
// after a shell's -c. Everything else becomes "<withheld>", so an unlisted credential shape is hidden, never kept.
func RedactCommand(argv []string) []string {
	if len(argv) == 0 {
		return nil
	}
	program := argv[0]
	if i := strings.LastIndexByte(program, '/'); i >= 0 {
		program = program[i+1:]
	}
	if !safeToken(program) {
		program = Withheld
	}
	out := []string{program}
	shell := shells[program]
	afterShellPayload := false
	previousShort := false
	for i, arg := range argv[1:] {
		if i >= maxCommandArguments {
			out = append(out, Withheld)
			continue
		}
		switch {
		case afterShellPayload:
			out = append(out, Withheld)
			previousShort = false
			continue
		case !safeToken(arg):
			out = append(out, Withheld)
			previousShort = false
			continue
		case strings.HasPrefix(arg, "--"):
			out = append(out, arg)
			previousShort = false
			continue
		case strings.HasPrefix(arg, "-") && len(arg) > 2:
			out = append(out, Withheld)
			previousShort = false
			continue
		case strings.HasPrefix(arg, "-"):
			out = append(out, arg)
			previousShort = true
			afterShellPayload = shell && arg == "-c"
			continue
		}
		if previousShort && !safeWords[arg] {
			out = append(out, Withheld)
		} else {
			out = append(out, arg)
		}
		previousShort = false
	}
	return out
}

func safeToken(s string) bool {
	if s == "" || len(s) > maxSafeArgumentBytes {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '.' || r == '/' || r == ',' || r == '+' || r == '%' || r == '-':
		default:
			return false
		}
	}
	return true
}

// SafePorts keeps only decimal port numbers.
func SafePorts(values []string) []string {
	var out []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if n, err := strconv.Atoi(part); err == nil && n > 0 && n < 65536 && len(out) < 16 {
				out = append(out, fmt.Sprint(n))
			}
		}
	}
	return out
}
