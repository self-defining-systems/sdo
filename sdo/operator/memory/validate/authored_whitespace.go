package validate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"sdo.dev/operator/memory/layout"
)

// NormalizeAuthoredWhitespace strips trailing line whitespace and trailing
// blank lines from changed SDO-authored text under .sdo/, so a stray space
// never costs a rejection. App/source files, captured corpus bytes, rendered
// deployments and outcomes are never touched. Space-before-tab remains a
// diagnostic because no single rewrite of it is obviously intended.
func NormalizeAuthoredWhitespace(root string, paths []string) ([]string, error) {
	var changed []string
	for _, path := range paths {
		if !authoredMemoryText(path) {
			continue
		}
		file := filepath.Join(root, filepath.FromSlash(path))
		info, err := os.Lstat(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		if bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
			continue
		}
		normalized := normalizeTrailingWhitespace(data)
		if bytes.Equal(normalized, data) {
			continue
		}
		if err := os.WriteFile(file, normalized, info.Mode().Perm()); err != nil {
			return nil, err
		}
		changed = append(changed, path)
	}
	return changed, nil
}

func authoredMemoryText(path string) bool {
	if !layout.IsMemoryPath(path) {
		return false
	}
	for _, dir := range []string{layout.CorpusDir, layout.RenderedDir, layout.OutcomesDir} {
		if strings.HasPrefix(path, dir+"/") {
			return false
		}
	}
	return true
}

func normalizeTrailingWhitespace(data []byte) []byte {
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t\r")
	}
	text := strings.Join(lines, "\n")
	if strings.HasSuffix(text, "\n") {
		text = strings.TrimRight(text, "\n")
		if text != "" {
			text += "\n"
		}
	}
	return []byte(text)
}
