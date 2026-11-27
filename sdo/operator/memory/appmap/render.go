package appmap

import (
	"strings"

	"gopkg.in/yaml.v3"
)

// Render returns m as the YAML of an application-map region: top-level keys
// in block style and every list entry on one flow-style line, so a component
// or edge costs one line to read and to diff. header is the comment block the
// region opened with (see Header); it is written first, unchanged.
func Render(m Map, header string) (string, error) {
	var doc yaml.Node
	if err := doc.Encode(m); err != nil {
		return "", err
	}
	for i := 1; i < len(doc.Content); i += 2 {
		if list := doc.Content[i]; list.Kind == yaml.SequenceNode {
			for _, item := range list.Content {
				setFlow(item)
			}
		}
	}
	var body strings.Builder
	enc := yaml.NewEncoder(&body)
	enc.SetIndent(2)
	if err := enc.Encode(&doc); err != nil {
		return "", err
	}
	if err := enc.Close(); err != nil {
		return "", err
	}
	return header + body.String(), nil
}

func setFlow(n *yaml.Node) {
	if n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode {
		n.Style |= yaml.FlowStyle
	}
	for _, child := range n.Content {
		setFlow(child)
	}
}

// Header returns the leading comment lines of region YAML, each with its
// newline, so a rewrite can keep them.
func Header(regionYAML string) string {
	var b strings.Builder
	for _, line := range strings.SplitAfter(regionYAML, "\n") {
		if !strings.HasPrefix(line, "#") {
			break
		}
		b.WriteString(line)
	}
	return b.String()
}
