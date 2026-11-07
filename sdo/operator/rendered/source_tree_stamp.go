package rendered

import "strings"

// SourceTreeKey names SDO's effective-tree stamp. It is an annotation on every
// stamped object except a Deployment, whose generation the API server bumps for
// any annotation change: an annotation stamp would roll the generation of every
// Deployment on every advance, rendered change or not. A Deployment carries the
// stamp in labels instead (labels never move generation).
const SourceTreeKey = "sdo.dev/source-tree"

// SourceTreeRestKey holds the part of a tree ID that does not fit a label value
// (63 characters; a SHA-256 tree ID has 64).
const SourceTreeRestKey = "sdo.dev/source-tree-rest"

const labelValueMax = 40

// StampInLabels reports whether kind carries its source-tree stamp in labels.
func StampInLabels(kind string) bool { return kind == "Deployment" }

// SourceTreeLabels splits tree into the label values that carry it.
func SourceTreeLabels(tree string) map[string]string {
	if len(tree) <= labelValueMax {
		return map[string]string{SourceTreeKey: tree}
	}
	return map[string]string{SourceTreeKey: tree[:labelValueMax], SourceTreeRestKey: tree[labelValueMax:]}
}

// IsSourceTreeLabel reports whether a label key is part of the stamp.
func IsSourceTreeLabel(key string) bool { return key == SourceTreeKey || key == SourceTreeRestKey }

// StampedTree returns the tree stamped on object metadata (labels first, then
// the annotation older installs carry), or "".
func StampedTree(metadata map[string]any) string {
	labels, _ := metadata["labels"].(map[string]any)
	if head, _ := labels[SourceTreeKey].(string); head != "" {
		rest, _ := labels[SourceTreeRestKey].(string)
		return head + rest
	}
	annotations, _ := metadata["annotations"].(map[string]any)
	tree, _ := annotations[SourceTreeKey].(string)
	return strings.TrimSpace(tree)
}
