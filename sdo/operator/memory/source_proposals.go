package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/patchset"
	"sdo.dev/operator/safefs"
	"sdo.dev/operator/sdk/program"
)

// CaptureSourceProposals turns the source changes playbook programs proposed
// during an independently verified incident into responder-owned patches in a
// reflection checkout, one patch per proposal. The object's source file is
// the one the accepted manifest inventory names; the field is set in its YAML
// document and the edit captured with patchset.Create, so the broker validates
// it like any responder patch. A proposal whose object has no single YAML
// source file (a rendered chart) or whose path does not resolve is skipped
// with its reason; the reflection agent still sees it in the outcome.
func CaptureSourceProposals(ctx context.Context, checkout, incidentID string, proposals []program.SourceChangeProposal) (created []string, skipped []string, err error) {
	if len(proposals) == 0 {
		return nil, nil, nil
	}
	architecture, err := os.ReadFile(filepath.Join(checkout, filepath.FromSlash(layout.ArchitectureFile)))
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest inventory: %w", err)
	}
	if _, err := patchset.Materialize(ctx, checkout); err != nil {
		return nil, nil, fmt.Errorf("materialize patch series: %w", err)
	}
	for _, proposal := range proposals {
		file, why := proposalFile(string(architecture), proposal)
		if file == "" {
			skipped = append(skipped, why)
			continue
		}
		if why := applyProposal(checkout, file, proposal); why != "" {
			skipped = append(skipped, why)
			continue
		}
		patch, err := patchset.Create(ctx, checkout, patchset.CreateRequest{Owner: patchset.OwnerResponder, Incident: incidentID, Reason: proposal.Reason, Paths: []string{file}})
		if err != nil {
			var perr *patchset.PatchError
			if errors.As(err, &perr) || strings.Contains(err.Error(), "no scratch edits") {
				skipped = append(skipped, fmt.Sprintf("%s %s %s: %v", proposal.Ref.Kind, proposal.Ref.Name, proposal.Path, err))
				continue
			}
			return created, skipped, err
		}
		created = append(created, patch.Name)
	}
	return created, skipped, nil
}

func proposalFile(architecture string, proposal program.SourceChangeProposal) (string, string) {
	line := regexp.MustCompile(`(?m)^- ` + regexp.QuoteMeta(proposal.Ref.Kind+"/"+proposal.Ref.Namespace+"/"+proposal.Ref.Name) + ` \(source: ([^)]+)\)`)
	match := line.FindStringSubmatch(architecture)
	if match == nil {
		return "", fmt.Sprintf("%s %s: not in the accepted manifest inventory", proposal.Ref.Kind, proposal.Ref.Name)
	}
	file := strings.TrimSpace(match[1])
	if !strings.HasSuffix(file, ".yaml") && !strings.HasSuffix(file, ".yml") {
		return "", fmt.Sprintf("%s %s: source %s is not a YAML manifest file", proposal.Ref.Kind, proposal.Ref.Name, file)
	}
	return file, ""
}

// applyProposal sets the proposed field in the object's YAML document.
func applyProposal(checkout, file string, proposal program.SourceChangeProposal) string {
	path := filepath.Join(checkout, filepath.FromSlash(file))
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("%s: %v", file, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var documents []*yaml.Node
	found := false
	for {
		var document yaml.Node
		if err := decoder.Decode(&document); err != nil {
			break
		}
		if !found && objectDocument(&document, proposal.Ref.Kind, proposal.Ref.Name) {
			if why := setYAMLField(document.Content[0], proposal.Path, proposal.Value); why != "" {
				return fmt.Sprintf("%s %s %s: %s", proposal.Ref.Kind, proposal.Ref.Name, proposal.Path, why)
			}
			found = true
		}
		documents = append(documents, &document)
	}
	if !found {
		return fmt.Sprintf("%s %s: no document in %s", proposal.Ref.Kind, proposal.Ref.Name, file)
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	encoder.SetIndent(2)
	for _, document := range documents {
		if err := encoder.Encode(document); err != nil {
			return fmt.Sprintf("%s: %v", file, err)
		}
	}
	if err := encoder.Close(); err != nil {
		return fmt.Sprintf("%s: %v", file, err)
	}
	if err := safefs.WriteFile(checkout, filepath.ToSlash(filepath.Clean(file)), out.Bytes(), 0o644); err != nil {
		return fmt.Sprintf("%s: %v", file, err)
	}
	return ""
}

func objectDocument(document *yaml.Node, kind, name string) bool {
	if len(document.Content) == 0 || document.Content[0].Kind != yaml.MappingNode {
		return false
	}
	root := document.Content[0]
	metadata := mappingValue(root, "metadata")
	nameNode := mappingValue(metadata, "name")
	kindNode := mappingValue(root, "kind")
	return kindNode != nil && kindNode.Value == kind && nameNode != nil && nameNode.Value == name
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

var pathSegment = regexp.MustCompile(`^([A-Za-z0-9_./-]+?)((?:\[[^\]]+\])*)$`)
var pathIndex = regexp.MustCompile(`\[([^\]]+)\]`)

// setYAMLField sets a SetField-vocabulary dotted path, creating missing
// mapping keys; list members are selected by index or name=, and data[key]
// selects a ConfigMap key.
func setYAMLField(root *yaml.Node, path string, value any) string {
	var encoded yaml.Node
	raw, err := yaml.Marshal(value)
	if err == nil {
		err = yaml.Unmarshal(raw, &encoded)
	}
	if err != nil || len(encoded.Content) == 0 {
		return "value does not encode as YAML"
	}
	replacement := encoded.Content[0]
	node := root
	segments := strings.Split(path, ".")
	for s, segment := range segments {
		match := pathSegment.FindStringSubmatch(segment)
		if match == nil {
			return "unsupported path segment " + segment
		}
		key, indexes := match[1], pathIndex.FindAllStringSubmatch(match[2], -1)
		if key == "data" && len(indexes) == 1 && s == len(segments)-1 {
			key, indexes = indexes[0][1], nil
			node = ensureMapping(node, "data")
		}
		last := s == len(segments)-1 && len(indexes) == 0
		if last {
			setMapping(node, key, replacement)
			return ""
		}
		next := mappingValue(node, key)
		if next == nil {
			if len(indexes) > 0 {
				return "list " + key + " is absent"
			}
			next = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
			setMapping(node, key, next)
		}
		node = next
		for i, index := range indexes {
			member := listMember(node, index[1])
			if member == nil {
				return "no member " + index[1] + " of " + key
			}
			if s == len(segments)-1 && i == len(indexes)-1 {
				*member = *replacement
				return ""
			}
			node = member
		}
	}
	return ""
}

func ensureMapping(node *yaml.Node, key string) *yaml.Node {
	if existing := mappingValue(node, key); existing != nil {
		return existing
	}
	created := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	setMapping(node, key, created)
	return created
}

func setMapping(node *yaml.Node, key string, value *yaml.Node) {
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			node.Content[i+1] = value
			return
		}
	}
	node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value)
}

func listMember(list *yaml.Node, selector string) *yaml.Node {
	if list == nil || list.Kind != yaml.SequenceNode {
		return nil
	}
	if name, ok := strings.CutPrefix(selector, "name="); ok {
		for _, item := range list.Content {
			if n := mappingValue(item, "name"); n != nil && n.Value == name {
				return item
			}
		}
		return nil
	}
	index, err := strconv.Atoi(selector)
	if err != nil || index < 0 || index >= len(list.Content) {
		return nil
	}
	return list.Content[index]
}
