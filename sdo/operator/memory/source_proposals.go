package memory

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/memory/internal/memoryio"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/safefs"
	"sdo.dev/operator/sdk/program"
)

// CaptureSourceProposals turns the source changes playbook programs proposed
// during an independently verified incident into responder source commits in a
// reflection checkout, one commit per proposal: the object's source file is the
// one the accepted manifest inventory names, the field is set in its YAML
// document, and Go commits the file itself (a deterministic-stage repeat has no
// responder session to commit it). The broker then publishes the commit like a
// responder's own committed source. A proposal whose object has no single YAML
// source file (a rendered chart), whose path does not resolve, or that changes
// nothing is skipped with its reason; the reflection agent still sees it in the
// outcome. created lists the committed files.
func CaptureSourceProposals(ctx context.Context, checkout, incidentID string, proposals []program.SourceChangeProposal) (created []string, skipped []string, err error) {
	if len(proposals) == 0 {
		return nil, nil, nil
	}
	architecture, err := os.ReadFile(filepath.Join(checkout, filepath.FromSlash(layout.ArchitectureFile)))
	if err != nil {
		return nil, nil, fmt.Errorf("read manifest inventory: %w", err)
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
		if dirty, err := proposalGit(ctx, checkout, "status", "--porcelain", "--", file); err != nil {
			return created, skipped, err
		} else if dirty == "" {
			skipped = append(skipped, fmt.Sprintf("%s %s %s: the source already holds the proposed value", proposal.Ref.Kind, proposal.Ref.Name, proposal.Path))
			continue
		}
		subject := fmt.Sprintf("keep %s %s %s from incident %s", proposal.Ref.Kind, proposal.Ref.Name, proposal.Path, incidentID)
		if _, err := proposalGit(ctx, checkout, "add", "--", file); err != nil {
			return created, skipped, err
		}
		if _, err := proposalGit(ctx, checkout, "commit", "--quiet", "--no-verify", "-m", subject, "-m", proposal.Reason, "--", file); err != nil {
			return created, skipped, err
		}
		created = append(created, file)
	}
	return created, skipped, nil
}

// proposalCommitter is the identity of Go's own source-proposal commits.
var proposalCommitter = []string{"GIT_AUTHOR_NAME=sdo-controller", "GIT_AUTHOR_EMAIL=sdo-controller@invalid", "GIT_COMMITTER_NAME=sdo-controller", "GIT_COMMITTER_EMAIL=sdo-controller@invalid"}

func proposalGit(ctx context.Context, checkout string, args ...string) (string, error) {
	raw, err := memoryio.Git(ctx, checkout, true, proposalCommitter, args...)
	if err != nil {
		return "", fmt.Errorf("source proposal git %s: %w: %s", args[0], err, raw)
	}
	return string(bytes.TrimSpace(raw)), nil
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
