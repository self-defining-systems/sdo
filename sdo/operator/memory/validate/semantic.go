package validate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/sdk"
)

var (
	rolePlaceholder = regexp.MustCompile(`<\w*[A-Z][A-Z0-9_]+>`)
	markdownLink    = regexp.MustCompile(`\[[^]]+\]\(([^)]+)\)`)
)

func (s *run) semantic(ctx context.Context) error {
	if s.layers()&Semantic == 0 {
		return nil
	}
	if s.memory {
		s.completeness()
		s.documents()
		s.observationRules()
		s.playbookIndex()
		s.detectorRules()
		if err := s.scripts(ctx); err != nil {
			return err
		}
		s.physicalPaths()
		if err := s.programRules(ctx); err != nil {
			return err
		}
		if s.req.Base != "" {
			base, err := exportBase(ctx, s.req.Root, s.req.Base)
			if err != nil {
				return err
			}
			defer os.RemoveAll(base)
			if err := s.corpusRules(base); err != nil {
				return err
			}

		} else if err := s.corpusRules(""); err != nil {
			return err
		}
	}
	return s.architectureCommit(ctx)
}

func (s *run) completeness() {
	if !s.complete() {
		return
	}
	if s.goal == nil && !s.hasFile(layout.GoalFile) {
		s.add(Semantic, Diagnostic{Rule: RuleGoalMissing, Path: layout.GoalFile, Message: "goal.md is missing", Hint: "the human goal document is required"})
	}
	if s.arch == nil && !s.hasFile(layout.ArchitectureFile) {
		s.add(Semantic, Diagnostic{Rule: RuleArchitectureMissing, Path: layout.ArchitectureFile, Message: "arch.md is missing"})
	}
	if len(s.playbooks) == 0 {
		s.add(Semantic, Diagnostic{Rule: RulePlaybookNone, Path: layout.PlaybooksDir, Message: "at least one playbook is required", Hint: "create .sdo/playbooks/<id>/README.md"})
	}
	if len(s.detectors) == 0 {
		s.add(Semantic, Diagnostic{Rule: RuleDetectorNone, Path: layout.DetectorsDir, Message: "memory registers no detectors"})
	}
}

// hasFile reports whether a classified file exists even though it failed to parse.
func (s *run) hasFile(rel string) bool {
	switch rel {
	case layout.GoalFile:
		return s.goal != nil || s.parseFailed[rel]
	case layout.ArchitectureFile:
		return s.arch != nil || s.parseFailed[rel]
	}
	return false
}

func (s *run) documents() {
	if s.goal != nil && s.goal.doc.Meta["owner"] != "human" {
		s.add(Semantic, Diagnostic{Rule: RuleGoalOwner, Path: s.goal.path, Message: "goal owner must be human", Hint: "set `owner: human` in the front matter"})
	}
	for _, book := range s.sortedPlaybooks() {
		if !book.hasReadme {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookNoReadme, Path: layout.PlaybookDir(book.id), Message: fmt.Sprintf("playbook %s has no %s", book.id, layout.PlaybookFileName)})
			continue
		}
		if book.readme == nil {
			continue
		}
		doc, at := book.readme.doc, book.readme.path
		if _, err := layout.PlaybookRetired(doc); err != nil {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookStatus, Path: at, Message: err.Error()})
		}
		if doc.Meta["owner"] != "responder" {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookOwner, Path: at, Message: "playbook owner must be responder"})
		}
		fault := layout.PlaybookFaultClass(book.id)
		if doc.Meta["fault_class"] != fault {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookFaultClass, Path: at, Message: fmt.Sprintf("fault_class must be %s", fault)})
		}
		if incident, ok := doc.Meta["originating_incident"]; !ok || incident == nil || strings.TrimSpace(fmt.Sprint(incident)) == "" {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookIncident, Path: at, Message: "originating_incident is required"})
		}
		if !rolePlaceholder.MatchString(doc.Body) {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookPlaceholder, Path: at, Message: "playbook must use a role placeholder", Hint: "write resource names as <RESOURCE_NAME> style placeholders, not literals"})
		}
	}
}

func (s *run) sortedPlaybooks() []*playbook {
	books := make([]*playbook, 0, len(s.playbooks))
	for _, book := range s.playbooks {
		books = append(books, book)
	}
	sort.Slice(books, func(i, j int) bool { return books[i].id < books[j].id })
	return books
}

func (s *run) sortedDetectors() []*detector {
	list := make([]*detector, 0, len(s.detectors))
	for _, d := range s.detectors {
		list = append(list, d)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].class != list[j].class {
			return list[i].class == layout.ClassHealth
		}
		return list[i].id < list[j].id
	})
	return list
}

func (s *run) playbookIndex() {
	if s.index == nil {
		if s.complete() && len(s.playbooks) > 0 {
			s.add(Semantic, Diagnostic{Rule: RuleIndexMissing, Path: layout.PlaybookIndexFile, Message: "the playbook index is required", Hint: "list every playbook as a Markdown link in .sdo/playbooks/README.md"})
		}
		return
	}
	linked := map[string]bool{}
	for _, match := range markdownLink.FindAllStringSubmatchIndex(string(s.index.content), -1) {
		target := string(s.index.content[match[2]:match[3]])
		line := strings.Count(string(s.index.content[:match[0]]), "\n") + 1
		link := strings.SplitN(target, "#", 2)[0]
		if strings.Contains(link, "://") || link == "" {
			continue
		}
		clean := path.Clean(link)
		if path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
			s.add(Semantic, Diagnostic{Rule: RuleIndexLink, Path: s.index.path, Line: line, Message: fmt.Sprintf("index link escapes the playbooks directory: %s", link)})
			continue
		}
		if !strings.HasSuffix(clean, layout.PlaybookFileName) {
			clean = path.Join(clean, layout.PlaybookFileName)
		}
		id, _, _ := strings.Cut(clean, "/")
		if book := s.playbooks[id]; book == nil || !book.hasReadme || clean != id+"/"+layout.PlaybookFileName {
			s.add(Semantic, Diagnostic{Rule: RuleIndexLink, Path: s.index.path, Line: line, Message: fmt.Sprintf("index link does not name a playbook: %s", link)})
			continue
		}
		linked[id] = true
	}
	if !s.complete() {
		return
	}
	for _, book := range s.sortedPlaybooks() {
		if book.hasReadme && !linked[book.id] {
			s.add(Semantic, Diagnostic{Rule: RulePlaybookNotIndexed, Path: s.index.path, Message: fmt.Sprintf("playbook %s is missing from the index", book.id), Hint: fmt.Sprintf("add `[%s](%s/%s)`", book.id, book.id, layout.PlaybookFileName)})
		}
	}
}

func (s *run) detectorRules() {
	seen := map[string]string{}
	for _, det := range s.sortedDetectors() {
		dir := layout.DetectorDir(det.class, det.id)
		if !det.hasManifest {
			s.add(Semantic, Diagnostic{Rule: RuleManifestMissing, Path: dir, Message: fmt.Sprintf("detector %s has no %s", det.key(), layout.ManifestFileName), Hint: "every detector package registers itself in its own manifest.yaml"})
			continue
		}
		if !det.hasGoSource() {
			s.add(Semantic, Diagnostic{Rule: RuleDetectorNoSource, Path: dir, Message: fmt.Sprintf("detector %s package has no Go source", det.id), Hint: "add detector.go to the package directory"})
		}
		if first, dup := seen[det.id]; dup {
			s.add(Semantic, Diagnostic{Rule: RuleDuplicateID, Path: det.manifestAt, Message: fmt.Sprintf("detector ID %q is already used by %s", det.id, first)})
		}
		seen[det.id] = det.manifestAt
		if det.manifest == nil {
			continue
		}
		for _, reference := range det.manifest.PossiblePlaybooks {
			if !layout.ValidPlaybookID(reference) {
				continue
			}
			if book := s.playbooks[reference]; book == nil || !book.hasReadme {
				s.add(Semantic, Diagnostic{Rule: RuleUnknownPlaybook, Path: det.manifestAt, Line: firstLine(det.manifestText, func(line string) bool { return strings.Contains(line, reference) }), Message: fmt.Sprintf("detector %s names playbook %q, which does not exist", det.id, reference), Hint: "create the playbook or remove it from possiblePlaybooks"})
			}
		}
	}
}

func (s *run) scripts(ctx context.Context) error {
	for _, book := range s.sortedPlaybooks() {
		sort.Slice(book.scripts, func(i, j int) bool { return book.scripts[i].path < book.scripts[j].path })
		for _, script := range book.scripts {
			name := path.Base(script.path)
			if script.mode.Perm()&0o111 == 0 {
				s.add(Semantic, Diagnostic{Rule: RuleScriptNotExecutable, Path: script.path, Message: "playbook scripts must be executable", Hint: "chmod +x " + script.path})
			}
			if book.readme != nil && !strings.Contains(book.readme.doc.Body, name) {
				s.add(Semantic, Diagnostic{Rule: RuleScriptUnreferenced, Path: script.path, Message: fmt.Sprintf("playbook %s never mentions script %s", book.id, name), Hint: "reference the script by name in the playbook README"})
			}
			if err := bashSyntax(ctx, script.content); err != nil {
				var failure *syntaxFailure
				if !errors.As(err, &failure) {
					return err
				}
				s.add(Semantic, Diagnostic{Rule: RuleScriptSyntax, Path: script.path, Line: failure.line, Message: "invalid shell syntax: " + failure.message})
			}
		}
	}
	return nil
}

type syntaxFailure struct {
	line    int
	message string
}

func (f *syntaxFailure) Error() string { return f.message }

var bashLine = regexp.MustCompile(`line (\d+):`)

func bashSyntax(ctx context.Context, script []byte) error {
	cmd := exec.CommandContext(ctx, "bash", "-n")
	cmd.Stdin = bytes.NewReader(script)
	output, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		return fmt.Errorf("bash -n: %w", err)
	}
	failure := &syntaxFailure{message: strings.TrimSpace(string(output))}
	if m := bashLine.FindStringSubmatch(failure.message); m != nil {
		fmt.Sscan(m[1], &failure.line)
	}
	return failure
}

// physicalPaths rejects physical memory paths in the text that agents author
// and other agents read: the architecture, the playbook index and playbooks,
// playbook scripts, and detector Go source. Detector code addresses playbooks by
// logical ID, never by path.
func (s *run) physicalPaths() {
	check := func(file textFile) {
		if line := firstLine(file.content, layout.ContainsPhysicalPath); line > 0 {
			s.add(Semantic, Diagnostic{Rule: RulePhysicalPath, Path: file.path, Line: line, Message: "text names a physical operational-memory path", Hint: "refer to playbooks as playbook:<id> and scripts as script:<playbook>/<name>"})
		}
	}
	if s.index != nil {
		check(*s.index)
	}
	for _, book := range s.sortedPlaybooks() {
		if book.hasReadme {
			if file, ok := s.readFile(layout.PlaybookDir(book.id) + "/" + layout.PlaybookFileName); ok {
				check(file)
			}
		}
		for _, script := range book.scripts {
			check(script)
		}
	}
	if s.arch != nil {
		check(textFile{path: s.arch.path, content: []byte(s.arch.doc.Source)})
	}
	for _, det := range s.sortedDetectors() {
		for _, file := range det.files {
			if strings.HasSuffix(file.path, ".go") {
				check(file)
			}
		}
	}
}

func (s *run) readFile(rel string) (textFile, bool) {
	for _, book := range s.playbooks {
		if book.readme != nil && book.readme.path == rel {
			return textFile{path: rel, content: []byte(book.readme.doc.Source)}, true
		}
	}
	return textFile{}, false
}

// architectureCommit enforces that a deployer's initial architecture cites the
// broker-accepted source commit it was derived from, which is the base.
func (s *run) architectureCommit(ctx context.Context) error {
	if s.req.Actor != Deployer || s.req.Phase != InitialPhase || s.req.Base == "" || s.arch == nil || !containsPath(s.changed, layout.ArchitectureFile) {
		return nil
	}
	fail := func(rule, message, hint string) {
		s.add(Semantic, Diagnostic{Rule: rule, Path: layout.ArchitectureFile, Message: message, Hint: hint})
	}
	sourceCommit, err := gitText(ctx, s.req.Root, "rev-parse", s.req.Base+"^{commit}")
	if err != nil {
		return err
	}
	message, err := gitText(ctx, s.req.Root, "log", "-1", "--format=%B", sourceCommit)
	if err != nil {
		return err
	}
	wrapped := "\n" + message + "\n"
	if !strings.Contains(wrapped, "\nSDO-Actor: deployer\n") || !strings.Contains(wrapped, "\nSDO-Phase: initial\n") {
		fail(RuleArchitectureBaseline, "initial architecture must follow a broker-accepted deployer source commit", "propose the architecture only after the source proposal is accepted")
		return nil
	}
	changed, err := gitText(ctx, s.req.Root, "diff-tree", "--root", "--no-commit-id", "--name-only", "-r", sourceCommit)
	if err != nil {
		return err
	}
	hasSource := false
	for _, p := range strings.Split(changed, "\n") {
		if p != "" && !layout.IsMemoryPath(p) {
			hasSource = true
			break
		}
	}
	if !hasSource {
		fail(RuleArchitectureBaseline, "initial architecture has no accepted deployer source commit", "")
		return nil
	}
	parts := strings.SplitN(s.arch.doc.Source, "---\n", 3)
	var metadata struct {
		GeneratedAtCommit string `yaml:"generated_at_commit"`
	}
	if len(parts) == 3 {
		if err := yaml.Unmarshal([]byte(parts[1]), &metadata); err != nil {
			return nil
		}
	}
	if metadata.GeneratedAtCommit != sourceCommit {
		fail(RuleArchitectureCommit, fmt.Sprintf("generated_at_commit must cite the accepted source commit %s", sourceCommit), "set generated_at_commit in the front matter")
	}
	return nil
}

func containsPath(paths []string, target string) bool {
	for _, p := range paths {
		if p == target {
			return true
		}
	}
	return false
}

func (s *run) observationRules() {
	if s.arch == nil {
		return
	}
	scope, err := observation.Parse([]byte(s.arch.doc.Source))
	if err == nil {
		err = scope.Within(s.req.ObservationCeiling)
	}
	if err != nil {
		s.add(Semantic, Diagnostic{Rule: "observation.scope", Path: layout.ArchitectureFile, Message: err.Error()})
		return
	}
	for _, d := range s.detectors {
		if d.manifest == nil {
			continue
		}
		for _, w := range d.manifest.Watches {
			if !scope.AllowsWatch(sdk.WatchKind{APIVersion: w.APIVersion, Kind: w.Kind, Namespace: w.Namespace}, s.req.ApplicationNamespace) {
				s.add(Semantic, Diagnostic{Rule: "observation.watch", Path: layout.ArchitectureFile, Message: "detector watch exceeds accepted observation scope"})
			}
		}
	}
}
