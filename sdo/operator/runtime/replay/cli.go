package replay

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// LoadCases recursively finds CorpusCase directories. Each has an accepted.yaml
// or accepted.json alongside labels.json, or uses the explicit shared source.
func LoadCases(root, sharedSource string) ([]Case, error) {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("fixture symlink is unsupported: %s", path)
		}
		if !d.IsDir() && d.Name() == "labels.json" {
			dirs = append(dirs, filepath.Dir(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(dirs)
	var shared []*unstructured.Unstructured
	if sharedSource != "" {
		shared, err = LoadManifests(sharedSource)
		if err != nil {
			return nil, err
		}
	}
	out := []Case{}
	for _, dir := range dirs {
		corpus, err := sdktest.LoadCorpusCase(dir)
		if err != nil {
			return nil, err
		}
		id, _ := filepath.Rel(root, dir)
		if id == "." {
			id = filepath.Base(dir)
		}
		c := Case{ID: id, Corpus: corpus, Accepted: shared}
		found := false
		for _, name := range []string{"accepted.yaml", "accepted.json"} {
			path := filepath.Join(dir, name)
			if _, e := os.Stat(path); e == nil {
				if found {
					return nil, fmt.Errorf("case %s has multiple accepted sources", id)
				}
				c.Accepted, err = LoadManifests(path)
				if err != nil {
					return nil, err
				}
				found = true
			} else if !os.IsNotExist(e) {
				return nil, e
			}
		}
		if !found && sharedSource == "" {
			return nil, fmt.Errorf("case %s requires accepted.yaml/accepted.json or -source (use [] for an explicitly empty inventory)", id)
		}
		revisions := filepath.Join(dir, "revisions")
		entries, e := os.ReadDir(revisions)
		if e != nil && !os.IsNotExist(e) {
			return nil, e
		}
		c.Revisions = map[string][]*unstructured.Unstructured{}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			ext := filepath.Ext(entry.Name())
			if ext != ".yaml" && ext != ".json" {
				continue
			}
			rev := strings.TrimSuffix(entry.Name(), ext)
			c.Revisions[rev], err = LoadManifests(filepath.Join(revisions, entry.Name()))
			if err != nil {
				return nil, err
			}
		}
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("fixtures contain no labels.json: %s", root)
	}
	return out, nil
}

// LoadManifests accepts multi-document YAML, a JSON array, or a Kubernetes List.
// Secret resources are refused; their bodies never appear in diagnostics.
func LoadManifests(path string) ([]*unstructured.Unstructured, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	decoder := yamlutil.NewYAMLOrJSONDecoder(f, 4096)
	out := []*unstructured.Unstructured{}
	var add func(any) error
	add = func(value any) error {
		switch v := value.(type) {
		case nil:
			return nil
		case []any:
			for _, item := range v {
				if err := add(item); err != nil {
					return err
				}
			}
			return nil
		case map[string]any:
			if v["kind"] == "Secret" {
				return fmt.Errorf("Secret manifest refused")
			}
			if v["kind"] == "List" {
				return add(v["items"])
			}
			raw, e := json.Marshal(v)
			if e != nil {
				return e
			}
			o := &unstructured.Unstructured{}
			if e = o.UnmarshalJSON(raw); e != nil {
				return fmt.Errorf("invalid manifest identity")
			}
			out = append(out, o)
			return nil
		default:
			return fmt.Errorf("manifest must be an object or array")
		}
	}
	for {
		var value any
		err = decoder.Decode(&value)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("invalid manifest document in %s", path)
		}
		if err = add(value); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Run is the compiled bundle entry point. JSONL goes to stdout, the short
// false-positive table and timing to stderr. It has no live client configuration.
func Run(ctx context.Context, registry program.ProgramRegistry, detectors []sdk.Detector, args []string, out, summary io.Writer) int {
	flags := flag.NewFlagSet("closed-loop", flag.ContinueOnError)
	flags.SetOutput(summary)
	fixtures := flags.String("fixtures", "", "CorpusCase fixtures directory")
	source := flags.String("source", "", "shared accepted manifests (optional when each case has accepted.yaml/json)")
	matrix := flags.Bool("matrix", true, "evaluate all programs/detectors against all cases")
	repeat := flags.Int("repeat", 1, "repeat cases to measure replay cost, without rebuilding")
	if flags.Parse(args) != nil {
		return 2
	}
	if *fixtures == "" || *repeat < 1 || flags.NArg() != 0 {
		fmt.Fprintln(summary, "-fixtures is required; -repeat must be positive")
		return 2
	}
	cases, err := LoadCases(*fixtures, *source)
	if err != nil {
		fmt.Fprintln(summary, err)
		return 2
	}
	start := time.Now()
	encoder := json.NewEncoder(out)
	count, failed, positives := 0, 0, 0
	fmt.Fprintln(summary, "FALSE POSITIVES (program/case/trigger)")
	for n := 0; n < *repeat; n++ {
		var rows []Result
		if *matrix {
			rows = Matrix(ctx, registry, detectors, cases)
		} else {
			for _, c := range cases {
				for _, spec := range registry.List() {
					if !contains(c.Corpus.RootCauseProgramIDs, spec.ID) {
						continue
					}
					p, _ := registry.Diagnoser(spec.ID)
					m, _ := registry.Mitigator(spec.ID)
					var ds []sdk.Detector
					for _, d := range detectors {
						if contains(d.Spec().Playbooks, spec.ID) {
							ds = append(ds, d)
						}
					}
					rows = append(rows, Check(ctx, p, m, ds, c))
				}
			}
		}
		for _, r := range rows {
			if r.ExpectedMatch && !strings.HasPrefix(r.ProgramID, "detector:") && len(r.Detectors["faulted"]) == 0 {
				r.Status = "failed"
				r.Errors = append(r.Errors, "matching program has no linked detector")
			}
			if err = encoder.Encode(r); err != nil {
				fmt.Fprintln(summary, err)
				return 2
			}
			count++
			if r.Status != "replayed" && r.Status != "negative" && r.Status != "detected" {
				failed++
			}
			for _, fp := range r.FalsePositives {
				positives++
				fmt.Fprintf(summary, "%s\t%s\t%s\n", r.ProgramID, r.CaseID, fp)
			}
		}
	}
	elapsed := time.Since(start)
	fmt.Fprintf(summary, "%d rows, %d failed/refused, %d false positives; %d cases in %s (%.3f ms/case after build)\n", count, failed, positives, len(cases)**repeat, elapsed, float64(elapsed)/float64(time.Millisecond)/float64(len(cases)**repeat))
	if failed > 0 {
		return 1
	}
	return 0
}
func contains(values []string, v string) bool {
	for _, s := range values {
		if s == v {
			return true
		}
	}
	return false
}
