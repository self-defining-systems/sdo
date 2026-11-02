package memory

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
)

// CapturedSessionRefs derives scope only from the current incident's action,
// CLI, primitive and Kubernetes audit envelopes. It never reads response blobs,
// model credentials, or other incidents. Contents do not enter durable memory.
func CapturedSessionRefs(session string, initial []sdk.ObjectRef) ([]sdk.ObjectRef, error) {
	refs := map[sdk.ObjectRef]bool{}
	add := func(ref sdk.ObjectRef) {
		if ref.Kind != "" && !strings.EqualFold(ref.Kind, "Secret") && !strings.EqualFold(ref.Kind, "secrets") {
			refs[ref] = true
		}
	}
	for _, ref := range initial {
		add(ref)
	}
	var visit func(any)
	visit = func(value any) {
		switch v := value.(type) {
		case []any:
			for _, item := range v {
				visit(item)
			}
		case map[string]any:
			get := func(key string) string { s, _ := v[key].(string); return s }
			api := get("apiVersion")
			if api == "" {
				api = get("api_version")
			}
			ref := sdk.ObjectRef{APIVersion: api, Kind: get("kind"), Namespace: get("namespace"), Name: get("name")}
			if meta, ok := v["metadata"].(map[string]any); ok {
				ref.Name, _ = meta["name"].(string)
				ref.Namespace, _ = meta["namespace"].(string)
			}
			add(ref)
			// API-history events use resource names, resolved through discovery later.
			if resource := get("resource"); resource != "" {
				version := get("version")
				if group := get("group"); group != "" && version != "" {
					version = group + "/" + version
				}
				add(sdk.ObjectRef{APIVersion: version, Kind: resource, Namespace: get("namespace"), Name: get("name")})
			}
			for key, item := range v {
				if key == "command" || key == "commands" || key == "cmd" || key == "argv" {
					switch cmd := item.(type) {
					case string:
						for _, ref := range commandCaptureRefs(cmd) {
							add(ref)
						}
					case []any:
						var words []string
						for _, w := range cmd {
							if text, ok := w.(string); ok {
								words = append(words, text)
							}
						}
						for _, ref := range commandCaptureRefs(strings.Join(words, " ")) {
							add(ref)
						}
					}
				}
				visit(item)
			}
		}
	}
	err := filepath.WalkDir(session, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := strings.ToLower(entry.Name())
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.IsDir() {
			switch name {
			case "codex", "claude", "model_io", "blobs", "cache", "checkpoints":
				return filepath.SkipDir
			}
			return nil
		}
		if strings.Contains(name, "auth") || strings.Contains(name, "credential") || strings.Contains(name, "token") || strings.Contains(name, "secret") {
			return nil
		}
		if !strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".jsonl") {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		if strings.HasSuffix(name, ".jsonl") {
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 4096), 8<<20)
			for scanner.Scan() {
				var value any
				if json.Unmarshal(scanner.Bytes(), &value) == nil {
					visit(value)
				}
			}
			if err := scanner.Err(); err != nil {
				return fmt.Errorf("read capture scope %s: %w", name, err)
			}
		} else {
			info, err := file.Stat()
			if err != nil {
				return err
			}
			if info.Size() > 8<<20 {
				return fmt.Errorf("capture scope file %s exceeds 8 MiB", name)
			}
			var value any
			if json.NewDecoder(file).Decode(&value) == nil {
				visit(value)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]sdk.ObjectRef, 0, len(refs))
	for ref := range refs {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]) < fmt.Sprint(out[j]) })
	return out, nil
}

var captureCommandWords = regexp.MustCompile(`[^\s'";|&()]+`)

func commandCaptureRefs(command string) []sdk.ObjectRef {
	words := captureCommandWords.FindAllString(command, -1)
	var refs []sdk.ObjectRef
	for start, w := range words {
		if filepath.Base(w) != "kubectl" {
			continue
		}
		ns := ""
		for i := start + 1; i < len(words); i++ {
			if (words[i] == "-n" || words[i] == "--namespace") && i+1 < len(words) {
				ns = words[i+1]
			}
			if strings.HasPrefix(words[i], "--namespace=") {
				ns = strings.TrimPrefix(words[i], "--namespace=")
			}
		}
		var args []string
		for i := start + 1; i < len(words); i++ {
			w := words[i]
			if strings.HasPrefix(w, "-") {
				if (w == "-n" || w == "--namespace" || w == "--context" || w == "--kubeconfig" || w == "-o" || w == "--output" || w == "-l" || w == "--selector" || w == "-p" || w == "--patch" || w == "-f" || w == "--filename") && i+1 < len(words) {
					i++
				}
				continue
			}
			args = append(args, w)
		}
		for i, w := range args {
			if kind, name, ok := strings.Cut(w, "/"); ok && kind != "" && name != "" && !strings.Contains(name, "/") && !strings.ContainsAny(w, "={}") {
				refs = append(refs, sdk.ObjectRef{Kind: kind, Name: name, Namespace: ns})
				continue
			}
			if i == 1 && len(args) > 1 {
				ref := sdk.ObjectRef{Kind: w, Namespace: ns}
				if len(args) > 2 && !strings.ContainsAny(args[2], "={}/") {
					ref.Name = args[2]
				}
				refs = append(refs, ref)
			}
		}
	}
	return refs
}
