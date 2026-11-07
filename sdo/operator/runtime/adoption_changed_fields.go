package runtime

import (
	"reflect"
	"sort"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// maxChangedFieldPaths bounds the paths a skipped adoption reports.
const maxChangedFieldPaths = 8

// changedFieldPaths lists the field paths whose values differ between live and
// preview (the server's dry run of an apply), never their values: server
// bookkeeping and status are ignored. The result is never nil.
func changedFieldPaths(live, preview *unstructured.Unstructured) []string {
	paths := []string{}
	var walk func(path string, a, b any)
	walk = func(path string, a, b any) {
		switch av := a.(type) {
		case map[string]any:
			if bv, ok := b.(map[string]any); ok {
				for key := range av {
					walk(path+"."+key, av[key], bv[key])
				}
				for key := range bv {
					if _, seen := av[key]; !seen {
						walk(path+"."+key, nil, bv[key])
					}
				}
				return
			}
		case []any:
			if bv, ok := b.([]any); ok && len(av) == len(bv) {
				for i := range av {
					walk(path+"["+strconv.Itoa(i)+"]", av[i], bv[i])
				}
				return
			}
		}
		if !reflect.DeepEqual(a, b) {
			paths = append(paths, path[1:])
		}
	}
	prepare := func(object *unstructured.Unstructured) map[string]any {
		out := object.DeepCopy().Object
		delete(out, "status")
		if metadata, ok := out["metadata"].(map[string]any); ok {
			for _, key := range []string{"resourceVersion", "managedFields", "generation"} {
				delete(metadata, key)
			}
		}
		return out
	}
	walk("", prepare(live), prepare(preview))
	sort.Strings(paths)
	if len(paths) > maxChangedFieldPaths {
		paths = paths[:maxChangedFieldPaths]
	}
	return paths
}
