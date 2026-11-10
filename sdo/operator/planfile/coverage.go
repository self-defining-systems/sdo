package planfile

import (
	"path"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Unbuilt returns, sorted, the repository paths no deployment of this plan
// reads: a path under no BuildKit image's context or Dockerfile and under no
// path the renderer's config names. A change to such a file never reaches a
// running workload (its image is pulled as published), so the patch that
// carries it is recorded but cannot take effect.
func Unbuilt(plan *contracts.DeploymentPlan, paths []string) []string {
	var covered []string
	for _, image := range plan.GetImages() {
		if image.GetSourceId() != "buildkit" || image.GetConfig() == nil {
			continue
		}
		if config, err := image.GetConfig().UnmarshalNew(); err == nil {
			covered = append(covered, stringFields(config)...)
		}
	}
	if source := plan.GetSource(); source.GetConfig() != nil {
		if config, err := source.GetConfig().UnmarshalNew(); err == nil {
			covered = append(covered, stringFields(config)...)
		}
	}
	var out []string
	for _, p := range paths {
		if !underAny(p, covered) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func underAny(p string, roots []string) bool {
	for _, root := range roots {
		if root == "" {
			continue
		}
		root = path.Clean(root)
		if root == "." || p == root || strings.HasPrefix(p, root+"/") {
			return true
		}
	}
	return false
}

// stringFields lists every string value of a config message, scalar or
// repeated: the renderer configs name their inputs as tree paths.
func stringFields(m proto.Message) []string {
	var out []string
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.StringKind {
			return true
		}
		if fd.IsList() {
			for i := 0; i < v.List().Len(); i++ {
				out = append(out, v.List().Get(i).String())
			}
		} else if s := v.String(); s != "" {
			out = append(out, s)
		}
		return true
	})
	return out
}
