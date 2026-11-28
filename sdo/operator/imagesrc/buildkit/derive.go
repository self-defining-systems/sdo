package buildkit

import (
	"google.golang.org/protobuf/types/known/anypb"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// DerivedConfig is the build config of an image the tree's files match to a
// Dockerfile (imagesrc.BuildFor): its build context and Dockerfile path.
func DerivedConfig(contextPath, dockerfile string) (*anypb.Any, error) {
	return anypb.New(&contracts.BuildKitImageConfig{ContextPath: contextPath, DockerfilePath: dockerfile})
}
