package imagesrc

import "strings"

// AuthoredDockerfileDir is where a deployer's authored Dockerfiles (and .dockerignore files) live in the tree: the
// "build" deployment files of operational memory (memory/layout pins the two together).
const AuthoredDockerfileDir = ".sdo/deployment/build"

// memoryDir is operational memory, which a build never reads except AuthoredDockerfileDir.
const memoryDir = ".sdo"

// BuildInputAllowed is the one rule for where an image build may read, used by build admission and by the
// recorded-inputs check alike. p is a clean, tree-relative path. A build context is application source and never
// lies in operational memory; a Dockerfile is application source or a deployer-authored file under
// AuthoredDockerfileDir.
func BuildInputAllowed(p string, dockerfile bool) bool {
	if p != memoryDir && !strings.HasPrefix(p, memoryDir+"/") {
		return true
	}
	return dockerfile && strings.HasPrefix(p, AuthoredDockerfileDir+"/")
}
