package imagesrc

import "strings"

// memoryDir is operational memory, which a build never reads.
const memoryDir = ".sdo"

// BuildInputAllowed is the one rule for where an image build may read, used by build admission and by the
// recorded-inputs check alike. p is a clean, tree-relative path. A build context and its Dockerfile are application
// source (committed files of the repository) and never lie in operational memory.
func BuildInputAllowed(p string, dockerfile bool) bool {
	return p != memoryDir && !strings.HasPrefix(p, memoryDir+"/")
}
