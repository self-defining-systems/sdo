package imagesrc

import "strings"

// SplitRegistryTarget splits the trusted push target, a registry host with an
// optional repository path prefix ("registry:5000", or
// "registry.example/team/app"), into the host
// (credentials, insecure transport) and the prefix.
func SplitRegistryTarget(target string) (host, prefix string) {
	host, prefix, _ = strings.Cut(target, "/")
	return host, prefix
}

// UnderTarget reports whether image's repository is below the push target:
// it starts with the target and one more path segment.
func UnderTarget(image, target string) bool {
	repository := Repository(image)
	return target != "" && strings.HasPrefix(repository, target+"/") && len(repository) > len(target)+1
}
