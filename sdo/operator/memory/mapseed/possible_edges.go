package mapseed

import (
	"regexp"
	"sort"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/rendered"
)

// PossibleEdge is a call the manifests suggest without naming its address: a
// Secret reference (an env var from a Secret key, an envFrom Secret, a mounted
// Secret) whose env var, Secret or key name names a Service of the
// application. Go never reads the Secret's value, so it is a candidate for the
// designer to confirm on the live application, not an edge.
type PossibleEdge struct {
	From, To string
	Source   string
}

var nameToken = regexp.MustCompile(`[a-z0-9]+`)

// minStemMatch is the shortest Service stem matched by prefix; a shorter stem
// must equal a word of the reference.
const minStemMatch = 5

// serviceStems strips the leading dash-separated words every Service shares
// (a release prefix such as "shop-"), leaving the part that tells them apart.
func serviceStems(names []string) map[string]string {
	shared := []string(nil)
	if len(names) > 1 {
		shared = strings.Split(names[0], "-")
		for _, name := range names[1:] {
			words := strings.Split(name, "-")
			n := 0
			for n < len(shared) && n < len(words)-1 && shared[n] == words[n] {
				n++
			}
			shared = shared[:n]
		}
	}
	out := map[string]string{}
	for _, name := range names {
		words := strings.Split(name, "-")
		stem := strings.Join(words[min(len(shared), len(words)-1):], "")
		out[name] = stem
	}
	return out
}

// names reports whether reference text names the Service stem: a word or two
// adjacent words of the text equal the stem, or share a prefix with it of at
// least minStemMatch characters and four fifths of the stem.
func names(text, stem string) bool {
	words := nameToken.FindAllString(strings.ToLower(text), -1)
	var candidates []string
	for i, word := range words {
		candidates = append(candidates, word)
		if i+1 < len(words) {
			candidates = append(candidates, word+words[i+1])
		}
	}
	for _, candidate := range candidates {
		if candidate == stem {
			return true
		}
		n := 0
		for n < len(candidate) && n < len(stem) && candidate[n] == stem[n] {
			n++
		}
		if len(stem) >= minStemMatch && n >= minStemMatch && 5*n >= 4*len(stem) {
			return true
		}
	}
	return false
}

// findPossibleEdges lists, per component, the Services its Secret references
// name and that no seeded edge already reaches.
func (s *seeder) findPossibleEdges() []PossibleEdge {
	serviceNames := make([]string, 0, len(s.services))
	for name := range s.services {
		serviceNames = append(serviceNames, name)
	}
	sort.Strings(serviceNames)
	stems := serviceStems(serviceNames)
	seen := map[[2]string]bool{}
	var out []PossibleEdge
	for _, id := range s.order {
		c := s.components[id]
		consider := func(source, text string) {
			for _, name := range serviceNames {
				if !names(text, stems[name]) {
					continue
				}
				for _, target := range s.services[name].components {
					key := [2]string{c.Name, target}
					if _, edge := s.edges[key]; edge || target == c.Name || seen[key] {
						continue
					}
					seen[key] = true
					out = append(out, PossibleEdge{From: c.Name, To: target, Source: c.Workload + " " + source})
				}
			}
		}
		pod, _ := rendered.PodSpec(c.object.Object)
		for _, group := range rendered.ContainerFields {
			for _, value := range listValue(pod[group]) {
				container := mapValue(value)
				for _, item := range listValue(container["env"]) {
					env := mapValue(item)
					ref := mapValue(mapValue(env["valueFrom"])["secretKeyRef"])
					if secret, key := stringValue(ref["name"]), stringValue(ref["key"]); secret != "" {
						name := stringValue(env["name"])
						consider("env "+name+" from Secret/"+secret+" key "+key, name+" "+secret+" "+key)
					}
				}
				for _, item := range listValue(container["envFrom"]) {
					if secret := stringValue(mapValue(mapValue(item)["secretRef"])["name"]); secret != "" {
						consider("envFrom Secret/"+secret, secret)
					}
				}
			}
		}
		for _, volume := range listValue(pod["volumes"]) {
			v := mapValue(volume)
			secrets := []map[string]any{mapValue(v["secret"])}
			for _, source := range listValue(mapValue(v["projected"])["sources"]) {
				secrets = append(secrets, mapValue(mapValue(source)["secret"]))
			}
			for _, secret := range secrets {
				name := stringValue(secret["secretName"]) + stringValue(secret["name"])
				if name == "" {
					continue
				}
				keys := ""
				for _, item := range listValue(secret["items"]) {
					keys += " " + stringValue(mapValue(item)["key"])
				}
				consider("volume Secret/"+name, name+keys)
			}
		}
	}
	return out
}

// possibleEdgeLine is how the architecture text lists a possible edge; the
// memory check reads it back (appmap.ParsePossibleEdges).
func possibleEdgeLine(e PossibleEdge) string {
	return appmap.PossibleEdgePrefix + e.From + " -> " + e.To + " (" + e.Source + ")"
}
