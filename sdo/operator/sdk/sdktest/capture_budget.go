package sdktest

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
)

// trimOrder lists the kinds a capture drops first when the snapshot is over
// budget, least relevant first. Kinds absent from the list rank after it and
// before the protected kinds. Pods, Deployments, StatefulSets and Services are
// never trimmed: every health and incident detector reads them.
var trimOrder = []string{
	"Event", "ConfigMap", "ServiceAccount", "ReplicaSet", "ControllerRevision", "PodDisruptionBudget",
	"NetworkPolicy", "Ingress", "PersistentVolumeClaim", "Job", "CronJob", "DaemonSet",
	"EndpointSlice", "Endpoints", "HorizontalPodAutoscaler", "Secret", "Node",
}

var protectedKinds = []string{"Pod", "Deployment", "StatefulSet", "Service"}

type trimCandidate struct {
	kind string
	size int
}

// trimToBudget drops the least relevant objects, largest first within a kind,
// until the encoded snapshot fits limit, and records the counts by kind in
// TrimmedObjects. It fails only when the protected kinds alone exceed limit.
func (s *Snapshot) trimToBudget(limit int) error {
	size, err := encodedSize(*s)
	if err != nil || size <= limit {
		return err
	}
	for pass := 0; size > limit && pass < 8; pass++ {
		candidates := s.trimCandidates()
		if len(candidates) == 0 {
			break
		}
		excess := size - limit
		dropObject := map[int]bool{}
		dropNode, dropSecret := map[int]bool{}, map[int]bool{}
		for _, c := range candidates {
			if excess <= 0 {
				break
			}
			c.mark(dropObject, dropNode, dropSecret)
			excess -= c.size + 1
			if s.TrimmedObjects == nil {
				s.TrimmedObjects = map[string]int{}
			}
			s.TrimmedObjects[c.kind]++
		}
		if err := s.applyTrim(dropObject, dropNode, dropSecret); err != nil {
			return err
		}
		if size, err = encodedSize(*s); err != nil {
			return err
		}
	}
	if size > limit {
		return fmt.Errorf("captured snapshot is %d bytes after trimming every trimmable object (%v), over the %d budget; the protected kinds %v alone are too large", size, s.TrimmedObjects, limit, protectedKinds)
	}
	return nil
}

func encodedSize(s Snapshot) (int, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return 0, fmt.Errorf("encode captured snapshot: %w", err)
	}
	return len(data), nil
}

// candidate is one droppable item addressed by its slice and index.
type candidate struct {
	trimCandidate
	source string
	index  int
}

func (c candidate) mark(objects, nodes, secrets map[int]bool) {
	switch c.source {
	case "objects":
		objects[c.index] = true
	case "nodes":
		nodes[c.index] = true
	default:
		secrets[c.index] = true
	}
}

// trimCandidates returns every trimmable item ordered by relevance (least
// relevant kind first, largest first within a kind).
func (s *Snapshot) trimCandidates() []candidate {
	var all []candidate
	add := func(source string, index int, kind string, value any) {
		if slices.Contains(protectedKinds, kind) {
			return
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return
		}
		all = append(all, candidate{trimCandidate: trimCandidate{kind: kind, size: len(raw)}, source: source, index: index})
	}
	for i := range s.Objects {
		add("objects", i, s.Objects[i].GetKind(), s.Objects[i].Object)
	}
	for i := range s.NodeList {
		add("nodes", i, "Node", s.NodeList[i])
	}
	for i := range s.SecretList {
		add("secrets", i, "Secret", s.SecretList[i])
	}
	rank := func(kind string) int {
		if at := slices.Index(trimOrder, kind); at >= 0 {
			return at
		}
		return len(trimOrder) - 2 // unknown kinds go just before Secret metadata and Node
	}
	sort.SliceStable(all, func(i, j int) bool {
		if ri, rj := rank(all[i].kind), rank(all[j].kind); ri != rj {
			return ri < rj
		}
		return all[i].size > all[j].size
	})
	return all
}

// applyTrim removes the marked items and rebuilds the typed lists, which are a
// projection of the namespace-local objects (nodes are cluster-scoped and kept
// separately).
func (s *Snapshot) applyTrim(objects, nodes, secrets map[int]bool) error {
	keptObjects := make([]unstructured.Unstructured, 0, len(s.Objects))
	var local []unstructured.Unstructured
	for i, o := range s.Objects {
		if objects[i] {
			continue
		}
		keptObjects = append(keptObjects, o)
		if o.GetNamespace() == s.NamespaceName {
			local = append(local, o)
		}
	}
	var keptNodes []corev1.Node
	for i, n := range s.NodeList {
		if !nodes[i] {
			keptNodes = append(keptNodes, n)
		}
	}
	var keptSecrets []sdk.SecretMetadata
	for i, m := range s.SecretList {
		if !secrets[i] {
			keptSecrets = append(keptSecrets, m)
		}
	}
	s.Objects = keptObjects
	for _, field := range sdk.TypedObjectFields(s) {
		field.SetZero()
	}
	if err := sdk.DecodeTypedObjects(s, local); err != nil {
		return err
	}
	s.NodeList = append(s.NodeList, keptNodes...)
	s.SecretList = keptSecrets
	return nil
}
