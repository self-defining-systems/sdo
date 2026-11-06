//archcheck:ratchet R5 count=1 until=S3-10d
package rendered

import (
	"context"
	"encoding/json"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type manifestCacheKey struct {
	repository, paths, namespace string
	qualified                    bool
}

// Keep only the latest requested revision, with a bounded set of namespace and
// selection projections. Accepted Git objects are immutable; live observations,
// API defaulting and admission checks deliberately stay outside this cache.
type manifestCache struct {
	mu       sync.Mutex
	revision string
	entries  map[manifestCacheKey][]*unstructured.Unstructured
}

func (c *manifestCache) read(ctx context.Context, source Source, revision, namespace string, qualified bool) ([]*unstructured.Unstructured, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	paths, _ := json.Marshal(source.ManifestPaths)
	key := manifestCacheKey{source.Repository, string(paths), namespace, qualified}
	if c.revision != revision {
		c.revision = revision
		c.entries = nil
	}
	if objects, ok := c.entries[key]; ok {
		return objects, nil
	}
	objects, err := source.loadManifests(ctx, revision, namespace, qualified)
	if err != nil {
		return nil, err
	}
	if c.entries == nil || len(c.entries) >= 16 {
		c.entries = make(map[manifestCacheKey][]*unstructured.Unstructured)
	}
	c.entries[key] = objects
	return objects, nil
}

func copyManifests(objects []*unstructured.Unstructured) []*unstructured.Unstructured {
	if objects == nil {
		return nil
	}
	result := make([]*unstructured.Unstructured, len(objects))
	for i, object := range objects {
		result[i] = object.DeepCopy()
	}
	return result
}
