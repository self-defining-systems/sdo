package rendered

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// manifestCacheKey identifies one render of the cached revision. The revision
// fixes its tree (and so the effective tree), so revision + deterministic
// DeploymentSource + Env digest is the design's (EffectiveTree, Deployment,
// Env) key without a Git process per read.
type manifestCacheKey struct {
	repository, deployment, env string
}

// Keep only the latest requested revision, with a bounded set of namespace and
// source projections. Accepted Git objects are immutable; live observations,
// API defaulting and admission checks deliberately stay outside this cache.
type manifestCache struct {
	mu       sync.Mutex
	revision string
	entries  map[manifestCacheKey][]DesiredObject
}

func (c *manifestCache) read(ctx context.Context, source Source, revision, namespace string) ([]DesiredObject, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	deployment, err := proto.MarshalOptions{Deterministic: true}.Marshal(source.Deployment)
	if err != nil {
		return nil, err
	}
	env, err := envDigest(source.env(namespace))
	if err != nil {
		return nil, err
	}
	key := manifestCacheKey{source.Repository, string(deployment), env}
	if c.revision != revision {
		c.revision = revision
		c.entries = nil
	}
	if objects, ok := c.entries[key]; ok {
		return objects, nil
	}
	objects, err := source.loadManifests(ctx, revision, namespace)
	if err != nil {
		return nil, err
	}
	if c.entries == nil || len(c.entries) >= 16 {
		c.entries = make(map[manifestCacheKey][]DesiredObject)
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
