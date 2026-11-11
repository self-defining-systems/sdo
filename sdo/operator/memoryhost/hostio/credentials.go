package hostio

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CredentialTTL bounds how long the prober reuses a Secret value it read, so
// a rotated credential reaches the probes within it.
const CredentialTTL = 30 * time.Second

// SecretCredentials is the prober's traffic.CredentialSource: it reads one key
// of a Secret in the application namespace, the way the kubelet resolves a
// Pod's secretKeyRef. Only the memory host's prober holds the value, in
// memory; it is never logged, persisted or returned over the host socket.
type SecretCredentials struct {
	Client    kubernetes.Interface
	Namespace string
	// Now is the cache clock; time.Now when nil.
	Now func() time.Time

	mu    sync.Mutex
	cache map[string]cachedCredential
}

type cachedCredential struct {
	value string
	at    time.Time
}

// Value implements traffic.CredentialSource.
func (s *SecretCredentials) Value(ctx context.Context, secret, key string) (string, error) {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	id := secret + "/" + key
	s.mu.Lock()
	cached, ok := s.cache[id]
	s.mu.Unlock()
	if ok && now().Sub(cached.at) < CredentialTTL {
		return cached.value, nil
	}
	object, err := s.Client.CoreV1().Secrets(s.Namespace).Get(ctx, secret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read Secret %s: %w", secret, err)
	}
	data, ok := object.Data[key]
	if !ok {
		return "", fmt.Errorf("Secret %s has no key %s", secret, key)
	}
	s.mu.Lock()
	if s.cache == nil {
		s.cache = map[string]cachedCredential{}
	}
	s.cache[id] = cachedCredential{value: string(data), at: now()}
	s.mu.Unlock()
	return string(data), nil
}
