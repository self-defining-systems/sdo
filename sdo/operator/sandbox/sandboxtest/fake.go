// Package sandboxtest is the in-memory sandbox.Provider the manager's rules
// are tested with.
package sandboxtest

import (
	"context"
	"os"
	"sort"
	"strings"
	"sync"

	"sdo.dev/operator/sandbox"
)

// Provider creates no clusters: it records names and writes a placeholder
// kubeconfig.
type Provider struct {
	mu      sync.Mutex
	Cluster map[string]sandbox.Spec
	// FailCreate, when set, makes Create fail after recording a half-made cluster.
	FailCreate error
	Deleted    []string
}

// Create implements sandbox.Provider.
func (p *Provider) Create(_ context.Context, spec sandbox.Spec) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Cluster == nil {
		p.Cluster = map[string]sandbox.Spec{}
	}
	p.Cluster[spec.Name] = spec
	if p.FailCreate != nil {
		return p.FailCreate
	}
	return os.WriteFile(spec.Kubeconfig, []byte("fake"), 0o600)
}

// Delete implements sandbox.Provider.
func (p *Provider) Delete(_ context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.Cluster[name]; ok {
		p.Deleted = append(p.Deleted, name)
	}
	delete(p.Cluster, name)
	return nil
}

// Names implements sandbox.Provider.
func (p *Provider) Names(context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for n := range p.Cluster {
		if strings.HasPrefix(n, sandbox.ClusterPrefix) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
}

var _ sandbox.Provider = (*Provider)(nil)
