package bench

import "sync"

// memoMap answers each key once for any number of concurrent askers: the first runs the probe and the others wait for
// its answer. A probe that fails is not remembered, so the next ask tries again (its waiters get the same error).
type memoMap[V any] struct {
	mu sync.Mutex
	m  map[string]*memoEntry[V]
}

type memoEntry[V any] struct {
	done chan struct{}
	v    V
	err  error
}

func newMemoMap[V any]() *memoMap[V] { return &memoMap[V]{m: map[string]*memoEntry[V]{}} }

func (m *memoMap[V]) do(key string, f func() (V, error)) (V, error) {
	m.mu.Lock()
	e, ok := m.m[key]
	if ok {
		m.mu.Unlock()
		<-e.done
		return e.v, e.err
	}
	e = &memoEntry[V]{done: make(chan struct{})}
	m.m[key] = e
	m.mu.Unlock()
	e.v, e.err = f()
	if e.err != nil {
		m.mu.Lock()
		delete(m.m, key)
		m.mu.Unlock()
	}
	close(e.done)
	return e.v, e.err
}
