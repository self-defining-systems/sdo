// Package sessionstorage coordinates canonical session directory replacement
// with controller operations that hold files within that directory.
package sessionstorage

import "sync"

var storage sync.RWMutex

// Use pins session storage for an operation's entire lifetime.
func Use() func() { storage.RLock(); return storage.RUnlock }

// TryBind never delays dispatch: a busy session uses the cold worker path.
func TryBind() (func(), bool) {
	if !storage.TryLock() {
		return nil, false
	}
	return storage.Unlock, true
}
