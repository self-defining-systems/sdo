package memory

import "sync"

// SessionStorageUse protects an in-flight capture while a standby binds its
// pre-mounted directory inodes to canonical incident paths. Writers never wait
// for standby readiness; a busy capture makes the optional warm claim abstain.
var sessionStorage sync.RWMutex

func SessionStorageUse() func() { sessionStorage.RLock(); return sessionStorage.RUnlock }

func TrySessionStorageBinding() (func(), bool) {
	if !sessionStorage.TryLock() {
		return nil, false
	}
	return sessionStorage.Unlock, true
}
