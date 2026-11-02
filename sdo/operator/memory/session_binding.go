package memory

import "sdo.dev/operator/sessionstorage"

// SessionStorageUse protects an in-flight capture while a standby binds its
// pre-mounted directory inodes to canonical incident paths. Mutation leases and
// observations share this pin, so binding cannot replace their locked inodes.
func SessionStorageUse() func() { return sessionstorage.Use() }

func TrySessionStorageBinding() (func(), bool) { return sessionstorage.TryBind() }
