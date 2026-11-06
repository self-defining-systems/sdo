package lifecycle

import c "fx.test/op/contracts/gen"

func is(m any) bool { _, ok := m.(*c.RawManifestStrategy); return ok }
