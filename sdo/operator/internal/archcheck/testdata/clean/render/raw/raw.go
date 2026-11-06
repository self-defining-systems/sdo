package raw

import "fx.test/op/contracts/gen"

func Is(m any) bool { _, ok := m.(*gen.RawManifestStrategy); return ok }
