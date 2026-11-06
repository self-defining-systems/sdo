package deployment

import c "fx.test/op/contracts/gen"

func f(m any) bool {
	_, ok := m.(*c.RawManifestStrategy)
	return ok
}

func g(m any) int {
	switch m.(type) {
	case c.RawManifestStrategy:
		return 1
	}
	return 0
}
