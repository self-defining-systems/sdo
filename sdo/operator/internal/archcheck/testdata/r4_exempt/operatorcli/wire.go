package operatorcli

import c "fx.test/op/contracts/gen"

func register[T any]() {}

var _ = register[*c.RawManifestStrategy]
