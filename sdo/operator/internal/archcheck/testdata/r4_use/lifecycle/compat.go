package lifecycle

import (
	"google.golang.org/protobuf/types/known/anypb"

	c "fx.test/op/contracts/gen"
)

type holder struct{ cfg *c.RawManifestStrategy }

func build() any { return &c.RawManifestStrategy{} }

func decode(a *anypb.Any) error {
	var cfg c.RawManifestStrategy
	return a.UnmarshalTo(&cfg)
}

func register[T any]() {}

var _ = register[*c.RawManifestStrategy]
