package helm

import (
	"fmt"

	"fx.test/op/contracts/gen"
	"fx.test/op/render"
	"fx.test/op/render/rendertest"
	"helm.sh/helm/v3/pkg/chart"
)

var _ render.Renderer
var _ = rendertest.X
var _ = fmt.Sprint
var _ = chart.X
var _ = gen.X
