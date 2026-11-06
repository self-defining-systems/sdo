package prebuilt

import (
	"net/http"
	"testing"


	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
)

const synthDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func conformanceCatalog(t *testing.T) *imagesrc.Catalog {
	t.Helper()
	source := &Source{Client: doerFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v2/acme/app/manifests/1" {
			return reply(200, "", http.Header{"Docker-Content-Digest": {synthDigest}})
		}
		return reply(404, "", nil)
	})}
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register[*contractsv1alpha1.PrebuiltImageConfig](c, "prebuilt", source, imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	if err := c.SetDefault("prebuilt"); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPrebuiltConformsToImageSource(t *testing.T) {
	c := conformanceCatalog(t)
	pinned := "r.example/acme/app@" + synthDigest
	imagesrctest.TestSource(t, c, "prebuilt", []imagesrctest.Case{
		{Name: "tag", Image: "r.example/acme/app:1", WantRef: pinned, WantInputs: &imagesrc.Inputs{SourceImage: "r.example/acme/app:1"}},
		{Name: "digest passes through", Image: pinned, WantRef: pinned},
		{Name: "missing tag", Image: "r.example/acme/app:2", WantErr: "no such tag"},
		{Name: "internal host", Image: "10.0.0.1/acme/app:1", WantErr: "not a public registry name"},
	})
	imagesrctest.TestCatalogContract(t, c, "prebuilt")
}

var _ imagesrc.Source[*contractsv1alpha1.PrebuiltImageConfig] = (*Source)(nil)
