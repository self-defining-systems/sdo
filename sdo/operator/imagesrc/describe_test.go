package imagesrc_test

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/imagesrctest"
)

func TestDescribeListsRegisteredSourcesSortedWithDoc(t *testing.T) {
	c := imagesrc.NewCatalog()
	if got := c.Describe(); len(got) != 0 {
		t.Fatalf("empty catalog describes %v", got)
	}
	if got := (*imagesrc.Catalog)(nil).Describe(); got != nil {
		t.Fatalf("nil catalog describes %v", got)
	}
	if err := imagesrc.Register(c, "zeta", imagesrc.Source[*emptypb.Empty](&imagesrctest.Fake{}), imagesrc.Options{}, imagesrc.WithDoc("Builds nothing.")); err != nil {
		t.Fatal(err)
	}
	if err := imagesrc.Register(c, "alpha", imagesrc.Source[*wrapperspb.StringValue](&stringSource{}), imagesrc.Options{}); err != nil {
		t.Fatal(err)
	}
	got := c.Describe()
	if ids := []string{got[0].ID, got[1].ID}; !reflect.DeepEqual(ids, []string{"alpha", "zeta"}) {
		t.Fatalf("order = %v", ids)
	}
	if got[1].Doc != "Builds nothing." || got[1].ConfigType != "google.protobuf.Empty" || got[1].ConfigSchemaJSON == "" {
		t.Fatalf("descriptor = %+v", got[1])
	}
}
