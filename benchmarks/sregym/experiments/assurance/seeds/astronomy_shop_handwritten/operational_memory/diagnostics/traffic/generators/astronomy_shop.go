package generators

import (
	"context"
	"math/rand/v2"

	"sdo.dev/operator/sdk/traffic"
)

// The frontend-proxy Envoy (Service frontend-proxy, port 8080) routes /images/
// to image-provider and every other path to the Next.js frontend, whose
// pages/api routes call the gRPC and HTTP backends. All scenarios are reads.

// products are the catalog IDs the postgresql-init script seeds and the
// upstream Locust load generator browses; each has five seeded reviews.
var products = []string{
	"0PUK6V6EV0", "1YMWWN1N4O", "2ZYFJ3GM2N", "66VCHSJNUP", "6E92ZMYYFZ",
	"9SIQT8TOJO", "L9ECAV7KIM", "LS4PSXUNUM", "OLJCESPC7Z", "HQTGWGPNH4",
}

// pictures are the seeded catalog pictures image-provider serves.
var pictures = []string{
	"EclipsmartTravelRefractorTelescope.jpg", "LensCleaningKit.jpg", "NationalParkFoundationExplorascope.jpg",
	"OpticalTubeAssembly.jpg", "RedFlashlight.jpg", "RoofBinoculars.jpg", "SolarFilter.jpg",
	"SolarSystemColorImager.jpg", "StarsenseExplorer.jpg", "TheCometBook.jpg",
}

// shippingAddress is the first checkout person of the Locust load generator,
// in the JSON shape the frontend's own API client sends.
const shippingAddress = `{"streetAddress":"1600 Amphitheatre Parkway","city":"Mountain View","state":"CA","country":"United States","zipCode":"94043"}`

// syntheticSession is a cart session no real shopper uses; reading its empty
// cart never touches shopper data.
const syntheticSession = "sdo-synthetic-probe"

// pick stores one of values, chosen from the iteration's random source, in
// State under key before building the wrapped endpoint, so a path segment
// "{key}" or a FromPrevious(key) parameter can use it.
type pick struct {
	key    string
	values []string
	next   traffic.Endpoint
}

func (p pick) Build(ctx context.Context, rng *rand.Rand, state traffic.State) (traffic.Request, error) {
	state[p.key] = p.values[rng.IntN(len(p.values))]
	return p.next.Build(ctx, rng, state)
}

func (p pick) Check(response traffic.Response, state traffic.State) traffic.Verdict {
	return p.next.Check(response, state)
}

func target() traffic.Target { return traffic.Target{Service: "frontend-proxy", Port: 8080} }

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		{
			ID:          "storefront-home",
			Description: "Render the storefront home page through Envoy and the Next.js frontend.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend"},
			SideEffect:  traffic.SideEffectRead,
			Steps:       []traffic.Step{{Name: "home", Endpoint: traffic.GET("/", nil).Contains("Otel Demo - Home")}},
			Detects:     []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "product-browse",
			Description: "Read a seeded product with its USD price from product-catalog.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "product-catalog", "postgresql"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "product", Endpoint: pick{key: "product", values: products,
				next: traffic.GET("/api/products/{product}", nil).Contains(`"priceUsd"`)}}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "product-recommendations",
			Description: "List recommendations for a seeded product; recommendation lists the catalog and the frontend resolves each product.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "recommendation", "product-catalog", "postgresql"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "recommend", Endpoint: pick{key: "product", values: products,
				next: traffic.GET("/api/recommendations", traffic.Params{"productIds": traffic.FromPrevious("product")}).Contains(`"priceUsd"`)}}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "product-reviews",
			Description: "Read the seeded reviews of a product from product-reviews.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "product-reviews", "postgresql"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "reviews", Endpoint: pick{key: "product", values: products,
				next: traffic.GET("/api/product-reviews/{product}", nil).Contains(`"username"`)}}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "ads",
			Description: "Fetch targeted ads for a catalog category from ad.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "ad"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "ads", Endpoint: traffic.GET("/api/data", traffic.Params{
				"contextKeys": traffic.OneOf("binoculars", "telescopes", "accessories", "assembly", "travel", "books"),
			}).Contains(`"redirectUrl"`)}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "cart-view",
			Description: "Read the (empty) cart of a dedicated synthetic session from cart and its Valkey store.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "cart", "valkey-cart"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "cart", Endpoint: traffic.GET("/api/cart", traffic.Params{
				"sessionId": traffic.Const(syntheticSession),
			}).Contains(`"items"`)}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "currency-conversion",
			Description: "List supported currencies and read a product priced in EUR through currency.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "currency", "product-catalog", "postgresql"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{
				{Name: "currencies", Endpoint: traffic.GET("/api/currency", nil).Contains(`"EUR"`)},
				{Name: "convert", Endpoint: pick{key: "product", values: products,
					next: traffic.GET("/api/products/{product}", traffic.Params{"currencyCode": traffic.Const("EUR")}).Contains(`"currencyCode":"EUR"`)}},
			},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "shipping-quote",
			Description: "Quote shipping for a one-item cart; shipping asks quote and the frontend converts the cost through currency.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "frontend", "shipping", "quote", "currency"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "quote", Endpoint: traffic.GET("/api/shipping", traffic.Params{
				"itemList":     traffic.Const(`[{"productId":"66VCHSJNUP","quantity":1}]`),
				"currencyCode": traffic.Const("USD"),
				"address":      traffic.Const(shippingAddress),
			}).Contains(`"currencyCode":"USD"`)}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID:          "product-image",
			Description: "Fetch a seeded product picture that Envoy routes to image-provider.",
			Target:      target(),
			DependsOn:   []string{"frontend-proxy", "image-provider"},
			SideEffect:  traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "picture", Endpoint: pick{key: "picture", values: pictures,
				next: traffic.GET("/images/products/{picture}", nil)}}},
		},
	}
}
