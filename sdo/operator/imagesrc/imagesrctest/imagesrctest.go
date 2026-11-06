// Package imagesrctest holds the conformance suite every imagesrc source must
// pass, and a deterministic fake source for catalog and core tests.
package imagesrctest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/emptypb"

	"sdo.dev/operator/imagesrc"
)

// Digest is the deterministic digest the Fake assigns to an image.
func Digest(image string) string {
	sum := sha256.Sum256([]byte(image))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Fake is a deterministic Source[*emptypb.Empty]: it pins an image to
// repository@Digest(image) and records calls. Hook, when set, runs first and
// may fail or block on the context.
type Fake struct {
	Hook func(ctx context.Context, req imagesrc.Request) error

	mu    sync.Mutex
	calls []string
}

// Resolve implements imagesrc.Source.
func (f *Fake) Resolve(ctx context.Context, req imagesrc.Request, _ *emptypb.Empty) (imagesrc.Resolved, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req.Image)
	f.mu.Unlock()
	if f.Hook != nil {
		if err := f.Hook(ctx, req); err != nil {
			return imagesrc.Resolved{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return imagesrc.Resolved{}, err
	}
	return imagesrc.Resolved{Ref: Repository(req.Image) + "@" + Digest(req.Image), Inputs: imagesrc.Inputs{SourceImage: req.Image}}, nil
}

// Calls returns the images resolved so far, in call order.
func (f *Fake) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// Repository strips a digest or tag from an image reference.
func Repository(image string) string {
	if at := strings.IndexByte(image, '@'); at >= 0 {
		return image[:at]
	}
	if colon := strings.LastIndexByte(image, ':'); colon > strings.LastIndexByte(image, '/') {
		return image[:colon]
	}
	return image
}

// Case is one conformance fixture for a source.
type Case struct {
	Name string
	// Image is the reference to resolve.
	Image string
	// Files are the tracked files of the tree the request carries.
	Files map[string]string
	// Config is the source's config; nil means the empty config.
	Config proto.Message
	// WantRef, when set, is the exact Ref expected.
	WantRef string
	// WantInputs, when set, is the exact Inputs expected.
	WantInputs *imagesrc.Inputs
	// WantErr, when set, is a substring the error must contain. Such a case is
	// skipped by the success-only properties.
	WantErr string
}

// TestSource runs the family contract against source id in c, through the
// catalog (so the catalog's own verification applies):
//  1. a successful resolution returns a digest reference whose Inputs name a
//     source image, and equals WantRef and WantInputs when given;
//  2. determinism: two resolutions of equal requests are equal;
//  3. a cancelled context yields an error, never a reference;
//  4. an error case fails with the expected message and no result.
func TestSource(t *testing.T, c *imagesrc.Catalog, id string, cases []Case) {
	t.Helper()
	if len(cases) == 0 {
		t.Fatal("imagesrctest: no cases")
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			declared, base := declare(t, id, tc)
			resolve := func(ctx context.Context) (map[string]imagesrc.Resolved, error) {
				return c.ResolveAll(ctx, []string{tc.Image}, declared, base)
			}
			first, err := resolve(context.Background())
			if tc.WantErr != "" {
				if err == nil {
					t.Fatalf("resolved %v, want error containing %q", first, tc.WantErr)
				}
				if !strings.Contains(err.Error(), tc.WantErr) {
					t.Fatalf("error %q lacks %q", err, tc.WantErr)
				}
				if first != nil {
					t.Fatalf("an error returned a result: %v", first)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			got, ok := first[tc.Image]
			if !ok || len(first) != 1 {
				t.Fatalf("result %v does not map exactly %q", first, tc.Image)
			}
			if got.Inputs.SourceImage == "" {
				t.Errorf("Inputs.SourceImage is empty: %+v", got.Inputs)
			}
			if tc.WantRef != "" && got.Ref != tc.WantRef {
				t.Errorf("Ref = %q, want %q", got.Ref, tc.WantRef)
			}
			if tc.WantInputs != nil && got.Inputs != *tc.WantInputs {
				t.Errorf("Inputs = %+v, want %+v", got.Inputs, *tc.WantInputs)
			}
			second, err := resolve(context.Background())
			if err != nil {
				t.Fatalf("second resolve: %v", err)
			}
			if !reflect.DeepEqual(first, second) {
				t.Errorf("resolution is not deterministic: %v then %v", first, second)
			}
			cancelled, cancel := context.WithCancel(context.Background())
			cancel()
			if res, err := resolve(cancelled); err == nil {
				t.Errorf("a cancelled context resolved %v", res)
			}
		})
	}
}

func declare(t *testing.T, id string, tc Case) ([]imagesrc.Declared, imagesrc.Request) {
	t.Helper()
	d := imagesrc.Declared{Image: tc.Image, SourceID: id}
	if tc.Config != nil {
		packed, err := anypb.New(tc.Config)
		if err != nil {
			t.Fatalf("pack config: %v", err)
		}
		d.Config = packed
	}
	files := fstest.MapFS{}
	for name, body := range tc.Files {
		files[name] = &fstest.MapFile{Data: []byte(body)}
	}
	return []imagesrc.Declared{d}, imagesrc.Request{Tree: "tree-for-" + tc.Name, FS: files}
}
