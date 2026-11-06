//archcheck:ratchet R5 count=2 until=S3-10d
package rendered

import (
	"context"
	"errors"
	"testing"
)

func TestSourceErrorKinds(t *testing.T) {
	commit := "0123456789abcdef0123456789abcdef01234567"
	failing := func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("git down") }
	_, err := Source{Repository: "r", ManifestPaths: []string{"k8s"}, Git: failing}.AcceptedManifests(context.Background(), commit, "app")
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("git failure = %v, want unavailable", err)
	}
	listing := func(context.Context, string, ...string) ([]byte, error) {
		return []byte("100644 blob x\tother.yaml\x00"), nil
	}
	_, err = Source{Repository: "r", ManifestPaths: []string{"k8s"}, Git: listing}.AcceptedManifests(context.Background(), commit, "app")
	if !errors.Is(err, ErrSourceInvalid) {
		t.Fatalf("no manifests = %v, want invalid", err)
	}
	_, err = Source{Repository: "r", Git: listing}.AcceptedManifests(context.Background(), commit, "app")
	if !errors.Is(err, ErrSourceUnconfigured) {
		t.Fatalf("nil selection = %v, want unconfigured", err)
	}
}
