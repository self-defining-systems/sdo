//archcheck:ratchet R5 count=2 until=S3-10d
package rendered

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
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

// A read failure inside a kustomize render keeps its cause: a transient Git
// failure on a referenced resource stays Unavailable (retried), not Invalid.
func TestKustomizeReadFailureKeepsItsKind(t *testing.T) {
	files := map[string]string{
		"app/kustomization.yaml": "resources: [svc.yaml]\nconfigMapGenerator:\n- {name: c, files: [conf/init.sh]}\n",
		"app/svc.yaml":           "apiVersion: v1\nkind: Service\nmetadata: {name: web}\n",
		"app/conf/init.sh":       "echo hi\n",
	}
	for _, failing := range []string{"app/svc.yaml", "app/conf/init.sh"} {
		t.Run(failing, func(t *testing.T) {
			read := func(_ context.Context, name string) ([]byte, error) {
				if name == failing {
					return nil, sourceErrorf(SourceUnavailable, "accepted input %s unavailable: %w", name, errors.New("git down"))
				}
				body, ok := files[name]
				if !ok {
					return nil, fmt.Errorf("accepted input %s is not a tracked regular file", name)
				}
				return []byte(body), nil
			}
			_, err := renderKustomization(context.Background(), "app/kustomization.yaml", read)
			if !errors.Is(classifySource(err), ErrSourceUnavailable) {
				t.Fatalf("transient read failure = %v, want unavailable", err)
			}
		})
	}
}

func TestClassifySourceByCause(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want error
	}{
		"read failure":  {&fs.PathError{Op: "read", Path: "a", Err: errors.New("i/o error")}, ErrSourceUnavailable},
		"wrapped read":  {fmt.Errorf("render: %w", &fs.PathError{Op: "read", Path: "a", Err: errors.New("i/o error")}), ErrSourceUnavailable},
		"not exist":     {&fs.PathError{Op: "read", Path: "a", Err: fs.ErrNotExist}, ErrSourceInvalid},
		"cancelled":     {context.Canceled, ErrSourceUnavailable},
		"uncategorized": {errors.New("bad yaml"), ErrSourceInvalid},
	} {
		if got := classifySource(tc.err); !errors.Is(got, tc.want) {
			t.Errorf("%s: %v, want %v", name, got, tc.want)
		}
	}
}
