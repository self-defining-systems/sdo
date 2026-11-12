package rendered_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

var secretRef = sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "creds"}

func literalSecretTree(body string) rendertest.MapFS {
	return rendertest.MapFS{"k8s/creds.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: creds}\n" + body}
}

// The accepted Secret data is the deployer's SecretsInclude render of the
// accepted tree, from literal data, read only by the restore path.
func TestAcceptedSecretDataReadsTheLiteralDataOfTheAcceptedRender(t *testing.T) {
	source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(literalSecretTree("stringData: {key-a: synthetic-one}\ndata: {key-b: c3ludGhldGljLXR3bw==}\n"))}
	got, err := source.AcceptedSecretData(context.Background(), syntheticCommit, "app", secretRef)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got["key-a"]) != "synthetic-one" || string(got["key-b"]) != "synthetic-two" {
		t.Fatalf("data keys = %v", keysOf(got))
	}
}

func keysOf(m map[string][]byte) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A Secret with no literal data in source, or one absent from it, is refused
// with a typed reason; the error never carries a value.
func TestAcceptedSecretDataRefusesWithoutLiteralSource(t *testing.T) {
	source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(literalSecretTree("type: Opaque\n"))}
	if _, err := source.AcceptedSecretData(context.Background(), syntheticCommit, "app", secretRef); !errors.Is(err, rendered.ErrSecretSourceNoLiteral) {
		t.Fatalf("no literal data: %v", err)
	}
	other := sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: "absent"}
	if _, err := source.AcceptedSecretData(context.Background(), syntheticCommit, "app", other); !errors.Is(err, rendered.ErrAcceptedManifestAbsent) {
		t.Fatalf("absent: %v", err)
	}
}

// A render that yields different data each time (a generated password) is not
// the accepted value: re-rendering would write a new one. It is refused.
func TestAcceptedSecretDataRefusesANondeterministicRender(t *testing.T) {
	calls := 0
	c := render.NewCatalog()
	if err := render.Register[*contracts.RawRenderConfig](c, "counter", render.RendererFunc[*contracts.RawRenderConfig](func(_ context.Context, _ render.FS, env render.Env, _ *contracts.RawRenderConfig) (render.Result, error) {
		calls++
		secret := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "creds", "namespace": "app"}}}
		if env.Secrets == render.SecretsInclude {
			secret.Object["stringData"] = map[string]any{"key": fmt.Sprintf("generated-%d", calls)}
		}
		return render.SinglePhase(secret), nil
	})); err != nil {
		t.Fatal(err)
	}
	config, err := anypb.New(&contracts.RawRenderConfig{})
	if err != nil {
		t.Fatal(err)
	}
	source := rendered.Source{Repository: "/synthetic", Deployment: &contracts.DeploymentSource{RendererId: "counter", Config: config}, Catalog: c, Git: treeGit(literalSecretTree(""))}
	if _, err := source.AcceptedSecretData(context.Background(), syntheticCommit, "app", secretRef); !errors.Is(err, rendered.ErrSecretSourceNondeterministic) {
		t.Fatalf("nondeterministic render: %v", err)
	}
}
