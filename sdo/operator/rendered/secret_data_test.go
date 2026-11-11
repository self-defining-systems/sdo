package rendered_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"sdo.dev/operator/render/rendertest"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// syntheticSecretValue is a value no fixture other than the declared Secret
// contains; every serialized form is searched for it.
const syntheticSecretValue = "synthetic-declared-value-7f3a"

var secretTree = rendertest.MapFS{
	"k8s/web.yaml":   webDeployment,
	"k8s/creds.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: creds}\ntype: Opaque\ndata: {token: c3ludGhldGljLWVuY29kZWQ=}\nstringData: {password: " + syntheticSecretValue + "}\n",
	"k8s/bare.yaml":  "apiVersion: v1\nkind: Secret\nmetadata: {name: bare}\n",
	"kust/kustomization.yaml": "resources: [creds.yaml]\nsecretGenerator:\n- {name: generated, literals: [k=" + syntheticSecretValue + "]}\n- {name: fromfile, files: [absent.key]}\n" +
		"generatorOptions: {disableNameSuffixHash: true}\n",
	"kust/creds.yaml": "apiVersion: v1\nkind: Secret\nmetadata: {name: creds}\nstringData: {password: " + syntheticSecretValue + "}\n",
}

func secretRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: name}
}

// TestAcceptedSecretDataKeepsLiteralDataInMemoryOnly: a Secret the accepted
// manifests declare with literal data resolves to its values for the Go writer,
// while every printable or serialized form names keys only.
func TestAcceptedSecretDataKeepsLiteralDataInMemoryOnly(t *testing.T) {
	source := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(secretTree)}
	data, err := source.AcceptedSecretData(context.Background(), syntheticCommit, "app", secretRef("creds"))
	if err != nil {
		t.Fatal(err)
	}
	if got := data.Keys(); !reflect.DeepEqual(got, []string{"password", "token"}) {
		t.Fatalf("keys = %v", got)
	}
	values := data.Values()
	if string(values["password"]) != syntheticSecretValue || string(values["token"]) != "synthetic-encoded" {
		t.Fatal("declared values were not resolved in memory")
	}
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{string(encoded), fmt.Sprint(data), fmt.Sprintf("%+v", data), fmt.Sprintf("%#v", data), fmt.Sprint(data.Digests())} {
		if strings.Contains(text, syntheticSecretValue) || strings.Contains(text, "synthetic-encoded") {
			t.Fatalf("a printable form exposes a Secret value: %s", text)
		}
	}
	diff := data.Compare(map[string][]byte{"password": []byte("rotated"), "extra": []byte("x")})
	if !reflect.DeepEqual(diff, rendered.SecretKeyDiff{Differs: []string{"password"}, Missing: []string{"token"}, Extra: []string{"extra"}}) || !diff.Changed() {
		t.Fatalf("diff = %+v", diff)
	}
	if same := data.Compare(values); same.Changed() {
		t.Fatalf("identical data compared changed: %+v", same)
	}
	// The ordinary read stays identity-only.
	object, err := source.AcceptedManifest(context.Background(), syntheticCommit, secretRef("creds"))
	if err != nil || len(object.Object) != 3 {
		t.Fatalf("ordinary Secret read is not identity-only: %v %v", object, err)
	}
}

// TestAcceptedSecretDataRefusesSecretsWithoutLiteralData: undeclared Secrets,
// identity-only manifests and generator-made Secrets are refused with a typed
// reason, and generator inputs are never loaded; a literal manifest that a
// kustomization lists as a resource still resolves.
func TestAcceptedSecretDataRefusesSecretsWithoutLiteralData(t *testing.T) {
	ctx := context.Background()
	raw := rendered.Source{Repository: "/synthetic", Deployment: rawSource(t, "k8s"), Catalog: catalog(t), Git: treeGit(secretTree)}
	kust := rendered.Source{Repository: "/synthetic", Deployment: kustomizeSource(t, "kust"), Catalog: catalog(t), Git: treeGit(secretTree)}
	for name, tc := range map[string]struct {
		source rendered.Source
		ref    sdk.ObjectRef
		reason rendered.SecretSourceReason
	}{
		"undeclared":     {source: raw, ref: secretRef("absent"), reason: rendered.SecretNotDeclared},
		"identity only":  {source: raw, ref: secretRef("bare"), reason: rendered.SecretNotLiteral},
		"generator made": {source: kust, ref: secretRef("generated"), reason: rendered.SecretNotLiteral},
		// The generator's input file is absent: a render that read it would fail.
		"generator input": {source: kust, ref: secretRef("fromfile"), reason: rendered.SecretNotLiteral},
		"not a Secret":    {source: raw, ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}, reason: rendered.SecretNotDeclared},
		"other namespace": {source: raw, ref: sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "other", Name: "creds"}, reason: rendered.SecretNotDeclared},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := tc.source.AcceptedSecretData(ctx, syntheticCommit, "app", tc.ref)
			var refusal *rendered.SecretSourceError
			if !errors.As(err, &refusal) || refusal.Reason != tc.reason {
				t.Fatalf("err = %v, want reason %s", err, tc.reason)
			}
			if strings.Contains(err.Error(), syntheticSecretValue) {
				t.Fatalf("refusal exposes a value: %v", err)
			}
		})
	}
	data, err := kust.AcceptedSecretData(ctx, syntheticCommit, "app", secretRef("creds"))
	if err != nil || string(data.Values()["password"]) != syntheticSecretValue {
		t.Fatalf("literal Secret listed by a kustomization did not resolve: %v", err)
	}
}
