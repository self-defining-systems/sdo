package rendertest

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"strings"

	"google.golang.org/protobuf/types/known/wrapperspb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/render"
)

// ToyID is the ID under which RegisterToy registers the toy renderer.
const ToyID = "toy"

// SecretFileKey names the field of a toy Secret that points at the file
// holding its value. The file is read only under SecretsInclude; under
// SecretsOmit it is only declared with Stat, like a kustomize
// secretGenerator source.
const SecretFileKey = "x-secret-file"

// RegisterToy registers the toy renderer. Its config is the directory (a
// wrapperspb.StringValue) holding one JSON Kubernetes object per .json file.
// Files named hook-*.json form a final Ephemeral phase. Objects without a
// namespace take Env.Namespace. A Secret object may carry SecretFileKey.
func RegisterToy(c *render.Catalog) error {
	return render.Register[*wrapperspb.StringValue](c, ToyID, render.RendererFunc[*wrapperspb.StringValue](renderToy))
}

func renderToy(_ context.Context, fsys render.FS, env render.Env, dir *wrapperspb.StringValue) (render.Result, error) {
	entries, err := fsys.ReadDir(dir.GetValue())
	if err != nil {
		return render.Result{}, err
	}
	var main, hooks []*unstructured.Unstructured
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		file := path.Join(dir.GetValue(), entry.Name())
		data, err := fsys.ReadFile(file)
		if err != nil {
			return render.Result{}, err
		}
		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(data); err != nil {
			return render.Result{}, fmt.Errorf("%s: %w", file, err)
		}
		if obj.GetNamespace() == "" && obj.GetKind() != "Namespace" {
			obj.SetNamespace(env.Namespace)
		}
		if err := resolveSecret(fsys, env, obj, file); err != nil {
			return render.Result{}, err
		}
		if strings.HasPrefix(entry.Name(), "hook-") {
			hooks = append(hooks, obj)
		} else {
			main = append(main, obj)
		}
	}
	var res render.Result
	if len(main) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "main", Objects: main, Gate: render.GateReady})
	}
	if len(hooks) > 0 {
		res.Phases = append(res.Phases, render.Phase{Name: "hooks", Objects: hooks, Gate: render.GateNone, Ephemeral: true})
	}
	return res, nil
}

func resolveSecret(fsys render.FS, env render.Env, obj *unstructured.Unstructured, file string) error {
	source, found := obj.Object[SecretFileKey].(string)
	if !found {
		return nil
	}
	delete(obj.Object, SecretFileKey)
	if env.Secrets == render.SecretsOmit {
		// Declare the source without reading it, so it stays an attributable
		// input (its blob ID is recorded) but its bytes never reach the render.
		if _, err := fsys.Stat(source); err != nil {
			return fmt.Errorf("%s: secret source: %w", file, err)
		}
		return nil
	}
	value, err := fsys.ReadFile(source)
	if err != nil {
		return fmt.Errorf("%s: secret source: %w", file, err)
	}
	obj.Object["data"] = map[string]any{"value": base64.StdEncoding.EncodeToString(value)}
	return nil
}
