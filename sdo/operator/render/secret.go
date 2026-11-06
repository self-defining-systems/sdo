package render

// SecretIdentity is the one projection of a Secret under SecretsOmit and in
// render digests: apiVersion, kind and metadata.name, plus metadata.namespace
// when the object sets one. Everything else is dropped, including type,
// labels, annotations (last-applied annotations can carry credential bytes)
// and extension fields. Dropping type is deliberate: it is not part of a
// Secret's identity, and renderers disagree on whether they know it (a
// kustomize generator's type is only known when the Secret is generated).
//
// Every renderer calls it for Secrets in SecretsOmit and the catalog rejects
// any other shape, so renderers cannot disagree; readers and the render
// digest call it so Omit and Include renders of one tree agree.
func SecretIdentity(object map[string]any) map[string]any {
	metadata, _ := object["metadata"].(map[string]any)
	identity := map[string]any{"name": metadata["name"]}
	if namespace, ok := metadata["namespace"].(string); ok && namespace != "" {
		identity["namespace"] = namespace
	}
	return map[string]any{"apiVersion": object["apiVersion"], "kind": "Secret", "metadata": identity}
}
