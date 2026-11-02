package rendered

// ClusterScopedKind identifies built-in Kubernetes object scopes while source
// is rendered offline. Unknown extension kinds retain their authored namespace;
// installation discovery remains authoritative for grants.
func ClusterScopedKind(kind string) bool {
	switch kind {
	case "Namespace", "Node", "PersistentVolume", "ClusterRole", "ClusterRoleBinding", "CustomResourceDefinition", "APIService", "StorageClass", "VolumeAttachment", "CSIDriver", "CSINode", "RuntimeClass", "PriorityClass", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration", "ValidatingAdmissionPolicy", "ValidatingAdmissionPolicyBinding", "ClusterTrustBundle", "CertificateSigningRequest", "IngressClass", "PodSecurityPolicy", "IPAddress", "ServiceCIDR":
		return true
	}
	return false
}
