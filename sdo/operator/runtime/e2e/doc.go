//go:build integration && controller_e2e

// Package e2e runs the real controller against a real Kubernetes API server
// (kube-apiserver plus etcd, no nodes) with a fake kubelet and no-model agent
// scripts. It needs the envtest binaries (KUBEBUILDER_ASSETS), bubblewrap and
// the uv environment; see docs/testing-guide.md#controller-end-to-end.
package e2e
