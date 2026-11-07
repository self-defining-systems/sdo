// Package integration holds the integration tests that need both the
// sdo-operator root module and the runtime module. It is a test-only module:
// no other module requires it, so the root and runtime go.mod files stay
// acyclic (runtime requires the root; the root must not require runtime).
package integration
