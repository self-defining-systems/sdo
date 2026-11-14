//go:build race

package sandboxio

// dataHeadroom is address space the race detector maps for its own shadow
// memory, which RLIMIT_DATA counts: without it a race-enabled child dies in
// ThreadSanitizer's allocator long before its heap reaches the limit. The
// heap limit and the caller's peak classification still bound the child.
const dataHeadroom = 1 << 30
