"""Evidence budgets shared by live recording and offline checkpoint consumers."""

# Match worker_registry.MaxRequestBytes: the native dispatch envelope. test_native_request_budget_parity
# compiles the Go constant to catch drift without changing production transport ownership.
MAX_TASK_BYTES = 512 << 10
MAX_RESULT_BYTES = 1 << 20
MAX_BROKER_BYTES = 1 << 20
