"""Evidence budgets shared by live recording and offline checkpoint consumers."""

# Match worker_registry.MaxStagedRequestBytes: staged native reflection tasks can
# carry full logical memory. test_native_request_budget_parity compiles the Go
# constant to catch drift without changing production transport ownership.
MAX_TASK_BYTES = 64 << 20
MAX_RESULT_BYTES = 1 << 20
MAX_BROKER_BYTES = 1 << 20
