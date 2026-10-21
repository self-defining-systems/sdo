# Missing required ConfigMap

Use this playbook when `<TARGET_RESOURCE>` references `<MISSING_CONFIG_MAP>` as required and the ConfigMap is absent.

## Decide

Confirm that the live `<TARGET_RESOURCE>` still contains a non-optional reference to `<MISSING_CONFIG_MAP>` and that no ConfigMap with that name exists in the same namespace.

## Repair

Restore the intended ConfigMap from the source-controlled deployment configuration, or correct the source reference when it points to the wrong role. Redeploy from source rather than creating an undocumented live-only object.

## Verify

Verify that `<MISSING_CONFIG_MAP>` now exists, the reference remains correct, replacement Pods become ready, and the application health objective passes.
