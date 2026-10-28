# Webhook trust

`SyncWebhookCA` synchronizes one named webhook caBundle from a named Secret key without exposing bytes. Select it through the mitigation menu using observed references; receipts record only the Secret reference and digest. Learned programs diagnose with `WebhookCAMatchesSecret`, which returns a boolean. Secret endpoint reads remain unavailable to programs.

Ordinary actions after synchronization also publish digest projections of CA bundles in receipts and captures, including copies in last-applied annotations.

Find the webhook, its backend Service and the Secret its Pods mount from the failing namespace; read names, never Secret data. Learned programs bind that Namespace or workload and read `AdmissionWebhooksFor`.
