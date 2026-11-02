# Closure recovery observations

Recovery publication now samples every health detector on the current snapshot, reusing a scheduled evaluation only from that same controller step. Broker closure uses the same detector reader and continuous 15-second stability proof. An active traffic SLO after a repair is recovery still in progress, not a permanent broker failure: the controller retains the incident's verification deadline, waits for fresh stable health, and returns unrecovered automatic-stage receipts and residuals to its first responder at the bound. Restart renews the observation window without extending that deadline. A red-to-clear boundary recheck refreshes final detector states and verification time before freezing; reflection latency is never part of the health rule. Historical runs may show `closure permanently failed after 1 broker attempts: closure observation detector traffic-health is active`; that was the retired treatment of a health observation as permanent broker rejection.

Frozen broker commands remain immutable across retries. If health cannot regain its shared proof before the original verification deadline, the native broker derives a failed (or observation-unconfirmed) outcome from the fresh controller observation while retaining the exact original `ledger.closure`. Historical clear states and the new observation remain partial outcome evidence; no final recovery is claimed and no accepted outcome is rewritten. This observation also cannot publish an unaccepted source repair. Newly activated post-proof health episodes stay queued through acknowledgment for the following responder, including recurrence of an original fingerprint. This differs from the pre-freeze automatic-stage handoff, which retains the incident and its checked stage receipts.


An incomplete, unavailable, recovering or not-yet-stable closure recheck owns a
one-second observation wake in both active and maintenance modes. This is not a
broker rejection and does not consume its retry budget. Broker rejection backoff
remains authoritative, and settled closures stop scheduling observation wakes.
Without this wake, a paused controller could fall back to an hour-long sleep
while a restarted recovery observer needed a fresh stability window.
