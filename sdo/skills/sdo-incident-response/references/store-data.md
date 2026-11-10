# Store data: backup, cache invalidation, repair, restore

Use these typed actions (menu or plan) for records inside a data store. Each needs a `StoreTarget`: the application-map component, its `data.engine` (redis, mongodb, postgres), and the store Pod and container. SDO refuses a target the accepted map does not ground. A `DataScope` selects records: redis database number and key glob; mongodb database, collection and JSON filter; postgres database, table and WHERE body. `Limit` bounds the items (default 10000).

Work in this order:

1. **Fix the authoritative source first.** A cache refills from its source, so flushing it over damaged source records brings the damage back. Find and repair the damage in the store the map marks `role: authoritative` before you touch a `derived` one.
2. **Take a backup before any record write.** `StoreBackup{ID, Target, Scope}` copies the scope to the controller's backup storage and records the item count, size, SHA-256 and store facts. It changes nothing. Back up the whole collection or table, or exactly the scope you will repair.
3. **Repair with a count.** First count the scope with a read and decide how many items the repair should touch. Then run `RepairData{Target, Scope, Statement, ExpectMin, ExpectMax, Backup}`. SDO checks that the backup covers the scope, counts again and refuses outside the bounds. Then it writes and counts again, and the receipt reports before, affected and after. The statement is one redis single-key write applied to each matched key (`SREM <member>`, `HDEL <field>`, `DEL`), a mongodb update document or pipeline (`{"$rename": {"addr": "address"}}`), a postgres `SET ...` clause, or `delete`.
4. **Invalidate the copy last.** `InvalidateDerived{Target, Scope}` drops redis cache entries only when the map marks the component `derived` and every `derived_from` source passes its live store-state checks. A refusal naming a source means: go back to step 1.
5. **Restore only deliberately.** `RestoreBackup{Target, Backup}` puts a backup's items back: redis keys with REPLACE, mongodb documents upserted by `_id`, postgres rows by primary key. It never deletes items created since the backup. Take a fresh `StoreBackup` of the same scope first; without one, the restore is an unbacked write.

**Name lost writes.** A restore discards every write the application made to the restored items after the backup. A repair can conflict with writes that land between its count and its write. In your report, give the backup time, the restore or repair time, and the keys, documents or rows that may have changed in between.

A `RepairData` with a covering backup of this incident does not by itself make the outcome unconfirmed. An opaque `kubectl exec`, script, Job or HTTP write to a `holds_data` store still does. So does a restore with no fresh backup, or a repair whose backup is missing or narrower than its scope. The automatic stage never repairs or restores records.
