SDO confirmed users recovered. These app files are not captured by a patch, so they are not kept:
app/main.go
app/debug.txt
Keep a change with `sdo-operator patch create --reason "<why>" <paths>`, or remove it (`git checkout -- <path>`, or delete the file). Cleanup turn 1.
Answer in the `reflection` field and leave `incident_result` null.
Put in note one line: what you kept or removed.
