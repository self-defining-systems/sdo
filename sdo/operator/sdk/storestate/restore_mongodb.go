package storestate

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// mongoOneScript runs one JavaScript program, passed whole as $1, with the
// shell the image carries, authenticated like the read.
const mongoOneScript = `set -- --quiet --port %d --eval "$1"
[ -n "$P" ]&&set -- "$@" -u "${U:-root}" -p "$P" --authenticationDatabase admin
m=mongosh;command -v mongo >/dev/null 2>&1&&m=mongo
$m "$@" 2>&1
`

func mongoCommand(e Endpoint, js string) []string {
	return readCommand([]string{"MONGO", "MONGODB"}, fmt.Sprintf(mongoOneScript, e.Port), js)
}

// mongoPayloadRead prints, per collection with a validator and per index
// other than _id_, a tab-separated line with the item's fact value (the same
// VV and IV the read uses) and its full text in the shell's own one-line
// format. It reads catalog metadata only.
const mongoPayloadRead = mongoValues + `var A=db.getSiblingDB("admin"),T="\t",F=typeof EJSON!="undefined"?"ejson":"shell";
function c(r){if(!r.ok)throw Error(r.errmsg);return r}
function S(x){return F=="ejson"?EJSON.stringify(x,{relaxed:false}):tojsononeline(x)}
print("== payload "+F);c(A.runCommand({listDatabases:1,nameOnly:true})).databases.forEach(function(b){var n=b.name;if(n=="local"||n=="config")return;var d=db.getSiblingDB(n);
d.getCollectionInfos({type:"collection"}).forEach(function(i){if(i.name.indexOf("system.")==0)return;var y=VV(i);if(y!="-")print("validator"+T+n+T+i.name+T+y+T+S(i.options.validator));
c(d.runCommand({listIndexes:i.name})).cursor.firstBatch.forEach(function(x){if(x.name=="_id_")return;var z={};for(var f in x)if(f!="v"&&f!="ns")z[f]=x[f];print("index"+T+n+T+i.name+T+x.name+T+IV(x)+T+S(z))})})})
print("== end")
`

// PayloadCommand implements PayloadReader: validator documents and index
// specifications, which the value-free read keeps only as digests.
func (mongoAdapter) PayloadCommand(e Endpoint) []string { return mongoCommand(e, mongoPayloadRead) }

// ParsePayload implements PayloadReader. A read without its end marker is
// incomplete and an error; lines the shell adds (warnings) are skipped.
func (mongoAdapter) ParsePayload(stdout string) (Payload, error) {
	format := ""
	ended := false
	var entries []PayloadEntry
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "== payload "):
			format = strings.TrimPrefix(line, "== payload ")
			continue
		case line == "== end":
			ended = true
			continue
		case format == "" || line == "":
			continue
		}
		f := strings.Split(line, "\t")
		switch {
		case f[0] == "validator" && len(f) == 5:
			entries = append(entries, PayloadEntry{Key: Key{Kind: KindValidator, Scope: f[1], Name: f[2]}, Value: f[3], Format: format, Text: f[4]})
		case f[0] == "index" && len(f) == 6:
			entries = append(entries, PayloadEntry{Key: Key{Kind: KindIndex, Scope: f[1] + "." + f[2], Name: f[3]}, Value: f[4], Format: format, Text: f[5]})
		case f[0] == "validator" || f[0] == "index":
			return Payload{}, fmt.Errorf("mongodb payload line is malformed: %q", firstLine(line))
		}
	}
	if format == "" || !ended {
		return Payload{}, fmt.Errorf("mongodb payload read is incomplete: %q", firstLine(stdout))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key.Less(entries[j].Key) })
	p := Payload{Entries: entries}
	return p, p.Validate()
}

// mongoRestoreHead defines c (fail on a refused command) and A (admin).
const mongoRestoreHead = `var A=db.getSiblingDB("admin");function c(r){if(!r.ok)throw Error(r.codeName+" "+r.errmsg);return r}
`

func jsString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// jsPayload is the expression that turns a payload's text back into a
// document in the shell it was captured with.
func jsPayload(entry PayloadEntry) string {
	switch entry.Format {
	case PayloadEJSON:
		return "EJSON.parse(" + jsString(entry.Text) + ")"
	case PayloadPlaceholder:
		return "{}"
	}
	return "eval(\"(\"+" + jsString(entry.Text) + "+\")\")"
}

// jsScalar is a setting value as a JavaScript literal: a boolean, a number or
// a string.
func jsScalar(v string) string {
	if v == "true" || v == "false" {
		return v
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return v
	}
	return jsString(v)
}

// mongoParams are the runtime parameters the read records; others of its
// settings are startup options.
var mongoParams = map[string]bool{"notablescan": true, "cursorTimeoutMillis": true, "transactionLifetimeLimitSeconds": true, "maxTransactionLockRequestTimeoutMillis": true}

// Restore implements Restorer: fsyncUnlock until no lock is left, a
// runtime parameter or profiling level back to its baseline, a validator
// cleared or put back from the payload with its level and action, an index
// dropped or rebuilt from the payload, and a user's baseline roles (its
// password untouched).
func (mongoAdapter) Restore(e Endpoint, c Change, _ Facts, options RestoreOptions) ([][]string, error) {
	js, err := mongoRestoreJS(c, options.Payload)
	if err != nil {
		return nil, err
	}
	return [][]string{mongoCommand(e, mongoRestoreHead+js+";print("+jsString(RestoredMarker)+")")}, nil
}

func mongoRestoreJS(c Change, payload PayloadLookup) (string, error) {
	key := c.Key()
	before := c.Before
	switch {
	case key.Kind == KindLock && key.Name == "fsync", key.Kind == KindWriteBlock && key.Name == "fsync-lock":
		return `var n=0;while(A.currentOp().fsyncLock&&n<100){A.fsyncUnlock();n++}if(A.currentOp().fsyncLock)throw Error("fsync lock still held after "+n+" unlocks")`, nil
	case key.Kind == KindSetting && key.Scope == "" && mongoParams[key.Name]:
		if before == nil {
			return "", notRestorable(key, "the baseline does not show it")
		}
		return fmt.Sprintf("var p={setParameter:1};p[%s]=%s;c(A.runCommand(p))", jsString(key.Name), jsScalar(before.Value)), nil
	case key.Kind == KindSetting && key.Scope != "" && key.Name == "profile":
		if before == nil {
			return "", notRestorable(key, "the baseline does not show it")
		}
		level, err := strconv.Atoi(before.Value)
		if err != nil {
			return "", notRestorable(key, "baseline profiling level %q is not a number", before.Value)
		}
		return fmt.Sprintf("c(db.getSiblingDB(%s).runCommand({profile:%d}))", jsString(key.Scope), level), nil
	case key.Kind == KindValidator:
		db := fmt.Sprintf("db.getSiblingDB(%s)", jsString(key.Scope))
		if before == nil {
			return fmt.Sprintf(`c(%s.runCommand({collMod:%s,validator:{},validationLevel:"strict",validationAction:"error"}))`, db, jsString(key.Name)), nil
		}
		parts := strings.Split(before.Value, "/")
		if len(parts) != 3 {
			return "", notRestorable(key, "baseline validator value %q has no level, action and digest", before.Value)
		}
		entry, err := payloadFor(payload, *before)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(`c(%s.runCommand({collMod:%s,validator:%s,validationLevel:%s,validationAction:%s}))`, db, jsString(key.Name), jsPayload(entry), jsString(parts[0]), jsString(parts[1])), nil
	case key.Kind == KindIndex:
		dbName, coll, ok := strings.Cut(key.Scope, ".")
		if !ok {
			return "", notRestorable(key, "index scope %q names no collection", key.Scope)
		}
		db := fmt.Sprintf("db.getSiblingDB(%s)", jsString(dbName))
		drop := fmt.Sprintf(`c(%s.runCommand({dropIndexes:%s,index:%s}))`, db, jsString(coll), jsString(key.Name))
		if before == nil {
			return drop, nil
		}
		entry, err := payloadFor(payload, *before)
		if err != nil {
			return "", err
		}
		create := fmt.Sprintf(`c(%s.runCommand({createIndexes:%s,indexes:[%s]}))`, db, jsString(coll), jsPayload(entry))
		if c.After != nil {
			return drop + ";" + create, nil
		}
		return create, nil
	case key.Kind == KindPrincipal && key.Scope != "":
		db := fmt.Sprintf("db.getSiblingDB(%s)", jsString(key.Scope))
		switch {
		case before == nil:
			return fmt.Sprintf(`c(%s.runCommand({dropUser:%s}))`, db, jsString(key.Name)), nil
		case c.After == nil:
			return "", notRestorable(key, "the user was removed and its password is not kept; recreate it with the password from the Secret the workload uses")
		}
		var roles []string
		for _, r := range strings.Split(before.Value, ",") {
			if r == "" {
				continue
			}
			role, roleDB, ok := strings.Cut(r, "@")
			if !ok {
				return "", notRestorable(key, "baseline role %q names no database", r)
			}
			roles = append(roles, fmt.Sprintf("{role:%s,db:%s}", jsString(role), jsString(roleDB)))
		}
		return fmt.Sprintf(`c(%s.runCommand({updateUser:%s,roles:[%s]}))`, db, jsString(key.Name), strings.Join(roles, ",")), nil
	}
	return "", notRestorable(key, "mongodb %s items of this name have no inverse (a startup option, the replication role or a count)", key.Kind)
}
