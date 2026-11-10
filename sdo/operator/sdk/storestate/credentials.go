package storestate

import (
	"fmt"
	"strings"
)

// credentialPrelude finds the store's credentials in the container's own
// environment the same way for every engine, then runs the engine script
// passed as its first argument with the remaining arguments. The values stay
// in the container: they are never printed, and SDO never reads a Secret
// itself (the controller refuses Secret value resolution). A Secret reaches the
// read only as the container already sees it, through an env var
// (secretKeyRef, envFrom) or a mounted file named by a "<NAME>_FILE" variable.
//
// P is the password: the first set variable whose name carries one of the
// engine's tags and ends in PASSWORD, PASS or AUTH, root or admin names first,
// then by name. U is the user variable of the same name with USERNAME or USER
// in place of that suffix, so a user and a password from different accounts
// are never mixed; without a password U is the first variable naming the engine and a
// user. Image flags that only look like credentials (MYSQL_RANDOM_ROOT_PASSWORD,
// MYSQL_ALLOW_EMPTY_PASSWORD) are skipped. The engine script supplies its
// default user. %[1]s is the tag ERE.
const credentialPrelude = `L(){ env|sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)=.*/\1/p'|grep -E "$1"|grep -vE 'RANDOM|ALLOW_EMPTY|ONETIME|INITDB_ARGS'|sort -u;}
V(){ eval "v=\${$1:-}";case $1 in *_FILE)if [ -r "$v" ];then v=$(cat "$v");else v=;fi;;esac;}
F(){ for n in $(L "$1"|grep -E 'ROOT|ADMIN|SUPER') $(L "$1"|grep -vE 'ROOT|ADMIN|SUPER');do V $n;[ -n "$v" ]&&return 0;done;return 1;}
T='(^|_)(%[1]s)_?([A-Z0-9]+_)*';P=;U=
if F "${T}(PASSWORD|PASS|AUTH)(_FILE)?$";then P=$v;b=${n%%_FILE};b=${b%%PASSWORD};b=${b%%PASS};b=${b%%AUTH};for m in ${b}USERNAME ${b}USER ${b}USERNAME_FILE ${b}USER_FILE;do V $m;[ -n "$v" ]&&U=$v&&break;done
elif F "${T}USER(NAME)?(_FILE)?$";then U=$v;fi
s=$1;shift;eval "$s"
`

// CredentialCommand is the argv that runs script in a store's container with
// the store's credentials found the same way every store-state read finds
// them: P is the password and U its paired user, both possibly empty; args
// are the script's $1.. . The record operations of storeops use it too.
func CredentialCommand(tags []string, script string, args ...string) []string {
	return readCommand(tags, script, args...)
}

// readCommand is the argv of an engine's read: sh runs the credential
// prelude for the engine's tags, which runs script with args as $1.. .
func readCommand(tags []string, script string, args ...string) []string {
	prelude := fmt.Sprintf(credentialPrelude, strings.Join(tags, "|"))
	return append([]string{"sh", "-c", prelude, "sdo-store-read", script}, args...)
}
