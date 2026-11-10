package storestate

import (
	"fmt"
	"strings"
)

// redisRestoreScript runs one redis command given as arguments against the
// store's own port, authenticated like the read, prints the store's reply
// (never a credential) and the restored marker when the reply is OK or 1.
const redisRestoreScript = `[ -n "$P" ]&&export REDISCLI_AUTH="$P"
o=$(redis-cli -p %d "$@" 2>&1|tr -d '\r');printf '%%s\n' "$o"|head -n3
case "$o" in OK|1)echo '` + RestoredMarker + `';;*)exit 1;;esac
`

// redisRequirepassScript sets requirepass to the password the store
// container's own environment holds (the Secret the workload already uses),
// without printing it; with none it restores nothing.
const redisRequirepassScript = `[ -n "$P" ]||{ echo 'the store container environment holds no password to restore';exit 3;}
export REDISCLI_AUTH="$P"
o=$(redis-cli -p %d CONFIG SET requirepass "$P" 2>&1|tr -d '\r');printf '%%s\n' "$o"|head -n3
case "$o" in OK)echo '` + RestoredMarker + `';;*)exit 1;;esac
`

func redisCommand(e Endpoint, words ...string) []string {
	return readCommand([]string{"REDISCLI", "REDIS"}, fmt.Sprintf(redisRestoreScript, e.Port), words...)
}

// Restore implements Restorer: CONFIG SET of a baseline setting, REPLICAOF NO
// ONE for a primary made a replica, CLIENT UNPAUSE, the user's baseline ACL
// rules (passwords untouched), requirepass back to empty or to the password
// the container's environment holds, and a write block through the settings
// it follows from.
func (redisAdapter) Restore(e Endpoint, c Change, baseline Facts, _ RestoreOptions) ([][]string, error) {
	key := c.Key()
	setting := func(name string) ([]string, error) {
		item, ok := baseline.Lookup(Key{Kind: KindSetting, Name: name})
		if !ok {
			return nil, notRestorable(key, "the baseline does not show setting %s", name)
		}
		return redisCommand(e, "CONFIG", "SET", name, item.Value), nil
	}
	primary := func() ([]string, error) {
		role, ok := baseline.Lookup(Key{Kind: KindRole, Name: "role"})
		if !ok || role.Value != "master" {
			return nil, notRestorable(key, "the baseline role is not master, so there is no primary to promote back")
		}
		return redisCommand(e, "REPLICAOF", "NO", "ONE"), nil
	}
	one := func(cmd []string, err error) ([][]string, error) {
		if err != nil {
			return nil, err
		}
		return [][]string{cmd}, nil
	}
	switch key.Kind {
	case KindSetting:
		return one(setting(key.Name))
	case KindRole:
		return one(primary())
	case KindPrincipal:
		return redisRestorePrincipal(e, c)
	case KindWriteBlock:
		if c.After == nil {
			return nil, notRestorable(key, "the write block is already gone")
		}
		switch key.Name {
		case "client-pause":
			return [][]string{redisCommand(e, "CLIENT", "UNPAUSE")}, nil
		case "min-replicas":
			return one(setting("min-replicas-to-write"))
		case "oom":
			var out [][]string
			for _, name := range []string{"maxmemory", "maxmemory-policy"} {
				cmd, err := setting(name)
				if err != nil {
					return nil, err
				}
				out = append(out, cmd)
			}
			return out, nil
		case "replica-read-only":
			return one(primary())
		}
		return nil, notRestorable(key, "it follows from a failed disk write or a load in progress, not from a setting")
	}
	return nil, notRestorable(key, "redis %s items have no inverse", key.Kind)
}

func redisRestorePrincipal(e Endpoint, c Change) ([][]string, error) {
	key := c.Key()
	if key.Name == "requirepass" {
		switch {
		case c.Before == nil:
			return nil, notRestorable(key, "the baseline does not show it")
		case c.Before.Value == "empty":
			return [][]string{redisCommand(e, "CONFIG", "SET", "requirepass", "")}, nil
		}
		return [][]string{readCommand([]string{"REDISCLI", "REDIS"}, fmt.Sprintf(redisRequirepassScript, e.Port))}, nil
	}
	if c.Before == nil {
		if key.Name == "default" {
			return nil, notRestorable(key, "the default user cannot be deleted")
		}
		return [][]string{redisCommand(e, "ACL", "DELUSER", key.Name)}, nil
	}
	rules := strings.Fields(c.Before.Value)
	for _, rule := range rules {
		if strings.ContainsAny(rule, "()") {
			return nil, notRestorable(key, "its baseline rules hold selectors the value-free read does not keep exactly")
		}
	}
	if c.After == nil && !fields(c.Before.Value)["nopass"] {
		return nil, notRestorable(key, "the user was removed and its password is not kept; recreate it with the password from the Secret the workload uses")
	}
	// Reset the permissions the rules set, never the passwords: the baseline
	// rules have their password hashes removed.
	words := append([]string{"ACL", "SETUSER", key.Name, "resetkeys", "resetchannels", "-@all"}, rules...)
	return [][]string{redisCommand(e, words...)}, nil
}
