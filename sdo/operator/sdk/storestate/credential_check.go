package storestate

import (
	"fmt"
	"regexp"
	"strings"
)

// Credential check results.
const (
	// CredentialOK: the store accepted the credential (or needs none).
	CredentialOK = "ok"
	// CredentialDenied: the store reached refused the credential.
	CredentialDenied = "denied"
	// CredentialUnreachable: no store answered the authentication.
	CredentialUnreachable = "unreachable"
	// CredentialError: the check could not run or its answer is unknown.
	CredentialError = "error"
)

// CredentialCheck is the value-free answer of one authentication attempt with
// a client's credential: the result and a short class (for example
// "authenticated", "no-credential", "wrong-password", "max-clients"). Via is
// "uri" when the credential came as a connection URI, whose own host was
// dialled. It never carries a user name, password or record.
type CredentialCheck struct {
	Result string `json:"result"`
	Class  string `json:"class,omitempty"`
	Via    string `json:"via,omitempty"`
}

// CredentialMarker starts the one line a credential check prints.
const CredentialMarker = "SDO-AUTH"

// CredentialInputs names, never holds, the client's credential: the
// environment variables (a "<NAME>_FILE" variable names a mounted file) that
// carry its user, password, database (the postgres database or the mongodb
// authentication database) or connection URI. Empty names are discovered from
// the variable names the way the store read discovers its own.
type CredentialInputs struct {
	User     string
	Password string
	Database string
	URI      string
}

var envName = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)?$`)

// Validate checks that every input is an environment variable name.
func (c CredentialInputs) Validate() error {
	for _, n := range []string{c.User, c.Password, c.Database, c.URI} {
		if !envName.MatchString(n) {
			return fmt.Errorf("credential input %q is not an environment variable name", n)
		}
	}
	return nil
}

// credentialCheckTags extend an engine's own tags with the generic names a
// client gives its store credential.
var credentialCheckTags = map[Engine][]string{
	EngineRedis:    {"REDISCLI", "REDIS", "CACHE"},
	EnginePostgres: {"POSTGRES", "POSTGRESQL", "PG", "DB", "DATABASE"},
	EngineMongoDB:  {"MONGO", "MONGODB", "DB", "DATABASE"},
}

// credentialCheckHead selects the inputs: named variables first, else the
// discovered pair, else a URI-valued variable of the engine's schemes (%[1]s).
// $1 host, $2 port, $3..$6 the user, password, database and URI variable
// names. It prints only the result line, never the CLI's output.
const credentialCheckHead = `H=$1;O=$2;B=;R=;W=direct
[ -n "$3" ]&&{ V "$3";U=$v;};[ -n "$4" ]&&{ V "$4";P=$v;};[ -n "$5" ]&&{ V "$5";B=$v;}
if [ -n "$6" ];then V "$6";R=$v;elif [ -z "$P" ];then for n in $(L .);do V $n;case "$v" in %[1]s)R=$v;break;;esac;done;fi
[ -n "$R" ]&&W=uri
z(){ echo "` + CredentialMarker + ` $1 $2 $W";exit 0;}
`

// credentialCheckScripts attempt one authentication with the engine's CLI and
// classify the answer; no command reads data.
var credentialCheckScripts = map[Engine]string{
	EngineRedis: credentialCheckHead + `set -- --no-auth-warning
if [ -n "$R" ];then set -- "$@" -u "$R";else set -- "$@" -h "$H" -p "$O";[ -n "$U" ]&&set -- "$@" --user "$U";[ -n "$P" ]&&export REDISCLI_AUTH="$P";fi
o=$(redis-cli "$@" PING 2>&1|tr -d '\r'|head -n3)
case "$o" in *PONG*)[ -n "$P$R" ]&&z ok authenticated;z ok no-credential;;*NOPERM*)z ok authenticated;;
*WRONGPASS*|*"AUTH failed"*|*"invalid password"*|*"invalid username-password"*)z denied wrong-password;;*NOAUTH*)z denied credential-required;;
*"max number of clients"*)z unreachable max-clients;;*"Could not connect"*|*refused*|*"Name or service"*|*timed*)z unreachable no-connection;;esac;z error unclassified
`,
	EnginePostgres: credentialCheckHead + `export PGPASSWORD="$P" PGCONNECT_TIMEOUT=5
if [ -n "$R" ];then set -- "$R";else set -- -h "$H" -p "$O" -U "${U:-postgres}" -d "${B:-postgres}";fi
o=$(psql -X -At "$@" -c 'SELECT 1' 2>&1|tr -d '\r'|head -n3)
case "$o" in 1)z ok authenticated;;*"password authentication failed"*|*"SCRAM"*)z denied wrong-password;;*"no password supplied"*)z denied credential-required;;
*role*"does not exist"*)z denied unknown-user;;*"not permitted to log in"*)z denied login-refused;;*pg_hba.conf*)z denied host-refused;;
*database*"does not exist"*)z ok authenticated-no-database;;*"too many clients"*|*"remaining connection slots"*)z unreachable max-clients;;
*"could not connect"*|*refused*|*"timeout expired"*|*"could not translate"*)z unreachable no-connection;;esac;z error unclassified
`,
	EngineMongoDB: credentialCheckHead + `m=mongosh;command -v mongo >/dev/null 2>&1&&m=mongo
if [ -n "$R" ];then set -- "$R";else set -- --host "$H" --port "$O";[ -n "$P" ]&&set -- "$@" -u "${U:-root}" -p "$P" --authenticationDatabase "${B:-admin}";fi
o=$($m --quiet "$@" --eval 'print("SDO-USERS "+db.runCommand({connectionStatus:1}).authInfo.authenticatedUsers.length)' 2>&1|tr -d '\r'|tail -n3)
case "$o" in *"SDO-USERS 0"*)z ok no-credential;;*"SDO-USERS "[1-9]*)z ok authenticated;;*"uthentication failed"*|*AuthenticationFailed*)z denied wrong-password;;
*ECONNREFUSED*|*"connect failed"*|*"couldn't connect"*|*ServerSelection*|*"timed out"*|*MongoNetworkError*)z unreachable no-connection;;esac;z error unclassified
`,
}

var credentialSchemes = map[Engine]string{
	EngineRedis:    "redis://*|rediss://*",
	EnginePostgres: "postgres://*|postgresql://*",
	EngineMongoDB:  "mongodb://*|mongodb+srv://*",
}

// CredentialCheckCommand is the argv of one authentication attempt against
// the store at host:port with the client's credential, for a container that
// carries the client's environment (its env and envFrom references, which the
// kubelet resolves; SDO never reads the values) and the store engine's CLI.
func CredentialCheckCommand(engine Engine, host string, port int, inputs CredentialInputs) ([]string, error) {
	script, ok := credentialCheckScripts[engine]
	if !ok {
		return nil, fmt.Errorf("engine %q has no credential check", engine)
	}
	if err := inputs.Validate(); err != nil {
		return nil, err
	}
	if host == "" || strings.ContainsAny(host, " \t\n'\"$`\\") || port < 1 || port > 65535 {
		return nil, fmt.Errorf("credential check requires a host and a port")
	}
	return readCommand(credentialCheckTags[engine], fmt.Sprintf(script, credentialSchemes[engine]),
		host, fmt.Sprint(port), inputs.User, inputs.Password, inputs.Database, inputs.URI), nil
}

// CredentialCheckEngines lists the engines with a credential check.
func CredentialCheckEngines() []Engine {
	return []Engine{EngineMongoDB, EnginePostgres, EngineRedis}
}

// ParseCredentialCheck reads the result line of a credential check.
func ParseCredentialCheck(stdout string) (CredentialCheck, error) {
	for _, line := range strings.Split(stdout, "\n") {
		f := strings.Fields(line)
		if len(f) == 4 && f[0] == CredentialMarker {
			switch f[1] {
			case CredentialOK, CredentialDenied, CredentialUnreachable, CredentialError:
				check := CredentialCheck{Result: f[1], Class: f[2]}
				if f[3] == "uri" {
					check.Via = "uri"
				}
				return check, nil
			}
		}
	}
	return CredentialCheck{}, fmt.Errorf("credential check gave no result: %q", firstLine(stdout))
}
