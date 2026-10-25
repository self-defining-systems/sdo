package program

import (
	"fmt"
	"net/url"
	"sdo.dev/operator/sdk"
	"strings"
)

// Command runs inside a named container. Output is discarded; observations are
// exit codes. Stdin permits scripts and byte streams (including tar for cp).
// Credentials must be resolved inside the container through environment refs.
type Command struct {
	Pod       sdk.ObjectRef `json:"pod"`
	Container string        `json:"container"`
	Args      []string      `json:"args"`
	Stdin     string        `json:"stdin,omitempty"`
}

func (c Command) Validate() error {
	if c.Pod.APIVersion != "v1" || c.Pod.Kind != "Pod" || c.Pod.Namespace == "" || c.Pod.Name == "" || c.Container == "" || len(c.Args) == 0 || c.Args[0] == "" {
		return fmt.Errorf("command requires named Pod, container and executable")
	}
	return nil
}

// HTTPRequest uses curl inside a selected container. Its result is curl's exit
// code, including HTTP errors with --fail. Response bodies and stderr are never
// captured. Bearer names an environment variable; it is expanded only there.
type HTTPRequest struct {
	Pod       sdk.ObjectRef            `json:"pod"`
	Container string                   `json:"container"`
	Method    string                   `json:"method"`
	URL       string                   `json:"url"`
	Headers   map[string]string        `json:"headers,omitempty"`
	HeaderEnv map[string]CredentialRef `json:"header_env,omitempty"`
	Body      string                   `json:"body,omitempty"`
	Bearer    *CredentialRef           `json:"bearer,omitempty"`
}

func (h HTTPRequest) Validate() error {
	if err := (Command{Pod: h.Pod, Container: h.Container, Args: []string{"curl"}}).Validate(); err != nil {
		return err
	}
	u, err := url.Parse(h.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("HTTP request requires an http(s) URL without credentials")
	}
	query, queryErr := url.ParseQuery(u.RawQuery)
	if queryErr != nil {
		return fmt.Errorf("invalid HTTP query encoding")
	}
	for name := range query {
		if credentialHTTPName(name) {
			return fmt.Errorf("URL credentials require an environment-referenced container command")
		}
	}

	if h.Method == "" || strings.ContainsAny(h.Method, " \r\n\t") {
		return fmt.Errorf("HTTP method is required")
	}
	for name, value := range h.Headers {
		if credentialHTTPName(name) {
			return fmt.Errorf("authentication headers require environment references")
		}
		if strings.ContainsAny(name+value, "\r\n") {
			return fmt.Errorf("invalid HTTP header")
		}
	}

	for name, ref := range h.HeaderEnv {
		if name == "" || strings.ContainsAny(name, "\r\n:") {
			return fmt.Errorf("invalid HTTP header name")
		}
		if ref.ConfigMap != nil {
			return fmt.Errorf("HTTP credentials require env references")
		}
		if err := ref.Validate(); err != nil {
			return err
		}
	}
	if h.Bearer != nil {
		if h.Bearer.ConfigMap != nil {
			return fmt.Errorf("HTTP credentials require env references")
		}
		return h.Bearer.Validate()
	}
	return nil
}

// ExternalOperation is a serializable exact operation identity for recordings.
type ExternalOperation struct {
	Exec *Command     `json:"exec,omitempty"`
	HTTP *HTTPRequest `json:"http,omitempty"`
}

func (o ExternalOperation) Validate() error {
	if (o.Exec == nil) == (o.HTTP == nil) {
		return fmt.Errorf("external operation requires exactly one command or HTTP request")
	}
	if o.Exec != nil {
		return o.Exec.Validate()
	}
	return o.HTTP.Validate()
}
func (o ExternalOperation) Pod() sdk.ObjectRef {
	if o.Exec != nil {
		return o.Exec.Pod
	}
	if o.HTTP != nil {
		return o.HTTP.Pod
	}
	return sdk.ObjectRef{}
}

// ExternalEffect is controller-recorded transition evidence. It binds exact
// observe/ensure operations and before/after exit codes, never an expected fixed
// snapshot. Replay cannot invent this evidence from an authored program.
type ExternalEffect struct {
	Observe  ExternalOperation `json:"observe"`
	Ensure   ExternalOperation `json:"ensure"`
	Before   int32             `json:"before"`
	After    int32             `json:"after"`
	ExitCode int32             `json:"exit_code"`
}

type EnsureExec struct {
	Observe Command
	Ensure  Command
	Expect  int32
	Reason  string
}

func (EnsureExec) action() {}

type EnsureHTTP struct {
	Observe HTTPRequest
	Ensure  HTTPRequest
	Expect  int32
	Reason  string
}

func (EnsureHTTP) action() {}

// RunJob creates a named Job or Pod and waits for its actual completion.
// Replay requires a recorded completion for this exact manifest.
type RunJob struct {
	Object     ApplyObject
	Completion WaitFor
}

func (RunJob) action() {}
func validateExternalAction(action Action) error {
	switch a := action.(type) {
	case EnsureExec:
		if err := a.Observe.Validate(); err != nil {
			return err
		}
		return a.Ensure.Validate()
	case EnsureHTTP:
		if err := a.Observe.Validate(); err != nil {
			return err
		}
		return a.Ensure.Validate()
	case RunJob:
		if err := a.Object.Validate(); err != nil {
			return err
		}
		if a.Object.Ref.Kind != "Job" && a.Object.Ref.Kind != "Pod" {
			return fmt.Errorf("one-off runner requires Job or Pod")
		}
		if a.Completion.Ref != a.Object.Ref {
			return fmt.Errorf("job completion must observe the created object")
		}
		return a.Completion.Validate()
	case *EnsureExec:
		if a != nil {
			return validateExternalAction(*a)
		}
	case *EnsureHTTP:
		if a != nil {
			return validateExternalAction(*a)
		}
	case *RunJob:
		if a != nil {
			return validateExternalAction(*a)
		}
	default:
		return nil
	}
	return fmt.Errorf("nil external action")
}

func credentialHTTPName(name string) bool {
	normalized := strings.Map(func(r rune) rune {
		switch {
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		}
		return -1
	}, name)
	for _, credential := range []string{"authorization", "cookie", "apikey", "token", "secret", "password", "credential"} {
		if strings.Contains(normalized, credential) {
			return true
		}
	}
	return false
}
