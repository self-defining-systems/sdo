package deploymentio

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"
)

// entryTimeout bounds one connection or request to an entry point.
const entryTimeout = 5 * time.Second

// AnswerEntry dials host:port; when isHTTP it also sends GET / and accepts any
// status below 500 (a 404 still answers; a 5xx does not).
func AnswerEntry(ctx context.Context, host string, port int64, isHTTP bool) error {
	address := net.JoinHostPort(host, strconv.FormatInt(port, 10))
	dialer := net.Dialer{Timeout: entryTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	_ = conn.Close()
	if !isHTTP {
		return nil
	}
	bounded, cancel := context.WithTimeout(ctx, entryTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(bounded, http.MethodGet, "http://"+address+"/", nil)
	if err != nil {
		return err
	}
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	if response.StatusCode >= 500 {
		return fmt.Errorf("GET / answered %d", response.StatusCode)
	}
	return nil
}
