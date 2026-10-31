package prober

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protowire"
	"sdo.dev/operator/sdk/traffic"
)

// HTTP/2 can replace a socket reset with a generic establishment error.
// Retain the underlying I/O category before the transport wraps it.
type linkConnection struct {
	net.Conn
	mu      sync.Mutex
	failure traffic.LinkFailureClass
}

func (c *linkConnection) record(err error) {
	if err == nil {
		return
	}
	class := linkErrorClass(err)
	if class == traffic.LinkFailureTransport {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure == "" {
		c.failure = class
	}
}
func (c *linkConnection) Read(b []byte) (int, error) {
	n, e := c.Conn.Read(b)
	c.record(e)
	return n, e
}
func (c *linkConnection) Write(b []byte) (int, error) {
	n, e := c.Conn.Write(b)
	c.record(e)
	return n, e
}
func (c *linkConnection) failureClass() traffic.LinkFailureClass {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

func bounded(s string) string {
	s = strings.ToValidUTF8(s, "?")
	if len(s) > traffic.MaxLinkExcerpt {
		s = s[:traffic.MaxLinkExcerpt]
		s = strings.ToValidUTF8(s, "")
	}
	return s
}

func linkErrorClass(err error) traffic.LinkFailureClass {
	var ne net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return traffic.LinkFailureTimeout
	case errors.Is(err, syscall.ECONNREFUSED):
		return traffic.LinkFailureRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return traffic.LinkFailureReset
	default:
		return traffic.LinkFailureTransport
	}
}

// probeProtocol uses only the fresh connection supplied by dialLink. All reads
// are bounded and share its deadline; no credentials or application data are sent.
func probeProtocol(ctx context.Context, c net.Conn, l traffic.Link) (status, excerpt string, class traffic.LinkFailureClass, err error) {
	fail := func(text string) (string, string, traffic.LinkFailureClass, error) {
		return "", bounded(text), traffic.LinkFailureProtocol, fmt.Errorf("invalid %s reply: %s", l.EffectiveProtocol(), bounded(text))
	}
	switch l.EffectiveProtocol() {
	case traffic.LinkProtocolTCP:
		return "connected", "", "", nil
	case traffic.LinkProtocolHTTP:
		path := "/"
		low, high := 100, 499
		if l.HTTP != nil {
			path = l.HTTP.Path
			low = l.HTTP.StatusMin
			high = l.HTTP.StatusMax
		}
		req, e := http.NewRequestWithContext(ctx, "GET", "http://"+l.To+path, nil)
		if e != nil {
			return fail(e.Error())
		}
		if e = req.Write(c); e != nil {
			return "", "", "", e
		}
		r, e := http.ReadResponse(bufio.NewReader(io.LimitReader(c, 16<<10)), req)
		if e != nil {
			return "", "", "", e
		}
		defer r.Body.Close()
		status = r.Status
		excerpt = bounded(r.Status)
		if r.StatusCode < low || r.StatusCode > high {
			return status, excerpt, traffic.LinkFailureNotServing, fmt.Errorf("HTTP %s outside %d..%d", status, low, high)
		}
		return status, excerpt, "", nil
	case traffic.LinkProtocolGRPC:
		return grpcProbe(ctx, c, l)
	case traffic.LinkProtocolRedis, traffic.LinkProtocolMemcached:
		command := "PING\r\n"
		if l.Protocol == traffic.LinkProtocolMemcached {
			command = "version\r\n"
		}
		if _, err = c.Write([]byte(command)); err != nil {
			return
		}
		line, e := bufio.NewReader(io.LimitReader(c, 1024)).ReadString('\n')
		if e != nil {
			return "", bounded(line), "", e
		}
		line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		status = bounded(line)
		excerpt = status
		if l.Protocol == traffic.LinkProtocolRedis {
			upper := strings.ToUpper(line)
			if line == "+PONG" || strings.HasPrefix(upper, "-NOAUTH ") || strings.HasPrefix(upper, "-ERR ") && strings.Contains(upper, "AUTH") {
				return
			}
			if strings.HasPrefix(line, "-") {
				class = traffic.LinkFailureNotServing
				err = fmt.Errorf("redis %s", status)
				return
			}
		} else {
			if strings.HasPrefix(line, "VERSION ") && len(line) > 8 {
				return
			}
			if strings.HasPrefix(line, "SERVER_ERROR") || strings.HasPrefix(line, "CLIENT_ERROR") || line == "ERROR" {
				class = traffic.LinkFailureNotServing
				err = fmt.Errorf("memcached %s", status)
				return
			}
		}
		return fail(line)
	case traffic.LinkProtocolPostgres:
		if _, err = c.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47}); err != nil {
			return
		}
		b := make([]byte, 1)
		if _, err = io.ReadFull(c, b); err != nil {
			return
		}
		if b[0] == 'S' || b[0] == 'N' {
			return string(b), string(b), "", nil
		}
		return fail(string(b))
	case traffic.LinkProtocolMySQL:
		h := make([]byte, 4)
		if _, err = io.ReadFull(c, h); err != nil {
			return
		}
		n := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
		if n < 1 || n > 65536 || h[3] != 0 {
			return fail("invalid greeting length or sequence")
		}
		b := make([]byte, n)
		if _, err = io.ReadFull(c, b); err != nil {
			return
		}
		if b[0] == 255 && n >= 3 {
			return "error", bounded(string(b[3:])), "", nil
		}
		// Protocol 10: NUL server version followed by connection ID, seed,
		// filler and lower capability flags (the oldest legal handshake).
		end := bytes.IndexByte(b[1:], 0) + 1
		if b[0] != 10 || end < 2 || len(b) < end+16 || b[end+13] != 0 {
			return fail("invalid protocol-10 greeting")
		}
		return "protocol-10", bounded(string(b[1:end])), "", nil
	case traffic.LinkProtocolMongoDB:
		return mongoProbe(c)
	default:
		return fail("unknown protocol")
	}
}

func grpcProbe(ctx context.Context, c net.Conn, l traffic.Link) (string, string, traffic.LinkFailureClass, error) {
	scheme := "http"
	if l.GRPC != nil && l.GRPC.TLS {
		scheme = "https"
		tc := tls.Client(c, &tls.Config{ServerName: l.To, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}})
		if err := tc.HandshakeContext(ctx); err != nil {
			return "", "", linkErrorClass(err), err
		}
		c = tc
	}
	cc, err := (&http2.Transport{MaxHeaderListSize: 16 << 10, MaxReadFrameSize: 16 << 10}).NewClientConn(c)
	if err != nil {
		return "", "", "", err
	}
	defer cc.Close()
	req, err := http.NewRequestWithContext(ctx, "POST", scheme+"://"+l.To+"/grpc.health.v1.Health/Check", bytes.NewReader([]byte{0, 0, 0, 0, 0}))
	if err != nil {
		return "", "", "", err
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	r, err := cc.RoundTrip(req)
	if err != nil {
		return "", "", "", err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 1025))
	if err != nil {
		return "", "", "", err
	}
	status := r.Trailer.Get("Grpc-Status")
	if status == "" {
		status = r.Header.Get("Grpc-Status")
	}
	excerpt := bounded(r.Trailer.Get("Grpc-Message"))
	bad := func() (string, string, traffic.LinkFailureClass, error) {
		return status, excerpt, traffic.LinkFailureProtocol, fmt.Errorf("invalid grpc health response (HTTP %d, grpc-status %q)", r.StatusCode, status)
	}
	if r.StatusCode != 200 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") || len(b) > 1024 {
		return bad()
	}
	code, e := strconv.Atoi(status)
	if e != nil || code < 0 || code > 16 {
		return bad()
	}
	if code != 0 {
		return status, excerpt, "", nil
	}
	if len(b) < 5 || b[0] != 0 || int(binary.BigEndian.Uint32(b[1:5])) != len(b)-5 {
		return bad()
	}
	health := uint64(0)
	for data := b[5:]; len(data) > 0; {
		number, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return bad()
		}
		data = data[n:]
		if number == 1 {
			if typ != protowire.VarintType {
				return bad()
			}
			value, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return bad()
			}
			health = value
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(number, typ, data)
			if n < 0 {
				return bad()
			}
			data = data[n:]
		}
	}
	if health == 1 {
		return "SERVING", excerpt, "", nil
	}
	if health > 3 {
		return bad()
	}
	return fmt.Sprintf("health=%d", health), excerpt, traffic.LinkFailureNotServing, fmt.Errorf("grpc health is not SERVING (%d)", health)

}

func mongoProbe(c net.Conn) (string, string, traffic.LinkFailureClass, error) {
	// OP_MSG {hello: 1, $db: "admin"}; fixed request ID 1, no credentials.
	doc := []byte{31, 0, 0, 0, 16, 'h', 'e', 'l', 'l', 'o', 0, 1, 0, 0, 0, 2, '$', 'd', 'b', 0, 6, 0, 0, 0, 'a', 'd', 'm', 'i', 'n', 0, 0}
	msg := make([]byte, 21)
	binary.LittleEndian.PutUint32(msg, uint32(len(msg)+len(doc)))
	binary.LittleEndian.PutUint32(msg[4:], 1)
	binary.LittleEndian.PutUint32(msg[12:], 2013)
	if _, err := c.Write(append(msg, doc...)); err != nil {
		return "", "", "", err
	}
	bad := func() (string, string, traffic.LinkFailureClass, error) {
		return "", "invalid OP_MSG hello reply", traffic.LinkFailureProtocol, fmt.Errorf("invalid mongodb OP_MSG hello reply")
	}
	h := make([]byte, 16)
	if _, err := io.ReadFull(c, h); err != nil {
		return "", "", "", err
	}
	n := int(binary.LittleEndian.Uint32(h))
	if n < 26 || n > 65536 || binary.LittleEndian.Uint32(h[12:]) != 2013 || binary.LittleEndian.Uint32(h[8:]) != 1 {
		return bad()
	}
	b := make([]byte, n-16)
	if _, err := io.ReadFull(c, b); err != nil {
		return "", "", "", err
	}
	flags := binary.LittleEndian.Uint32(b)
	if flags & ^uint32(1) != 0 || b[4] != 0 {
		return bad()
	}
	b = b[5:]
	if flags&1 != 0 {
		if len(b) < 4 {
			return bad()
		}
		b = b[:len(b)-4]
	}
	fields, e := mongoFields(b)
	if e != nil {
		return bad()
	}
	ok, exists := fields["ok"]
	if !exists {
		return bad()
	}
	if ok == float64(1) {
		return "ok=1", "", "", nil
	}
	code := fields["code"]
	text, _ := fields["errmsg"].(string)
	if code == float64(13) || code == float64(18) {
		return "auth-required", bounded(text), "", nil
	}
	if ok != float64(0) {
		return bad()
	}
	return "ok=0", bounded(text), traffic.LinkFailureNotServing, fmt.Errorf("mongodb hello failed: %s", bounded(text))
}

// mongoFields walks BSON boundaries rather than searching untrusted reply bytes.
// Values not needed for hello are skipped, including nested topology documents.
func mongoFields(b []byte) (map[string]any, error) {
	bad := fmt.Errorf("invalid BSON")
	if len(b) < 5 || int(binary.LittleEndian.Uint32(b)) != len(b) || b[len(b)-1] != 0 {
		return nil, bad
	}
	fields := map[string]any{}
	end := len(b) - 1
	for i := 4; i < end; {
		typ := b[i]
		i++
		z := bytes.IndexByte(b[i:end], 0)
		if z < 0 {
			return nil, bad
		}
		name := string(b[i : i+z])
		i += z + 1
		size := 0
		var v any
		switch typ {
		case 1, 9, 17, 18:
			size = 8
		case 16:
			size = 4
		case 8:
			size = 1
		case 7:
			size = 12
		case 10, 6:
			size = 0
		case 2, 3, 4, 5:
			if i+4 > end {
				return nil, bad
			}
			n := int(binary.LittleEndian.Uint32(b[i:]))
			size = n
			if typ == 2 {
				size = n + 4
				if n < 1 || i+size > end || b[i+size-1] != 0 {
					return nil, bad
				}
				v = string(b[i+4 : i+size-1])
			}
			if typ == 5 {
				size = n + 5
			}
			if (typ == 3 || typ == 4) && n < 5 {
				return nil, bad
			}
		default:
			return nil, bad
		}
		if size < 0 || size > end-i {
			return nil, bad
		}
		switch typ {
		case 1:
			v = math.Float64frombits(binary.LittleEndian.Uint64(b[i:]))
		case 16:
			v = float64(int32(binary.LittleEndian.Uint32(b[i:])))
		case 18:
			v = float64(int64(binary.LittleEndian.Uint64(b[i:])))
		}
		if _, dup := fields[name]; dup {
			return nil, bad
		}
		fields[name] = v
		i += size
	}
	return fields, nil
}
