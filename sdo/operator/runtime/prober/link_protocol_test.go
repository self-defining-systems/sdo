package prober

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"sdo.dev/operator/sdk/traffic"
)

func mongoReply(ok int32) []byte {
	doc := []byte{13, 0, 0, 0, 16, 'o', 'k', 0, 0, 0, 0, 0, 0}
	binary.LittleEndian.PutUint32(doc[8:], uint32(ok))
	b := make([]byte, 21)
	binary.LittleEndian.PutUint32(b, uint32(21+len(doc)))
	binary.LittleEndian.PutUint32(b[12:], 2013)
	binary.LittleEndian.PutUint32(b[8:], 1)
	return append(b, doc...)
}

func TestLinkProtocols(t *testing.T) {
	for _, protocol := range []string{"tcp", "http", "grpc", "redis", "memcached", "mongodb", "postgres", "mysql"} {
		for _, mode := range []string{"reachable", "protocol", "refused", "timeout", "not-serving", "reset", "auth", "unimplemented", "unknown-health", "other-status"} {
			if mode == "auth" && protocol != "redis" && protocol != "mongodb" && protocol != "grpc" {
				continue
			}
			if (mode == "unimplemented" || mode == "unknown-health" || mode == "other-status") && protocol != "grpc" {
				continue
			}
			if protocol == "tcp" && mode != "reachable" && mode != "refused" {
				continue
			}
			if (mode == "not-serving") && (protocol == "postgres" || protocol == "mysql") {
				continue
			}
			t.Run(protocol+"/"+mode, func(t *testing.T) {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				address := ln.Addr().String()
				if mode == "refused" {
					ln.Close()
				} else {
					go func() {
						c, e := ln.Accept()
						if e != nil {
							return
						}
						defer c.Close()
						c.SetDeadline(time.Now().Add(time.Second))
						if mode == "timeout" {
							io.Copy(io.Discard, c)
							return
						}
						if mode == "reset" {
							c.(*net.TCPConn).SetLinger(0)
							return
						}
						if mode == "protocol" {
							c.Write([]byte("wrong\r\n"))
							c.(*net.TCPConn).CloseWrite()
							io.Copy(io.Discard, c)
							return
						}
						if protocol == "grpc" {
							(&http2.Server{}).ServeConn(c, &http2.ServeConnOpts{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								body, _ := io.ReadAll(r.Body)
								if r.Method != "POST" || r.URL.Path != "/grpc.health.v1.Health/Check" || !bytes.Equal(body, []byte{0, 0, 0, 0, 0}) {
									t.Errorf("invalid grpc health request %s %s %x", r.Method, r.URL.Path, body)
								}
								w.Header().Set("Content-Type", "application/grpc")
								w.Header().Set("Trailer", "Grpc-Status")
								w.WriteHeader(200)
								if mode == "auth" || mode == "unimplemented" || mode == "other-status" {
									code := "16"
									if mode == "other-status" {
										code = "7"
									}
									if mode == "unimplemented" {
										code = "12"
									}
									w.Header().Set("Grpc-Status", code)
									return
								}
								status := byte(1)
								if mode == "unknown-health" {
									status = 0
								}
								if mode == "not-serving" {
									status = 2
								}
								w.Write([]byte{0, 0, 0, 0, 2, 8, status})
								w.Header().Set("Grpc-Status", "0")
							})})
							return
						}
						if protocol != "mysql" {
							b := make([]byte, 1024)
							n, _ := c.Read(b)
							switch protocol {
							case "redis":
								if string(b[:n]) != "PING\r\n" {
									t.Errorf("redis request %q", b[:n])
								}
							case "memcached":
								if string(b[:n]) != "version\r\n" {
									t.Errorf("memcached request %q", b[:n])
								}
							case "postgres":
								if !bytes.Equal(b[:n], []byte{0, 0, 0, 8, 4, 210, 22, 47}) {
									t.Errorf("SSLRequest %x", b[:n])
								}
							case "mongodb":
								if n < 26 || int(binary.LittleEndian.Uint32(b)) != n || binary.LittleEndian.Uint32(b[12:]) != 2013 {
									t.Errorf("mongo request %x", b[:n])
								} else {
									fields, e := mongoFields(b[21:n])
									if e != nil || fields["hello"] != float64(1) || fields["$db"] != "admin" {
										t.Errorf("hello %v %v", fields, e)
									}
								}
							}

						}
						reply := ""
						switch protocol {
						case "http":
							reply = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"
							if mode == "not-serving" {
								reply = "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\n\r\n"
							}
						case "redis":
							reply = "+PONG\r\n"
							if mode == "auth" {
								reply = "-NOAUTH Authentication required.\r\n"
							}
							if mode == "not-serving" {
								reply = "-ERR unavailable\r\n"
							}
						case "memcached":
							reply = "VERSION 1.6\r\n"
							if mode == "not-serving" {
								reply = "SERVER_ERROR unavailable\r\n"
							}
						case "mongodb":
							ok := int32(1)
							if mode == "not-serving" {
								ok = 0
							}
							reply := mongoReply(ok)
							if mode == "auth" {
								// {ok: 0, code: 13}
								doc := []byte{23, 0, 0, 0, 16, 'o', 'k', 0, 0, 0, 0, 0, 16, 'c', 'o', 'd', 'e', 0, 13, 0, 0, 0, 0}
								reply = append(reply[:21], doc...)
								binary.LittleEndian.PutUint32(reply, uint32(len(reply)))
							}
							c.Write(reply)
							return
						case "postgres":
							reply = "N"
						case "mysql":
							reply = string(append([]byte{0, 0, 0, 0}, append([]byte{10}, append([]byte("8.0\x00"), make([]byte, 40)...)...)...))
							b := []byte(reply)
							b[0] = byte(len(b) - 4)
							reply = string(b)
						}
						c.Write([]byte(reply))
					}()
				}
				docs := map[string][]byte{"links": []byte(fmt.Sprintf(`{"apiVersion":"sdo.dev/v1alpha1","kind":"TrafficWorkload","name":"links","purpose":"link-probe","timeout":"50ms","links":[{"from":"web","to":"backend","port":1234,"protocol":%q}]}`, protocol))}
				ws, e := ParseWorkloads(docs)
				if e != nil {
					t.Fatal(e)
				}
				p, e := New(Config{Namespace: "test", Catalog: traffic.Catalog{{ID: "dummy", Target: traffic.Target{Service: "web", Port: 80}, SideEffect: traffic.SideEffectRead, Steps: []traffic.Step{{Name: "dummy", Endpoint: traffic.GET("/", nil)}}}}, Workloads: ws, LinkAddress: func(traffic.Link) string { return address }})
				if e != nil {
					t.Fatal(e)
				}
				state := &linkState{link: ws[0].Links[0]}
				p.dialLink(context.Background(), ws[0], state)
				if len(state.samples) != 1 {
					t.Fatal(state.samples)
				}
				s := state.samples[0]
				want := ""
				switch mode {
				case "protocol":
					want = "protocol_error"
				case "refused":
					want = "refused"
				case "timeout":
					want = "timeout"
				case "not-serving", "unknown-health":
					want = "not_serving"
				case "reset":
					want = "reset"
				}
				if s.OK != (mode == "reachable" || mode == "auth" || mode == "unimplemented" || mode == "other-status") || string(s.Class) != want || s.Duration <= 0 {
					t.Fatalf("sample %+v; want class %q", s, want)
				}
			})
		}
	}
}

// Opt-in wire smoke against a real Redis endpoint, e.g. a disposable kind pod.
func TestLinkRealRedis(t *testing.T) {
	address := os.Getenv("SDO_LINK_TEST_REDIS")
	if address == "" {
		t.Skip("set SDO_LINK_TEST_REDIS for the real Redis smoke")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	status, excerpt, class, err := probeProtocol(ctx, c, traffic.Link{Protocol: traffic.LinkProtocolRedis})
	if err != nil || class != "" || status != "+PONG" {
		t.Fatalf("status=%s excerpt=%s class=%s err=%v", status, excerpt, class, err)
	}
}

func TestLinkWireReplyVariants(t *testing.T) {
	for _, tc := range []struct {
		name  string
		link  traffic.Link
		reply []byte
		class traffic.LinkFailureClass
	}{
		{"redis auth error", traffic.Link{Protocol: traffic.LinkProtocolRedis}, []byte("-ERR Authentication required\r\n"), ""},
		{"redis malformed", traffic.Link{Protocol: traffic.LinkProtocolRedis}, []byte("+OK\r\n"), traffic.LinkFailureProtocol},
		{"postgres SSL", traffic.Link{Protocol: traffic.LinkProtocolPostgres}, []byte("S"), ""},
		{"mysql error", traffic.Link{Protocol: traffic.LinkProtocolMySQL}, []byte{6, 0, 0, 0, 255, 16, 4, 'b', 'a', 'd'}, ""},
		{"mysql truncated version", traffic.Link{Protocol: traffic.LinkProtocolMySQL}, []byte{2, 0, 0, 0, 10, 0}, traffic.LinkFailureProtocol},
		{"mongo invalid BSON", traffic.Link{Protocol: traffic.LinkProtocolMongoDB}, func() []byte { b := mongoReply(1); b[len(b)-1] = 1; return b }(), traffic.LinkFailureProtocol},
		{"mongo unrelated response", traffic.Link{Protocol: traffic.LinkProtocolMongoDB}, func() []byte { b := mongoReply(1); b[8] = 2; return b }(), traffic.LinkFailureProtocol},
		{"HTTP configured range", traffic.Link{To: "api", Protocol: traffic.LinkProtocolHTTP, HTTP: &traffic.LinkHTTPOptions{Path: "/ready", StatusMin: 200, StatusMax: 299}}, []byte("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"), traffic.LinkFailureNotServing},
		{"HTTP default accepts 401", traffic.Link{To: "api", Protocol: traffic.LinkProtocolHTTP}, []byte("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"), ""},
		{"bounded Redis error", traffic.Link{Protocol: traffic.LinkProtocolRedis}, []byte("-ERR " + strings.Repeat("x", 500) + "\r\n"), traffic.LinkFailureNotServing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			defer ln.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, e := ln.Accept()
				if e != nil {
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(time.Second))
				if tc.link.Protocol != traffic.LinkProtocolMySQL {
					b := make([]byte, 1024)
					c.Read(b)
					if tc.name == "HTTP configured range" && !bytes.Contains(b, []byte("GET /ready HTTP/1.1")) {
						t.Error("wrong path")
					}
				}
				c.Write(tc.reply)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c, e := (&net.Dialer{}).DialContext(ctx, "tcp", ln.Addr().String())
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			deadline, _ := ctx.Deadline()
			c.SetDeadline(deadline)
			_, excerpt, class, e := probeProtocol(ctx, c, tc.link)
			if class != tc.class || (e == nil) != (tc.class == "") {
				t.Fatalf("class=%s err=%v", class, e)
			}
			if len(excerpt) > traffic.MaxLinkExcerpt {
				t.Fatal("unbounded excerpt")
			}
			<-done
		})
	}
}

func TestLinkGRPCTLSVerifiesCertificates(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("untrusted TLS connection reached handler") }))
	server.EnableHTTP2 = true
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	deadline, _ := ctx.Deadline()
	c.SetDeadline(deadline)
	_, _, class, err := probeProtocol(ctx, c, traffic.Link{To: "backend", Protocol: traffic.LinkProtocolGRPC, GRPC: &traffic.LinkGRPCOptions{TLS: true}})
	if err == nil || class != traffic.LinkFailureTransport {
		t.Fatalf("untrusted TLS: class=%s err=%v", class, err)
	}
}
