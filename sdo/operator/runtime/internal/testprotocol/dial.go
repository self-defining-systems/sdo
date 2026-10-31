// Package testprotocol supplies bounded, credential-free wire peers for hermetic tests.
package testprotocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"golang.org/x/net/http2"
)

// Dial returns a fresh peer in healthy, refused or protocol_error mode.
// The real prober still performs every handshake and classifies the reply.
func Dial(ctx context.Context, protocol, mode string) (net.Conn, error) {
	if mode == "refused" {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	}
	c, peer := net.Pipe()
	go func() {
		defer peer.Close()
		peer.SetDeadline(time.Now().Add(time.Second))
		stop := context.AfterFunc(ctx, func() { peer.Close() })
		defer stop()
		if protocol == "grpc" {
			(&http2.Server{}).ServeConn(peer, &http2.ServeConnOpts{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/grpc")
				w.Header().Set("Trailer", "Grpc-Status")
				w.WriteHeader(200)
				if mode == "protocol_error" {
					w.Write([]byte{0, 0, 0, 0, 1, 255})
				} else {
					w.Write([]byte{0, 0, 0, 0, 2, 8, 1})
				}
				w.Header().Set("Grpc-Status", "0")
			})})
			return
		}
		if protocol != "mysql" {
			b := make([]byte, 4096)
			if _, err := peer.Read(b); err != nil {
				return
			}
		}
		if mode == "protocol_error" {
			peer.Write([]byte("wrong\r\n"))
			return
		}
		switch protocol {
		case "http":
			peer.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
		case "redis":
			peer.Write([]byte("+PONG\r\n"))
		case "memcached":
			peer.Write([]byte("VERSION 1.6\r\n"))
		case "postgres":
			peer.Write([]byte("N"))
		case "mysql":
			b := append([]byte{0, 0, 0, 0, 10}, append([]byte("8.0\x00"), make([]byte, 40)...)...)
			b[0] = byte(len(b) - 4)
			peer.Write(b)
		case "mongodb":
			doc := []byte{13, 0, 0, 0, 16, 'o', 'k', 0, 1, 0, 0, 0, 0}
			b := make([]byte, 21)
			binary.LittleEndian.PutUint32(b, uint32(21+len(doc)))
			binary.LittleEndian.PutUint32(b[8:], 1)
			binary.LittleEndian.PutUint32(b[12:], 2013)
			peer.Write(append(b, doc...))
		default:
			panic(fmt.Sprintf("unsupported fake protocol %q", protocol))
		}
	}()
	return c, nil
}
