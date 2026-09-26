package websocket

import (
	"bufio"
	"io"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// wsDial performs a raw opening handshake against an httptest server
// and returns a frame-speaking client.
func wsDial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil || !strings.Contains(status, "101") {
		t.Fatalf("handshake status = %q err=%v", status, err)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &testClient{nc: conn, br: br}
}

func TestServerTracksAndClosesConnections(t *testing.T) {
	release := make(chan struct{})
	cfg := quietConfig()
	cfg.CloseTimeout = 200 * time.Millisecond
	srv := NewServer(cfg, func(c *Conn) { <-release })
	ts := httptest.NewServer(srv)
	defer ts.Close()

	tc1 := wsDial(t, ts.Listener.Addr().String())
	tc2 := wsDial(t, ts.Listener.Addr().String())
	defer tc1.nc.Close()
	defer tc2.nc.Close()

	deadline := time.Now().Add(2 * time.Second)
	for srv.ConnCount() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := srv.ConnCount(); got != 2 {
		t.Fatalf("ConnCount = %d, want 2", got)
	}

	closeSrv := make(chan struct{})
	go func() { srv.Close(); close(closeSrv) }()

	// Each client must observe a close frame from the server.
	if code := tc1.readClose(t); code != CloseNormalClosure {
		t.Fatalf("conn1 close code = %d", code)
	}
	if code := tc2.readClose(t); code != CloseNormalClosure {
		t.Fatalf("conn2 close code = %d", code)
	}
	// Let the blocked handlers return so their deferred c.Close() runs;
	// it is idempotent and only untracks an already-closed connection.
	close(release)

	select {
	case <-closeSrv:
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close did not return")
	}
	// Wait for ServeHTTP handlers to untrack after release.
	deadline = time.Now().Add(2 * time.Second)
	for srv.ConnCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := srv.ConnCount(); got != 0 {
		t.Fatalf("ConnCount after close = %d, want 0", got)
	}
}

func TestServerRejectsAfterClose(t *testing.T) {
	srv := NewServer(quietConfig(), func(c *Conn) {})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	srv.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn,
		"GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 503") {
		t.Fatalf("status = %q, want 503", buf[:n])
	}
}
