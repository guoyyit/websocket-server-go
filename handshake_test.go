package websocket

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestComputeAcceptKeyRFCExample(t *testing.T) {
	// RFC 6455 §1.3 worked example.
	got := computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("computeAcceptKey = %q, want %q", got, want)
	}
}

func validHandshakeRequest(t *testing.T) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, "ws://example.com/chat", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Connection", "keep-alive, Upgrade")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	r.Header.Set("Sec-WebSocket-Version", "13")
	return r
}

func TestValidateHandshakeAcceptsValid(t *testing.T) {
	key, err := validateHandshake(validHandshakeRequest(t))
	if err != nil {
		t.Fatalf("validateHandshake: %v", err)
	}
	if key != "dGhlIHNhbXBsZSBub25jZQ==" {
		t.Fatalf("key = %q", key)
	}
}

func TestValidateHandshakeRejectsInvalid(t *testing.T) {
	cases := map[string]func(r *http.Request){
		"wrong method":       func(r *http.Request) { r.Method = http.MethodPost },
		"http/1.0":           func(r *http.Request) { r.ProtoMajor, r.ProtoMinor = 1, 0 },
		"missing upgrade":    func(r *http.Request) { r.Header.Del("Upgrade") },
		"wrong upgrade":      func(r *http.Request) { r.Header.Set("Upgrade", "h2c") },
		"missing connection": func(r *http.Request) { r.Header.Del("Connection") },
		"missing key":        func(r *http.Request) { r.Header.Del("Sec-Websocket-Key") },
		"key not base64":     func(r *http.Request) { r.Header.Set("Sec-Websocket-Key", "!!!") },
		"key wrong length":   func(r *http.Request) { r.Header.Set("Sec-Websocket-Key", "aGVsbG8=") }, // 5 bytes
		"wrong version":      func(r *http.Request) { r.Header.Set("Sec-Websocket-Version", "12") },
		"missing version":    func(r *http.Request) { r.Header.Del("Sec-Websocket-Version") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r := validHandshakeRequest(t)
			mutate(r)
			if _, err := validateHandshake(r); err == nil {
				t.Fatal("expected handshake rejection")
			}
		})
	}
}

func TestValidateHandshakeConnectionTokenList(t *testing.T) {
	r := validHandshakeRequest(t)
	if _, err := validateHandshake(r); err != nil {
		t.Fatalf("token list connection header rejected: %v", err)
	}
	r.Header.Set("Connection", "keep-alive")
	if _, err := validateHandshake(r); err == nil {
		t.Fatal("connection header without upgrade token accepted")
	}
}

func TestWriteHandshakeResponse(t *testing.T) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	key := computeAcceptKey("dGhlIHNhbXBsZSBub25jZQ==")
	if err := writeHandshakeResponse(w, key); err != nil {
		t.Fatalf("writeHandshakeResponse: %v", err)
	}
	resp := buf.String()
	for _, want := range []string{
		"HTTP/1.1 101 Switching Protocols\r\n",
		"Upgrade: websocket\r\n",
		"Connection: Upgrade\r\n",
		"Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n",
		"\r\n",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("response missing %q:\n%s", want, resp)
		}
	}
}

// TestServeHTTPBadHandshake verifies an invalid request receives a 400
// and the underlying TCP connection is closed.
func TestServeHTTPBadHandshake(t *testing.T) {
	srv := NewServer(quietConfig(), func(c *Conn) {})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer srv.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read error response: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "HTTP/1.1 400") {
		t.Fatalf("response = %q, want 400", buf[:n])
	}
	// Connection must be closed by the server afterwards.
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected underlying connection to be closed")
	}
}

// TestServerUpgradeEndToEnd performs a full RFC 6455 opening handshake.
func TestServerUpgradeEndToEnd(t *testing.T) {
	srv := NewServer(quietConfig(), func(c *Conn) {
		m, err := c.ReadMessage()
		if err == nil {
			_ = c.WriteMessage(m.Type, m.Data)
		}
	})
	ts := httptest.NewServer(srv)
	defer ts.Close()
	defer srv.Close()

	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	req := "GET / HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("status = %q, want 101", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
		if strings.HasPrefix(line, "Sec-WebSocket-Accept:") {
			if !strings.Contains(line, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=") {
				t.Fatalf("bad accept header: %q", line)
			}
		}
	}
	// Echo one frame after upgrade.
	tc := &testClient{nc: conn, br: br}
	if err := tc.writeFrame(true, OpText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "hello")
}
