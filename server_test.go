package websocket

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// echoHandler echoes every message back to the sender.
func echoHandler(c *Conn) {
	for {
		m, err := c.ReadMessage()
		if err != nil {
			return
		}
		if err := c.WriteMessage(m.Type, m.Data); err != nil {
			return
		}
	}
}

// wsDial performs a real WebSocket handshake against url and returns
// a connected test client.
func wsDial(t *testing.T, url string) *testClient {
	t.Helper()
	addr := strings.TrimPrefix(url, "http://")
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(nonce)
	fmt.Fprintf(nc, "GET /chat HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", addr, key)

	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-Websocket-Accept"); got != computeAcceptKey(key) {
		t.Fatalf("accept key = %q, want %q", got, computeAcceptKey(key))
	}
	return &testClient{nc: nc, br: br}
}

func testServerConfig() Config {
	cfg := quietConfig()
	cfg.CloseTimeout = 200 * time.Millisecond
	return cfg
}

func TestServerEndToEndEcho(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()

	tc := wsDial(t, hs.URL)
	if err := tc.writeFrame(true, OpText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "hello")
}

func TestServerRejectsBadHandshake(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()

	// Plain GET without upgrade headers.
	resp, err := http.Get(hs.URL + "/chat")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}

	// Upgrade requested but missing Sec-WebSocket-Key.
	req, _ := http.NewRequest(http.MethodGet, hs.URL+"/chat", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp2.StatusCode)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after rejected handshakes, want 0", n)
	}
}

func TestServerTracksConnections(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()

	tc1 := wsDial(t, hs.URL)
	tc2 := wsDial(t, hs.URL)

	waitFor := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for srv.ConnCount() != want {
			if time.Now().After(deadline) {
				t.Fatalf("ConnCount = %d, want %d", srv.ConnCount(), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor(2)

	// Peer-initiated close must unregister the connection.
	if err := tc1.writeFrame(true, OpClose, []byte{0x03, 0xE8}); err != nil {
		t.Fatal(err)
	}
	if code := tc1.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close code = %d", code)
	}
	waitFor(1)
	_ = tc2
}

func TestServerCloseShutsDownConnections(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	hs := httptest.NewServer(srv)
	defer hs.Close()

	tc := wsDial(t, hs.URL)
	if err := tc.writeFrame(true, OpText, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "hi")

	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	// The client must have received a close frame.
	if code := tc.readClose(t); code != CloseNormalClosure {
		t.Fatalf("close code = %d, want %d", code, CloseNormalClosure)
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after Close, want 0", n)
	}
}

func TestUpgradeAfterServerClose(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	hs := httptest.NewServer(srv)
	defer hs.Close()
	srv.Close()

	req, _ := http.NewRequest(http.MethodGet, hs.URL+"/chat", nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Version", "13")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
}
