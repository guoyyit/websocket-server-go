package websocket

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// rawExchange sends a raw HTTP request over a fresh TCP connection,
// reads and drains the response, and returns the still-open
// connection with a reader positioned right after the response.
func rawExchange(t *testing.T, addr, req string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	if _, err := nc.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	resp.Body.Close()
	return nc, br, resp
}

// expectConnClosed requires the next read on the connection to return
// EOF promptly: the server must have closed the TCP connection after
// its response. A timeout here means the connection was left open.
func expectConnClosed(t *testing.T, nc net.Conn, br *bufio.Reader) {
	t.Helper()
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := br.ReadByte()
	if err != io.EOF {
		t.Fatalf("read after rejection = %v, want io.EOF (connection must be closed)", err)
	}
}

// waitGoroutineCount polls until the goroutine count drops to at most
// want, failing after a generous settle window.
func waitGoroutineCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if n := runtime.NumGoroutine(); n <= want {
			return
		} else if time.Now().After(deadline) {
			t.Fatalf("goroutines = %d, want <= %d (background activity leaked)", n, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func badHandshakeRequests(addr string) map[string]string {
	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	upgrade := "GET /chat HTTP/1.1\r\nHost: " + addr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
	return map[string]string{
		"plain GET no upgrade headers": "GET /chat HTTP/1.1\r\nHost: " + addr + "\r\n\r\n",
		"missing Sec-WebSocket-Key":    upgrade + "Sec-WebSocket-Version: 13\r\n\r\n",
		"key not base64":               upgrade + "Sec-WebSocket-Key: !!!\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"key wrong length":             upgrade + "Sec-WebSocket-Key: aGVsbG8=\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"wrong version":                upgrade + "Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 12\r\n\r\n",
		"missing version":              upgrade + "Sec-WebSocket-Key: " + key + "\r\n\r\n",
		"missing upgrade header":       "GET /chat HTTP/1.1\r\nHost: " + addr + "\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"wrong method":                 "POST /chat HTTP/1.1\r\nHost: " + addr + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\nContent-Length: 0\r\n\r\n",
	}
}

// Every kind of rejected upgrade must get its error response and then
// have the underlying TCP connection closed by the server.
func TestRejectedUpgradeClosesConnection(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	addr := strings.TrimPrefix(hs.URL, "http://")

	for name, req := range badHandshakeRequests(addr) {
		t.Run(name, func(t *testing.T) {
			nc, br, resp := rawExchange(t, addr, req)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			expectConnClosed(t, nc, br)
			if n := srv.ConnCount(); n != 0 {
				t.Fatalf("ConnCount = %d after rejection, want 0", n)
			}
		})
	}
}

// A connection refused because the server is shutting down must also
// be closed right after the 503 response.
func TestUpgradeAfterServerCloseClosesConnection(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	hs := httptest.NewServer(srv)
	defer hs.Close()
	srv.Close()
	addr := strings.TrimPrefix(hs.URL, "http://")

	req := fmt.Sprintf("GET /chat HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", addr)
	nc, br, resp := rawExchange(t, addr, req)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	expectConnClosed(t, nc, br)
}

// A pipelined valid upgrade behind a rejected request must never be
// processed: the connection is closed after the 400 response.
func TestRejectedConnectionCannotBeReused(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	addr := strings.TrimPrefix(hs.URL, "http://")

	bad := "GET /chat HTTP/1.1\r\nHost: " + addr + "\r\n\r\n"
	good := fmt.Sprintf("GET /chat HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n", addr)
	nc, br, resp := rawExchange(t, addr, bad+good)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	// No 101 for the pipelined request: the connection is gone.
	expectConnClosed(t, nc, br)
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d, want 0", n)
	}
}

// Rejections must not affect the server's ability to accept later
// valid upgrades.
func TestServerUsableAfterRejections(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	addr := strings.TrimPrefix(hs.URL, "http://")

	for _, req := range badHandshakeRequests(addr) {
		nc, br, _ := rawExchange(t, addr, req)
		expectConnClosed(t, nc, br)
		nc.Close()
	}

	tc := wsDial(t, hs.URL)
	if err := tc.writeFrame(true, OpText, []byte("still alive")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "still alive")
}

// A flood of concurrent invalid requests must leave no tracked
// connections and no lingering background activity.
func TestConcurrentRejectionsLeaveNoResidue(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()
	addr := strings.TrimPrefix(hs.URL, "http://")

	// Warm up, then take a goroutine baseline.
	nc, br, _ := rawExchange(t, addr, "GET /chat HTTP/1.1\r\nHost: "+addr+"\r\n\r\n")
	expectConnClosed(t, nc, br)
	nc.Close()
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const flood = 64
	var wg sync.WaitGroup
	errs := make([]error, flood)
	for i := 0; i < flood; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.Dial("tcp", addr)
			if err != nil {
				errs[i] = err
				return
			}
			defer c.Close()
			fmt.Fprintf(c, "GET /chat HTTP/1.1\r\nHost: %s\r\n\r\n", addr)
			c.SetReadDeadline(time.Now().Add(5 * time.Second))
			r := bufio.NewReader(c)
			resp, err := http.ReadResponse(r, &http.Request{Method: http.MethodGet})
			if err != nil {
				errs[i] = err
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				errs[i] = fmt.Errorf("status = %d, want 400", resp.StatusCode)
				return
			}
			if _, err := r.ReadByte(); err != io.EOF {
				errs[i] = fmt.Errorf("read after rejection = %v, want io.EOF", err)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d after %d rejections, want 0", n, flood)
	}
	waitGoroutineCount(t, baseline+4)
}
