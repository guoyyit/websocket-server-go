package websocket

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

// rawDial opens a raw TCP connection to the test server.
func rawDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// httpAddr strips the scheme off an httptest URL.
func httpAddr(url string) string {
	return strings.TrimPrefix(url, "http://")
}

// readRawResponse reads one HTTP response over a raw connection.
func readRawResponse(t *testing.T, nc net.Conn) *http.Response {
	t.Helper()
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(nc), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read error response: %v", err)
	}
	return resp
}

// expectEOF asserts that the next read on the connection returns EOF
// (peer closed the TCP connection) promptly rather than blocking.
func expectEOF(t *testing.T, nc net.Conn) {
	t.Helper()
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 16)
	n, err := nc.Read(buf)
	var ne net.Error
	if n == 0 && errors.As(err, &ne) && ne.Timeout() {
		t.Fatalf("read after error response timed out: TCP connection was not closed")
	}
	if n != 0 || (err != io.EOF && !errors.Is(err, net.ErrClosed) && !isReset(err)) {
		t.Fatalf("read after error response = (%d, %v), want immediate connection close (EOF/RST)", n, err)
	}
}

// isReset reports whether err is a TCP RST, which also signals an
// immediate peer-side connection close.
func isReset(err error) bool {
	return err != nil && strings.Contains(err.Error(), "reset by peer")
}

// validUpgrade is a well-formed WebSocket upgrade request.
const validUpgrade = "GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"

func TestRejectedUpgradeClosesTCP(t *testing.T) {
	cases := map[string]string{
		"plain get":           "GET /chat HTTP/1.1\r\nHost: example.com\r\n\r\n",
		"missing key":         "GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"bad version":         "GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 12\r\n\r\n",
		"missing connection":  "GET /chat HTTP/1.1\r\nHost: example.com\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"missing upgrade hdr": "GET /chat HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
		"bad method":          strings.Replace(validUpgrade, "GET", "POST", 1),
		"bad key encoding":    strings.Replace(validUpgrade, "dGhlIHNhbXBsZSBub25jZQ==", "not-base64!!", 1),
		"short key":           strings.Replace(validUpgrade, "dGhlIHNhbXBsZSBub25jZQ==", "AAAA", 1),
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			srv := NewServer(testServerConfig(), echoHandler)
			defer srv.Close()
			hs := httptest.NewServer(srv)
			defer hs.Close()

			nc := rawDial(t, httpAddr(hs.URL))
			if _, err := nc.Write([]byte(req)); err != nil {
				t.Fatal(err)
			}
			resp := readRawResponse(t, nc)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			resp.Body.Close()
			expectEOF(t, nc)

			// The connection must not accept a pipelined follow-up.
			if _, err := nc.Write([]byte(validUpgrade)); err == nil {
				expectEOF(t, nc)
			}
			if n := srv.ConnCount(); n != 0 {
				t.Fatalf("ConnCount = %d, want 0", n)
			}
		})
	}
}

func TestRejectedUpgradeDuringShutdownClosesTCP(t *testing.T) {
	srv := NewServer(testServerConfig(), echoHandler)
	hs := httptest.NewServer(srv)
	defer hs.Close()

	srv.Close()

	// A well-formed upgrade arriving while shutting down gets 503 and
	// an immediate TCP close.
	nc := rawDial(t, httpAddr(hs.URL))
	if _, err := nc.Write([]byte(validUpgrade)); err != nil {
		t.Fatal(err)
	}
	resp := readRawResponse(t, nc)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	resp.Body.Close()
	expectEOF(t, nc)
	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d, want 0", n)
	}

	// A malformed request while shut down also closes the connection.
	nc2 := rawDial(t, httpAddr(hs.URL))
	if _, err := nc2.Write([]byte("GET /chat HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	resp2 := readRawResponse(t, nc2)
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp2.StatusCode)
	}
	resp2.Body.Close()
	expectEOF(t, nc2)
}

func TestManyRejectedRequestsLeaveNoResidue(t *testing.T) {
	cfg := testServerConfig()
	srv := NewServer(cfg, echoHandler)
	defer srv.Close()
	hs := httptest.NewServer(srv)
	defer hs.Close()

	runtime.GC()
	// Warm up background goroutines, then measure the steady state.
	for i := 0; i < 20; i++ {
		nc := rawDial(t, httpAddr(hs.URL))
		nc.Write([]byte("GET /chat HTTP/1.1\r\nHost: example.com\r\n\r\n"))
		readRawResponse(t, nc).Body.Close()
		expectEOF(t, nc)
		nc.Close()
	}
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const n = 200
	errCh := make(chan error, n)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			nc, err := net.Dial("tcp", httpAddr(hs.URL))
			if err != nil {
				errCh <- fmt.Errorf("dial %d: %w", i, err)
				continue
			}
			req := "GET /chat HTTP/1.1\r\nHost: example.com\r\n\r\n"
			if i%2 == 0 {
				req = strings.Replace(validUpgrade, "Sec-WebSocket-Version: 13", "Sec-WebSocket-Version: 7", 1)
			}
			if _, err := nc.Write([]byte(req)); err != nil {
				errCh <- err
				nc.Close()
				continue
			}
			nc.SetReadDeadline(time.Now().Add(2 * time.Second))
			resp, rerr := http.ReadResponse(bufio.NewReader(nc), &http.Request{Method: http.MethodGet})
			if rerr != nil {
				errCh <- rerr
				nc.Close()
				continue
			}
			if resp.StatusCode != http.StatusBadRequest {
				errCh <- fmt.Errorf("status = %d, want 400", resp.StatusCode)
			}
			resp.Body.Close()
			buf := make([]byte, 16)
			nc.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, rerr := nc.Read(buf); rerr == nil {
				errCh <- fmt.Errorf("connection %d not closed after 400", i)
			} else {
				var ne net.Error
				if errors.As(rerr, &ne) && ne.Timeout() {
					errCh <- fmt.Errorf("connection %d not closed (read timed out)", i)
				}
			}
			nc.Close()
		}
	}()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("bulk reject clients did not finish in time")
	}
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	if n := srv.ConnCount(); n != 0 {
		t.Fatalf("ConnCount = %d, want 0", n)
	}

	// Give the scheduler time to recycle transient goroutines and
	// assert no background activity growth remains.
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > baseline+2 {
		runtime.GC()
		if time.Now().After(deadline) {
			t.Fatalf("NumGoroutine = %d after bulk rejects, baseline %d", runtime.NumGoroutine(), baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A valid upgrade after the storm must still work.
	tc := wsDial(t, hs.URL)
	if err := tc.writeFrame(true, OpText, []byte("still alive")); err != nil {
		t.Fatal(err)
	}
	tc.expectMessage(t, OpText, "still alive")
}
