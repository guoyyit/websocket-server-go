package websocket

import (
	"bufio"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// testClient is a raw WebSocket peer used to drive a server-side Conn
// in tests. It speaks masked frames like a real client.
type testClient struct {
	nc net.Conn
	br *bufio.Reader
}

// newConnPair returns a running server-side Conn and a raw client
// connected to it over TCP.
func newConnPair(t *testing.T, cfg Config) (*Conn, *testClient) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	type res struct {
		c   *Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			ch <- res{err: err}
			return
		}
		c := newConn(nc, bufio.NewReader(nc), bufio.NewWriter(nc), cfg)
		go c.run()
		ch <- res{c: c}
	}()

	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	tc := &testClient{nc: nc, br: bufio.NewReader(nc)}
	t.Cleanup(func() { tc.nc.Close() })
	return r.c, tc
}

// quietConfig disables heartbeat and idle reaper for deterministic
// tests unless they opt in.
func quietConfig() Config {
	return Config{PingInterval: -1, IdleTimeout: -1}
}

// clientFrame serializes a masked client frame.
func clientFrame(fin bool, op Opcode, payload []byte) []byte {
	var b0 byte
	if fin {
		b0 |= 0x80
	}
	b0 |= byte(op)

	var head []byte
	n := len(payload)
	switch {
	case n <= 125:
		head = append(head, b0, 0x80|byte(n))
	case n <= 0xFFFF:
		head = append(head, b0, 0x80|126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		head = append(head, ext[:]...)
	default:
		head = append(head, b0, 0x80|127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		head = append(head, ext[:]...)
	}
	key := [4]byte{0x1a, 0x2b, 0x3c, 0x4d}
	head = append(head, key[:]...)
	masked := make([]byte, n)
	for i, b := range payload {
		masked[i] = b ^ key[i&3]
	}
	return append(head, masked...)
}

// writeFrame sends a masked client frame.
func (tc *testClient) writeFrame(fin bool, op Opcode, payload []byte) error {
	_, err := tc.nc.Write(clientFrame(fin, op, payload))
	return err
}

// readFrame reads one (unmasked) server frame.
func (tc *testClient) readFrame() (frameHeader, []byte, error) {
	tc.nc.SetReadDeadline(time.Now().Add(5 * time.Second))
	return readFrame(tc.br, 1<<20, false)
}

// readClose reads a frame and expects it to be a close frame.
func (tc *testClient) readClose(t *testing.T) int {
	t.Helper()
	h, payload, err := tc.readFrame()
	if err != nil {
		t.Fatalf("read close frame: %v", err)
	}
	if h.op != OpClose {
		t.Fatalf("op = %v, want OpClose", h.op)
	}
	if len(payload) < 2 {
		return CloseNoStatusReceived
	}
	return int(payload[0])<<8 | int(payload[1])
}

// expectMessage reads one data frame and checks op and payload.
func (tc *testClient) expectMessage(t *testing.T, op Opcode, want string) {
	t.Helper()
	h, payload, err := tc.readFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if h.op != op {
		t.Fatalf("op = %v, want %v", h.op, op)
	}
	if string(payload) != want {
		t.Fatalf("payload = %q, want %q", payload, want)
	}
}
