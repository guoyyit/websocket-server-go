package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

// clientFrame builds a masked frame as a real client would send it.
func clientFrame(fin bool, op Opcode, payload []byte) []byte {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d} // RFC 6455 §5.7 example key
	var buf bytes.Buffer
	b0 := byte(op)
	if fin {
		b0 |= 0x80
	}
	buf.WriteByte(b0)
	n := len(payload)
	switch {
	case n <= 125:
		buf.WriteByte(0x80 | byte(n))
	case n <= 0xFFFF:
		buf.WriteByte(0x80 | 126)
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	default:
		buf.WriteByte(0x80 | 127)
		for i := 7; i >= 0; i-- {
			buf.WriteByte(byte(uint64(n) >> (8 * i)))
		}
	}
	buf.Write(key[:])
	masked := make([]byte, n)
	copy(masked, payload)
	mask(key, masked)
	buf.Write(masked)
	return buf.Bytes()
}

func readFrameFrom(t *testing.T, raw []byte, maxFrame int64, requireMask bool) (frameHeader, []byte, error) {
	t.Helper()
	return readFrame(bufio.NewReader(bytes.NewReader(raw)), maxFrame, requireMask)
}

func TestReadFrameUnmasksPayload(t *testing.T) {
	// RFC 6455 §5.7: masked "Hello" must decode correctly.
	raw := []byte{0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}
	h, payload, err := readFrameFrom(t, raw, 1<<20, true)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if !h.fin || h.op != OpText || !h.masked {
		t.Fatalf("unexpected header: %+v", h)
	}
	if string(payload) != "Hello" {
		t.Fatalf("payload = %q, want %q", payload, "Hello")
	}
}

func TestReadFrameRejectsUnmaskedClientFrame(t *testing.T) {
	raw := []byte{0x81, 0x02, 'h', 'i'}
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameExtendedLengths(t *testing.T) {
	for _, n := range []int{126, 200, 65535, 65536, 70000} {
		payload := bytes.Repeat([]byte{'x'}, n)
		raw := clientFrame(true, OpBinary, payload)
		h, got, err := readFrameFrom(t, raw, 1<<20, true)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if h.length != int64(n) || !bytes.Equal(got, payload) {
			t.Fatalf("n=%d: length=%d got=%d bytes", n, h.length, len(got))
		}
	}
}

func TestReadFrameRejectsNonMinimalLength(t *testing.T) {
	// 100 encoded with the 16-bit extended form is non-minimal.
	raw := []byte{0x82, 0xFE, 0x00, 100}
	raw = append(raw, 0x37, 0xfa, 0x21, 0x3d)
	raw = append(raw, make([]byte, 100)...)
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameRejectsReservedBits(t *testing.T) {
	raw := clientFrame(true, OpText, []byte("hi"))
	raw[0] |= 0x40 // set RSV1
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameRejectsFragmentedControl(t *testing.T) {
	raw := clientFrame(false, OpPing, []byte("x"))
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameRejectsOversizedControl(t *testing.T) {
	raw := clientFrame(true, OpClose, make([]byte, 126))
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameRejectsUnknownOpcode(t *testing.T) {
	raw := clientFrame(true, Opcode(0x3), nil)
	_, _, err := readFrameFrom(t, raw, 1<<20, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseProtocolError {
		t.Fatalf("err = %v, want protocol error 1002", err)
	}
}

func TestReadFrameEnforcesMaxFrameSize(t *testing.T) {
	raw := clientFrame(true, OpBinary, make([]byte, 1024))
	_, _, err := readFrameFrom(t, raw, 100, true)
	var pe *protocolError
	if !errors.As(err, &pe) || pe.code != CloseMessageTooBig {
		t.Fatalf("err = %v, want protocol error 1009", err)
	}
}

func TestWriteFrameRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 125, 126, 65535, 65536} {
		payload := bytes.Repeat([]byte{'y'}, n)
		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		if err := writeFrame(w, true, OpBinary, payload); err != nil {
			t.Fatalf("n=%d writeFrame: %v", n, err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("n=%d flush: %v", n, err)
		}
		// Server frames are unmasked; client side reads with requireMask=false.
		h, got, err := readFrame(bufio.NewReader(&buf), 1<<20, false)
		if err != nil {
			t.Fatalf("n=%d readFrame: %v", n, err)
		}
		if h.masked {
			t.Fatalf("n=%d: server frame must not be masked", n)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("n=%d: payload mismatch", n)
		}
	}
}

func TestMaskVector(t *testing.T) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	b := []byte("Hello")
	mask(key, b)
	want := []byte{0x7f, 0x9f, 0x4d, 0x51, 0x58}
	if !bytes.Equal(b, want) {
		t.Fatalf("mask = %x, want %x", b, want)
	}
	mask(key, b) // masking twice restores the original
	if string(b) != "Hello" {
		t.Fatalf("double mask = %q, want %q", b, "Hello")
	}
}
