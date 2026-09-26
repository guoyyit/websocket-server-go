package websocket

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Opcode identifies the type of a WebSocket frame (RFC 6455 §5.2).
type Opcode byte

const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// isControl reports whether the opcode is a control opcode (0x8-0xF).
func (o Opcode) isControl() bool { return o >= 0x8 }

// valid reports whether the opcode is defined by RFC 6455.
func (o Opcode) valid() bool {
	switch o {
	case OpContinuation, OpText, OpBinary, OpClose, OpPing, OpPong:
		return true
	}
	return false
}

// Close status codes (RFC 6455 §7.4).
const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseInternalServerErr       = 1011
)

// CloseError represents a WebSocket close frame payload.
type CloseError struct {
	Code int
	Text string
}

func (e *CloseError) Error() string {
	return "websocket: close " + itoa(e.Code) + ": " + e.Text
}

// frameHeader is the parsed header of a single WebSocket frame.
type frameHeader struct {
	fin     bool
	op      Opcode
	masked  bool
	maskKey [4]byte
	length  int64 // payload length after decoding the length field
}

// protocolError describes an RFC 6455 violation; code is the close
// status the endpoint must send before closing the connection.
type protocolError struct {
	code int
	msg  string
}

func (e *protocolError) Error() string { return "websocket: protocol error: " + e.msg }

func protoErr(code int, msg string) *protocolError {
	return &protocolError{code: code, msg: msg}
}

// maxControlPayload is the RFC 6455 §5.5 limit on control frame bodies.
const maxControlPayload = 125

// readFrame reads a single frame (header + unmasked payload) from r.
// It enforces the per-frame size limit and, when requireMask is true,
// rejects unmasked frames (server side per RFC 6455 §5.1).
// All protocol violations are reported as *protocolError carrying the
// close code the peer must be sent.
func readFrame(r *bufio.Reader, maxFrame int64, requireMask bool) (frameHeader, []byte, error) {
	var h frameHeader

	b0, err := r.ReadByte()
	if err != nil {
		return h, nil, err
	}
	b1, err := r.ReadByte()
	if err != nil {
		return h, nil, err
	}

	h.fin = b0&0x80 != 0
	rsv := b0 & 0x70
	h.op = Opcode(b0 & 0x0F)
	h.masked = b1&0x80 != 0
	h.length = int64(b1 & 0x7F)

	// RFC 6455 §5.2: no extensions negotiated, RSV bits must be 0.
	if rsv != 0 {
		return h, nil, protoErr(CloseProtocolError, "non-zero RSV bits without negotiated extension")
	}
	if !h.op.valid() {
		return h, nil, protoErr(CloseProtocolError, "unknown opcode "+itoa(int(h.op)))
	}
	// RFC 6455 §5.5: control frames must not be fragmented and their
	// payload is limited to 125 bytes.
	if h.op.isControl() {
		if !h.fin {
			return h, nil, protoErr(CloseProtocolError, "fragmented control frame")
		}
		if h.length > maxControlPayload {
			return h, nil, protoErr(CloseProtocolError, "control frame payload too large")
		}
	}

	// Extended payload lengths (RFC 6455 §5.2).
	switch h.length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return h, nil, err
		}
		h.length = int64(binary.BigEndian.Uint16(ext[:]))
		if h.length < 126 {
			return h, nil, protoErr(CloseProtocolError, "non-minimal length encoding")
		}
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return h, nil, err
		}
		v := binary.BigEndian.Uint64(ext[:])
		if v&(1<<63) != 0 {
			return h, nil, protoErr(CloseProtocolError, "frame length with high bit set")
		}
		if v < 1<<16 {
			return h, nil, protoErr(CloseProtocolError, "non-minimal length encoding")
		}
		h.length = int64(v)
	}

	if h.length > maxFrame {
		return h, nil, protoErr(CloseMessageTooBig, "frame exceeds maximum size")
	}

	// RFC 6455 §5.1: a server must not accept unmasked frames.
	if requireMask && !h.masked {
		return h, nil, protoErr(CloseProtocolError, "received unmasked frame from client")
	}

	if h.masked {
		if _, err := io.ReadFull(r, h.maskKey[:]); err != nil {
			return h, nil, err
		}
	}

	payload := make([]byte, h.length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return h, nil, err
	}
	if h.masked {
		mask(h.maskKey, payload)
	}
	return h, payload, nil
}

// writeFrame serializes one frame (server side: never masked) into w.
// The caller is responsible for flushing.
func writeFrame(w *bufio.Writer, fin bool, op Opcode, payload []byte) error {
	var b0 byte
	if fin {
		b0 |= 0x80
	}
	b0 |= byte(op)
	if err := w.WriteByte(b0); err != nil {
		return err
	}

	n := len(payload)
	switch {
	case n <= 125:
		if err := w.WriteByte(byte(n)); err != nil {
			return err
		}
	case n <= 0xFFFF:
		if err := w.WriteByte(126); err != nil {
			return err
		}
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		if _, err := w.Write(ext[:]); err != nil {
			return err
		}
	default:
		if err := w.WriteByte(127); err != nil {
			return err
		}
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		if _, err := w.Write(ext[:]); err != nil {
			return err
		}
	}
	_, err := w.Write(payload)
	return err
}

// mask applies the RFC 6455 §5.3 masking algorithm in place.
func mask(key [4]byte, b []byte) {
	for i := range b {
		b[i] ^= key[i&3]
	}
}

// itoa is a tiny strconv.Itoa to keep imports minimal in this file.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
