// Package websocket implements the server side of RFC 6455 (the
// WebSocket protocol) on top of a hijacked net.Conn: opening
// handshake, frame parsing/serialization, control-frame handling,
// the closing handshake, bounded backpressure, size limits and
// heartbeat/idle reaping.
//
// # Connection state machine
//
// Each connection moves through three states:
//
//		open -> closing -> closed
//
//	  - open: normal operation. Data and control frames are accepted.
//	  - closing: entered exactly once when either side sends a close
//	    frame. New application writes are rejected (ErrClosed); queued
//	    outbound data is flushed before the close frame; control frames
//	    already queued are still delivered.
//	  - closed: the transport is shut down and Conn.Done is closed.
//	    Exactly one teardown happens regardless of how many goroutines
//	    call Close.
//
// Close handshake (RFC 6455 §5.5.1, §7.1):
//
//   - On a peer close frame the read loop validates the body, replies
//     with a close frame (after draining pending writes) and tears
//     down immediately.
//   - On a locally initiated close the write loop flushes pending
//     data, sends the close frame and waits for the peer's reply,
//     bounded by CloseTimeout.
//   - TCP loss before any close frame is reported as
//     CloseAbnormalClosure (1006); protocol violations send the
//     matching close code (1002/1007/1009) first.
//
// # Concurrency model
//
// A single goroutine (writeLoop) owns the wire for writes; a single
// readLoop parses frames; a watchdog emits pings and reaps idle/dead
// connections. Application goroutines enqueue frames into a bounded
// channel, so a slow consumer blocks only its own producers and never
// another connection. Queue depth is observable through
// WriteBufferLen/WriteBufferCap.
//
// # Limits
//
// MaxFrameSize bounds any single inbound frame (CloseMessageTooBig).
// MaxMessageSize bounds the reassembled message across fragments,
// including the final single-frame case. Control frames are always
// capped at 125 bytes by the protocol.
package websocket
