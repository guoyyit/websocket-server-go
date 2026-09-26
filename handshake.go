package websocket

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
)

// websocketGUID is the magic string from RFC 6455 §1.3 used when
// computing the Sec-WebSocket-Accept response value.
const websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// errBadHandshake is returned when the HTTP request is not a valid
// WebSocket upgrade request.
var errBadHandshake = errors.New("websocket: bad handshake")

// computeAcceptKey derives Sec-WebSocket-Accept from the client's
// Sec-WebSocket-Key: base64(sha1(key + GUID)) per RFC 6455 §1.3.
func computeAcceptKey(key string) string {
	h := sha1.New()
	h.Write([]byte(key))
	h.Write([]byte(websocketGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// headerContainsToken reports whether the named header contains the
// given token (case-insensitive, comma-separated list per RFC 2616).
func headerContainsToken(h http.Header, name, token string) bool {
	for _, v := range h[http.CanonicalHeaderKey(name)] {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// validateHandshake checks that r is a well-formed WebSocket upgrade
// request (RFC 6455 §4.2.1): GET method, HTTP/1.1+, Upgrade and
// Connection headers, a valid Sec-WebSocket-Key and version 13.
// It returns the client key on success.
func validateHandshake(r *http.Request) (string, error) {
	if r.Method != http.MethodGet {
		return "", errBadHandshake
	}
	if r.ProtoMajor < 1 || (r.ProtoMajor == 1 && r.ProtoMinor < 1) {
		return "", errBadHandshake
	}
	if !headerContainsToken(r.Header, "Connection", "upgrade") {
		return "", errBadHandshake
	}
	if !headerContainsToken(r.Header, "Upgrade", "websocket") {
		return "", errBadHandshake
	}
	key := r.Header.Get("Sec-Websocket-Key")
	if key == "" {
		return "", errBadHandshake
	}
	// RFC 6455 §4.1: the key must be a base64-encoded 16-byte nonce.
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		return "", errBadHandshake
	}
	if v := r.Header.Get("Sec-Websocket-Version"); v != "13" {
		return "", errBadHandshake
	}
	return key, nil
}

// writeHandshakeResponse writes the 101 Switching Protocols response
// (RFC 6455 §4.2.2) and flushes it.
func writeHandshakeResponse(w *bufio.Writer, acceptKey string) error {
	var sb strings.Builder
	sb.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
	sb.WriteString("Upgrade: websocket\r\n")
	sb.WriteString("Connection: Upgrade\r\n")
	sb.WriteString("Sec-WebSocket-Accept: ")
	sb.WriteString(acceptKey)
	sb.WriteString("\r\n\r\n")
	if _, err := w.WriteString(sb.String()); err != nil {
		return err
	}
	return w.Flush()
}
