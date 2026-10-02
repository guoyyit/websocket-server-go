// Command echo runs a WebSocket echo server on :8080 for local
// experimentation with the websocket package.
package main

import (
	"log"
	"net/http"

	ws "github.com/guoyyit/websocket-server-go"
)

func main() {
	srv := ws.NewServer(ws.DefaultConfig(), func(c *ws.Conn) {
		for {
			m, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(m.Type, m.Data); err != nil {
				return
			}
		}
	})
	log.Println("websocket echo server listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", srv))
}
