package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	logger := log.Default()
	var storage MessageStorage

	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value := body["message"].(float64)
		storage.Store(int(value))
		resp := map[string]any{"type": "broadcast_ok"}
		logger.Printf("Processed broadcast message: stored %f", value)
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		values := storage.Read()
		resp := map[string]any{"messages": values, "type": "read_ok"}
		logger.Printf("Processed read message")
		return n.Reply(msg, resp)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// ignoring topology for now
		resp := map[string]any{"type": "topology_ok"}
		logger.Printf("Processed topology message")
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
