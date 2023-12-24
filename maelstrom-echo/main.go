package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	logger := log.Default()
	n := maelstrom.NewNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		logger.Printf("Processed message %s with body %s", body["type"], body["echo"])
		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
