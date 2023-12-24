package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	logger := log.Default()
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		id := 0
		logger.Printf("Processed message %s: generated id %d", body["type"], id)
		body["type"] = "generate_ok"
		body["id"] = uuid.New().String()
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
