package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	logger := log.Default()

	n := maelstrom.NewNode()
	stg := NewTopicStorage(maelstrom.NewLinKV(n))
	n.Handle("send", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		key := body["key"].(string)
		value := int(body["msg"].(float64))
		offset, err := stg.Append(key, value)
		if err != nil {
			return err
		}

		resp := map[string]any{"type": "send_ok", "offset": offset}
		return n.Reply(msg, resp)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]interface{})
		msgs := make(map[string][][2]int)
		for key, offset := range offsets {
			poll, err := stg.Poll(key, int(offset.(float64)))
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
			}
			if err != nil {
				return err
			}
			conv := make([][2]int, len(poll))
			for i, entry := range poll {
				conv[i] = [2]int{entry.offset, entry.value}
			}
			msgs[key] = conv
		}

		resp := map[string]any{"type": "poll_ok", "msgs": msgs}
		return n.Reply(msg, resp)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		offsets := body["offsets"].(map[string]interface{})
		for key, offset := range offsets {
			err := stg.Commit(key, int(offset.(float64)))
			if err != nil {
				return err
			}
		}

		resp := map[string]any{"type": "commit_offsets_ok"}
		return n.Reply(msg, resp)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		keys := body["keys"].([]interface{})
		offsets := make(map[string]int)
		for _, k := range keys {
			key := k.(string)
			offset, err := stg.GetCommitedOffset(key)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				continue
			}
			if err != nil {
				return err
			}
			offsets[key] = offset
		}

		resp := map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
