package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type storedValue struct {
	// last operation id. CAS after read will synchronize all reads with all cas
	SeqID int64
	Value int
}

func requestValue(kv *maelstrom.KV) (storedValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	valueJson, err := kv.Read(ctx, "global-counter")
	if err != nil {
		return storedValue{}, err
	}
	var value storedValue
	err = json.Unmarshal([]byte(valueJson.(string)), &value)
	return value, err
}

func casValue(kv *maelstrom.KV, old, new storedValue) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldJson, err := json.Marshal(old)
	if err != nil {
		return err
	}
	newJson, err := json.Marshal(new)
	if err != nil {
		return err
	}
	return kv.CompareAndSwap(ctx, "global-counter", string(oldJson), string(newJson), true)
}

func addValue(kv *maelstrom.KV, delta int) (storedValue, error) {
	for {
		value, err := requestValue(kv)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			return storedValue{}, err
		}
		newValue := storedValue{
			SeqID: value.SeqID + 1,
			Value: value.Value + delta,
		}
		err = casValue(kv, value, newValue)
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			continue
		}
		return newValue, err
	}
}

func readValue(kv *maelstrom.KV) (storedValue, error) {
	return addValue(kv, 0)
}

func main() {
	logger := log.Default()

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := int(body["delta"].(float64))
		if _, err := addValue(kv, delta); err != nil {
			return err
		}
		resp := map[string]any{"type": "add_ok"}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		value, err := readValue(kv)
		if err != nil {
			return err
		}
		resp := map[string]any{"type": "read_ok", "value": value.Value}
		return n.Reply(msg, resp)
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
