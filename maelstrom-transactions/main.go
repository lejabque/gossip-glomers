package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TxnRequest struct {
	MsgID int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

func main() {
	logger := log.Default()

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	var m sync.Mutex
	n.Handle("txn", func(msg maelstrom.Message) error {
		ctx := context.Background()
		var body TxnRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		resp := make([][]any, len(body.Txn))
		copy(resp, body.Txn)
		m.Lock()
		defer m.Unlock()
		for i, t := range body.Txn {
			mode := t[0].(string)
			key := strconv.FormatInt(int64(t[1].(float64)), 10)
			switch mode {
			case "r":
				value, err := kv.Read(ctx, key)
				if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
					continue
				}
				if err != nil {
					return err
				}
				resp[i][2] = value
			case "w":
				value := int64(t[2].(float64))
				kv.Write(ctx, key, value)
			}

		}

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  resp,
		})
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
