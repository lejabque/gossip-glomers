package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"slices"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/maps"
)

type TxnRequest struct {
	MsgID int     `json:"msg_id"`
	Txn   [][]any `json:"txn"`
}

type Storage struct {
	m      sync.RWMutex
	data   map[int64]int64
	owners map[int64]string // msgID
}

func NewStorage() *Storage {
	return &Storage{
		data:   make(map[int64]int64),
		owners: make(map[int64]string),
	}
}

func (s *Storage) Lock(keys []int64, msgID string) error {
	s.m.Lock()
	defer s.m.Unlock()
	for i, k := range keys {
		if s.owners[k] != "" {
			s.unlockUnsafe(keys[:i])
			return fmt.Errorf("Failed to lock all keys")
		}
		s.owners[k] = msgID
	}
	return nil
}

func (s *Storage) unlockUnsafe(keys []int64) {
	for _, k := range keys {
		s.owners[k] = ""
	}
}

func (s *Storage) Unlock(keys []int64) {
	s.m.RLock()
	defer s.m.RUnlock()
	s.unlockUnsafe(keys)
}

func (s *Storage) Read(key int64) (int64, bool) {
	s.m.RLock()
	defer s.m.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *Storage) Write(key, value int64) {
	s.m.Lock()
	defer s.m.Unlock()
	s.data[key] = value
}

// TODO: refactor

func main() {
	logger := log.Default()

	n := maelstrom.NewNode()
	storage := NewStorage()
	n.Handle("txn", func(msg maelstrom.Message) error {
		ctx := context.Background()
		var body TxnRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		resp := make([][]any, len(body.Txn))

		keysSet := make(map[int64]struct{})
		for _, t := range body.Txn {
			keysSet[int64(t[1].(float64))] = struct{}{}
		}
		keys := maps.Keys(keysSet)
		slices.Sort(keys)
		/*
			if err := storage.Lock(keys, strconv.FormatInt(int64(body.MsgID), 10)); err != nil {
				return n.Reply(msg, map[string]any{
					"type":        "error",
					"in_reply_to": body.MsgID,
					"code":        maelstrom.TxnConflict,
					"text":        err.Error(),
				})
			}
		*/
		msgID := strconv.FormatInt(int64(body.MsgID), 10)
		for {
			err := storage.Lock(keys, msgID)
			if err == nil {
				break
			}
			logger.Printf("Failed to lock transaction: %s", err)
			time.Sleep(100 * time.Millisecond)
		}

		copy(resp, body.Txn)
		replication := make(map[int64]int64)
		for i, t := range body.Txn {
			mode := t[0].(string)
			k := int64(t[1].(float64))
			switch mode {
			case "r":
				v, ok := storage.Read(k)
				if !ok {
					continue
				}
				resp[i][2] = v
			case "w":
				value := int64(t[2].(float64))
				storage.Write(k, value)
				replication[k] = value
			}
		}
		storage.Unlock(keys)

		go func() {
			var req TxnRequest
			for k, v := range replication {
				req.Txn = append(req.Txn, []any{"w", k, v})
			}
			for _, nodeID := range n.NodeIDs() {
				if nodeID == n.ID() {
					continue
				}
				// TODO: retries
				_, err := n.SyncRPC(ctx, nodeID, req)
				if err != nil {
					continue
				}
			}
		}()

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  resp,
		})
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}
