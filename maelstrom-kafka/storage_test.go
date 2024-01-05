package main

import (
	"context"
	"testing"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/stretchr/testify/assert"
)

type inmemoryKV struct {
	stg map[string]any
}

func newInmemoryKV() *inmemoryKV {
	return &inmemoryKV{stg: make(map[string]any)}
}

func (kv *inmemoryKV) ReadInt(ctx context.Context, key string) (int, error) {
	if value, ok := kv.stg[key]; ok {
		return value.(int), nil
	}
	return 0, maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "unknown key")
}

func (kv *inmemoryKV) Write(ctx context.Context, key string, value any) error {
	kv.stg[key] = value
	return nil
}

func (kv *inmemoryKV) CompareAndSwap(ctx context.Context, key string, from, to any, createIfNotExists bool) error {
	value, ok := kv.stg[key]
	if !ok && !createIfNotExists {
		return maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "unknown key")
	}
	if ok && value != from {
		return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "different value")
	}
	kv.stg[key] = to
	return nil
}

func TestStorageAppendPoll(t *testing.T) {
	stg := NewTopicStorage(newInmemoryKV())
	id1, err := stg.Append("k1", 123)
	assert.NoError(t, err)
	id2, err := stg.Append("k1", 321)
	assert.NoError(t, err)
	assert.Less(t, id1, id2)
	poll, err := stg.Poll("k1", id1)
	assert.NoError(t, err)
	if assert.Greater(t, len(poll), 0) {
		assert.Equal(t, poll[0].offset, 0)
		assert.Equal(t, poll[0].value, 123)
	}

	id3, err := stg.Append("k2", 1234)
	assert.NoError(t, err)
	poll, err = stg.Poll("k2", id3)
	assert.NoError(t, err)
	if assert.Greater(t, len(poll), 0) {
		assert.Equal(t, poll[0].offset, 0)
		assert.Equal(t, poll[0].value, 1234)
	}
}

func TestStorageCommits(t *testing.T) {
	stg := NewTopicStorage(newInmemoryKV())
	stg.Append("k1", 123)
	stg.Commit("k1", 1234)
	stg.Commit("k2", 1)

	offset, err := stg.GetCommitedOffset("k1")
	assert.NoError(t, err)
	assert.Equal(t, offset, 1234)
	offset, err = stg.GetCommitedOffset("k2")
	assert.NoError(t, err)
	assert.Equal(t, offset, 1)
}
