package main

import (
	"context"
	"fmt"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type logEntry struct {
	offset int
	value  int
}

const (
	commitedSuffix = "commited_offset"
	offsetSuffix   = "offset"
	dataSuffix     = "data"
)

type KV interface {
	ReadInt(ctx context.Context, key string) (int, error)
	Write(ctx context.Context, key string, value any) error
	CompareAndSwap(ctx context.Context, key string, from, to any, createIfNotExists bool) error
}

type TopicStorage struct {
	// store in kv for each key:
	// key.commited_offset
	// key.offset
	// key.data.<offset> = value
	kv KV
}

func NewTopicStorage(kv KV) *TopicStorage {
	return &TopicStorage{kv}
}

func (stg *TopicStorage) readInt(key string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return stg.kv.ReadInt(ctx, key)
}

func (stg *TopicStorage) casInt(key string, from, to int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return stg.kv.CompareAndSwap(ctx, key, from, to, true)
}

func (stg *TopicStorage) incOffset(key string) (int, error) {
	key = fmt.Sprintf("%s.%s", key, offsetSuffix)
	for {
		from, err := stg.readInt(key)
		to := from + 1
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			from = -1
			to = 0
		} else if err != nil {
			return 0, err
		}
		err = stg.casInt(key, from, to)
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			continue
		}
		return to, err
	}
}

func (stg *TopicStorage) Append(key string, value int) (int, error) {
	offset, err := stg.incOffset(key)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return offset, stg.kv.Write(ctx, fmt.Sprintf("%s.%s.%d", key, dataSuffix, offset), value)
}

func (stg *TopicStorage) Poll(key string, offset int) ([]logEntry, error) {
	last, err := stg.readInt(fmt.Sprintf("%s.%s", key, offsetSuffix))
	if err != nil {
		return nil, err
	}
	var entries []logEntry
	for i := offset; i <= last; i++ {
		value, err := stg.readInt(fmt.Sprintf("%s.%s.%d", key, dataSuffix, i))
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			continue
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, logEntry{offset: i, value: value})
	}
	return entries, nil
}

func (stg *TopicStorage) Commit(key string, offset int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return stg.kv.Write(ctx, fmt.Sprintf("%s.%s", key, commitedSuffix), offset)
}

func (stg *TopicStorage) GetCommitedOffset(key string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return stg.kv.ReadInt(ctx, fmt.Sprintf("%s.%s", key, commitedSuffix))
}
