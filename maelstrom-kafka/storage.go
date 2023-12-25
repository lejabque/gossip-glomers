package main

import "sync"

type logEntry struct {
	offset int
	value  int
}

type topic struct {
	// TODO: retention?
	commitedOffset int
	data           []logEntry
	mutex          sync.Mutex
}

type TopicStorage struct {
	topics sync.Map // string -> topic
}

func (stg *TopicStorage) Append(key string, value int) int {
	raw, _ := stg.topics.LoadOrStore(key, &topic{})
	t := raw.(*topic)
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.data = append(t.data, logEntry{len(t.data), value})
	return len(t.data) - 1
}

func (stg *TopicStorage) Poll(key string, offset int) []logEntry {
	raw, loaded := stg.topics.Load(key)
	if !loaded {
		return nil
	}
	t := raw.(*topic)
	t.mutex.Lock()
	defer t.mutex.Unlock()
	var out []logEntry
	for i := offset; i < len(t.data); i++ {
		out = append(out, t.data[i])
	}
	return out
}

func (stg *TopicStorage) Commit(key string, offset int) {
	raw, _ := stg.topics.LoadOrStore(key, &topic{})
	t := raw.(*topic)
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.commitedOffset = offset
}

func (stg *TopicStorage) GetCommitedOffset(key string) int {
	raw, loaded := stg.topics.Load(key)
	if !loaded {
		return 0
	}
	t := raw.(*topic)
	t.mutex.Lock()
	defer t.mutex.Unlock()
	return t.commitedOffset
}
