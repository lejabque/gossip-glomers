package main

import "sync"

type MessageStorage struct {
	messages []int
	mutex    sync.Mutex
}

func (s *MessageStorage) Store(msg int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.messages = append(s.messages, msg)
}

func (s *MessageStorage) Read() []int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	res := make([]int, len(s.messages))
	copy(res, s.messages)
	return res
}
