package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStorageAppendPoll(t *testing.T) {
	var stg TopicStorage
	id1 := stg.Append("k1", 123)
	id2 := stg.Append("k1", 321)
	assert.Less(t, id1, id2)
	poll := stg.Poll("k1", id1)
	if assert.Greater(t, len(poll), 0) {
		assert.Equal(t, poll[0].offset, 0)
		assert.Equal(t, poll[0].value, 123)
	}

	id3 := stg.Append("k2", 1234)
	poll = stg.Poll("k2", id3)
	if assert.Greater(t, len(poll), 0) {
		assert.Equal(t, poll[0].offset, 0)
		assert.Equal(t, poll[0].value, 1234)
	}
}

func TestStorageCommits(t *testing.T) {
	var stg TopicStorage
	stg.Append("k1", 123)
	stg.Commit("k1", 1234)
	stg.Commit("k2", 1)

	assert.Equal(t, stg.GetCommitedOffset("k1"), 1234)
	assert.Equal(t, stg.GetCommitedOffset("k2"), 1)
}
