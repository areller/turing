package redis

import (
	"github.com/areller/turing"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestLRange(t *testing.T) {
	db := NewDBMock()
	lst, err := db.LRange("myList", 0, 10)
	assert.Equal(t, []string{}, lst)
	assert.Nil(t, err)
	db.RPush("myList", "a")
	db.RPush("myList", "b")
	db.RPush("myList", "c")
	lst, err = db.LRange("myList", 0, 10)
	assert.Equal(t, []string{"a", "b", "c"}, lst)
	assert.Nil(t, err)
	lst, err = db.LRange("myList", 0, 0)
	assert.Equal(t, []string{"a"}, lst)
	assert.Nil(t, err)
	lst, err = db.LRange("myList", 0, 1)
	assert.Equal(t, []string{"a", "b"}, lst)
	assert.Nil(t, err)
	lst, err = db.LRange("myList", 1, 2)
	assert.Equal(t, []string{"b", "c"}, lst)
	assert.Nil(t, err)
}

func TestPushPop(t *testing.T) {
	db := NewDBMock()
	_, err := db.LPop("myList")
	assert.Equal(t, turing.KeyNotExistsError, err)
	err = db.RPush("myList", "a")
	assert.Nil(t, err)
	err = db.RPush("myList", "b")
	assert.Nil(t, err)
	err = db.RPush("myList", "c")
	assert.Nil(t, err)
	lst, err := db.LRange("myList", 0, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"a","b","c"}, lst)
	elem, err := db.LPop("myList")
	assert.Nil(t, err)
	assert.Equal(t, "a", elem)
	elem, err = db.LPop("myList")
	assert.Nil(t, err)
	assert.Equal(t, "b", elem)
	elem, err = db.LPop("myList")
	assert.Nil(t, err)
	assert.Equal(t, "c", elem)
	_, err = db.LPop("myList")
	assert.Equal(t, turing.KeyNotExistsError, err)
}

func TestLTrim(t *testing.T) {
	db := NewDBMock()
	db.RPush("myList", "a")
	db.RPush("myList", "b")
	db.RPush("myList", "c")
	err := db.LTrim("myList", 0, 1)
	assert.Nil(t, err)
	lst, err := db.LRange("myList", 0, 2)
	assert.Nil(t, err)
	assert.Equal(t, []string{"a", "b"}, lst)
	db.RPush("myList", "c")
	err = db.LTrim("myList", 1, 1)
	assert.Nil(t, err)
	lst, _ = db.LRange("myList", 0, 2)
	assert.Equal(t, []string{"b"}, lst)
}

func TestPubSub(t *testing.T) {
	db := NewDBMock()
	c := make(chan int, 100)
	db.Subscribe("myTopic", func (topic string, value interface{}) {
		assert.Equal(t, "myTopic", topic)
		c <- value.(int)
	})
	db.Subscribe("myTopic", func (topic string, value interface{}) {
		assert.Equal(t, "myTopic", topic)
		c <- value.(int)
	})
	db.Publish("myTopic2", 1)
	assert.Equal(t, 0, len(c))
	db.Publish("myTopic", 2)
	n := <- c
	assert.Equal(t, 2, n)
	n = <- c
	assert.Equal(t, 2, n)
	assert.Equal(t, 0, len(c))
}