package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_InitDb(t *testing.T) {
	db, err := NewDB("wal_test")
	defer db.Close()

	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, db)
}

func Test_PutKey(t *testing.T) {
	db, err := NewDB("wal_test")
	defer db.Close()

	assert.Equal(t, nil, err)

	err = db.Put("key", "value")
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, db.kvSize)
}

func Test_GetKey(t *testing.T) {
	db, err := NewDB("wal_test")
	defer db.Close()

	assert.Equal(t, nil, err)

	err = db.Put("key", "value")
	assert.Equal(t, nil, err)

	v, err := db.Get("key")
	assert.Equal(t, nil, err)
	assert.Equal(t, "value", v)
}

func Test_DbInitPersistenceAfterRestart(t *testing.T) {
	db, err := NewDB("wal_test")
	defer db.Close()

	assert.Equal(t, nil, err)

	err = db.Put("key", "value")
	err = db.Put("key2", "value2")
	err = db.Put("key3", "value3")
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, db.kvSize)

	db.Delete("key")
	db.Delete("key2")
	db.Delete("key3")

	assert.Equal(t, 0, db.kvSize)

	err = db.Init()

	assert.Equal(t, nil, err)
	assert.Equal(t, 3, db.kvSize)
}
