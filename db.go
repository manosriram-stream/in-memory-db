package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gofiber/fiber/v3"
)

var (
	SPACE           string = " "
	WAL_LINE_LENGTH int    = 4

	TYPE_STRING  = "1"
	TYPE_INT     = "2"
	TYPE_FLOAT   = "3"
	TYPE_UNKNOWN = "4"
)

type DB struct {
	kv             map[any]any //  TODO: name this better!
	kvSize         int
	lock           sync.RWMutex
	walFilePointer *os.File
}

func NewDB(walFilePath string) *DB {
	f, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalf("Error reading WAL %s\n", err.Error())
	}

	db := &DB{
		kv:             make(map[any]any),
		lock:           sync.RWMutex{},
		walFilePointer: f,
		kvSize:         0,
	}
	db.Init()

	return db
}

type PutReq struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func (d *DB) Init() {
	file, err := os.Open(d.walFilePointer.Name())
	if err != nil {
		log.Fatalf("Error reading WAL")
	}
	defer file.Close() // Ensure the file is closed

	// Create a new scanner
	scanner := bufio.NewScanner(file)

	// Iterate through the file line by line
	for scanner.Scan() {
		line := scanner.Text()

		keyAndValue := strings.Split(line, " ")
		if len(keyAndValue) != WAL_LINE_LENGTH {
			log.Fatalf("Error reading WAL")
		}

		var v any = keyAndValue[1]

		switch keyAndValue[1] {
		case TYPE_STRING:
			break
		case TYPE_INT:
			v, _ = strconv.ParseInt(keyAndValue[3], 10, 64)
			break
		case TYPE_FLOAT:
			v, _ = strconv.ParseFloat(keyAndValue[3], 64)
			break
		case TYPE_UNKNOWN:
			break
		}

		// d.kv[keyAndValue[2]] = v
		d.PutWithoutWAL(keyAndValue[2], v)
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error during scanning: %v", err)
	}
}

func (d *DB) Close() error {
	d.walFilePointer.Close()
	// TODO: clean up the d.walFilePointer file

	err := os.Remove(d.walFilePointer.Name())
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Delete(key any) {
	db.lock.Lock()
	if db.kv[key] == nil {
		return
	}
	delete(db.kv, key)
	db.kvSize -= 1
	db.lock.Unlock()
}

func (db *DB) Get(key any) (any, error) {
	db.lock.RLock()
	v, ok := db.kv[key]
	db.lock.RUnlock()

	if !ok {
		return nil, errors.New("Error getting key")
	}

	return v, nil
}

func (db *DB) GetKey(c fiber.Ctx) error {
	key := c.Params("key")

	v, err := db.Get(key)
	if err != nil {
		return c.SendStatus(400)
	}
	return c.JSON(fiber.Map{"value": v})
}
func (db *DB) PutWithoutWAL(key, value any) error {
	db.lock.Lock()

	if db.kv[key] == nil {
		db.kvSize += 1
	}

	db.kv[key] = value
	db.lock.Unlock()
	return nil
}

func (db *DB) Put(key, value any) error {
	var data string

	data += "1" // TODO: update this to use var
	data += SPACE

	// type of the value
	switch value.(type) {
	case string:
		data += TYPE_STRING
		break
	case int, int32, int64:
		data += TYPE_INT
		break
	case float32, float64:
		data += TYPE_FLOAT
		break
	default:
		data += TYPE_UNKNOWN
		break
	}
	data += SPACE

	data += key.(string)
	data += SPACE

	data += fmt.Sprintf("%v", value)

	data += "\n"

	_, err := db.walFilePointer.WriteString(data)
	if err != nil {
		// log.Fatalf("Error writing to WAL %s\n", err.Error())
		return errors.New(fmt.Sprintf("Error writing to WAL %s\n", err.Error()))
	}

	db.lock.Lock()

	if db.kv[key] == nil {
		db.kvSize += 1
	}

	db.kv[key] = value
	db.lock.Unlock()

	return nil
}

func (db *DB) PutKey(c fiber.Ctx) error {
	var r PutReq
	json.Unmarshal(c.Body(), &r)

	err := db.Put(r.Key, r.Value)
	if err != nil {
		return err
	}

	return c.SendStatus(200)
}
