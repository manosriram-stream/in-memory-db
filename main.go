package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"

	"github.com/gofiber/fiber/v3"
)

type DB struct {
	kv             map[any]any //  TODO: name this better!
	lock           sync.Mutex
	walFilePointer *os.File
}

func (d *DB) Init() {
	dat, err := os.ReadFile("wal")

	if err != nil {
		log.Fatalf("Error reading WAL file %s\n", err.Error())
	}

	fmt.Println("size = ", dat)

	keySize := dat[1]
	key := string(dat[2 : 3+keySize])

	valSize := dat[3+keySize+1]

	typ := int(dat[len(dat)-1])

	var val any
	switch typ {
	case 0:
		val = string(dat[3+keySize+1 : 3+keySize+1+valSize])
		break
	case 1:
		val, _ = strconv.Atoi(string(dat[3+keySize+1 : 3+keySize+1+valSize]))
		break
	case 2:
		val, _ = strconv.ParseFloat(string(dat[3+keySize+1:3+keySize+1+valSize]), 64)
		break
	case 3:
		break
	}

	// switch v := val.(type) {

	// }
	fmt.Println(key)
	fmt.Println(val)
	// keySize := binary.BigEndian.Uint64()
	// fmt.Println("size = ", keySize)
	// keySize := int(dat[1:8])

	// fmt.Println("key = ",
}

type PutReq struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func main() {
	f, err := os.OpenFile("wal", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatalf("Error reading WAL %s\n", err.Error())
	}
	defer f.Close()

	db := &DB{
		kv:             make(map[any]any),
		lock:           sync.Mutex{},
		walFilePointer: f,
	}
	db.Init()

	app := fiber.New()

	app.Get("/:key", func(c fiber.Ctx) error {
		key := c.Params("key")

		db.lock.Lock()
		v, ok := db.kv[key]
		db.lock.Unlock()

		if !ok {
			return c.SendStatus(400)
		}
		return c.JSON(fiber.Map{"value": v})
	})

	app.Post("/", func(c fiber.Ctx) error {
		var r PutReq
		json.Unmarshal(c.Body(), &r)

		var data []byte

		data = append(data, byte(1))

		data = append(data, byte(len(r.Key)))
		data = append(data, r.Key...)

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(r.Value)
		if err != nil {
			log.Fatalf("Error encoding value to []byte %s\n", err.Error())
		}

		data = append(data, byte(len(buf.Bytes())))
		data = append(data, buf.Bytes()...)

		fmt.Printf("type = %T\n", r.Value)

		switch r.Value.(type) {
		case string:
			data = append(data, 0)
		case int, int32, int64:
			data = append(data, 1)
		case float32, float64:
			data = append(data, 2)
		default:
			data = append(data, 3)
		}
		// Write to WAL
		// 1 <KeySize><Key><ValueSize><Value>

		// 0 -> str
		// 1 -> int

		// How to create a file and write bytes to it
		// How to convert string to bytes

		_, err = f.Write(data)
		if err != nil {
			log.Fatalf("Error writing to WAL %s\n", err.Error())
		}

		db.lock.Lock()
		db.kv[r.Key] = r.Value
		db.lock.Unlock()

		return c.SendStatus(200)
	})

	// Start the server on port 3000
	log.Fatal(app.Listen(":3000"))
}
