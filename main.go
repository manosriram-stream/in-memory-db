package main

import (
	"bufio"
	"encoding/json"
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
)

type DB struct {
	kv             map[any]any //  TODO: name this better!
	lock           sync.Mutex
	walFilePointer *os.File
}

func (d *DB) Init() {
	file, err := os.Open("wal")
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
		case "1":
			// string
			break
		case "2":
			v, _ = strconv.ParseInt(keyAndValue[3], 10, 64)
			// int
			break
		case "3":
			v, _ = strconv.ParseFloat(keyAndValue[3], 64)
			// float
			break
		case "4":
			break
		}

		d.kv[keyAndValue[2]] = v
	}

	// Check for errors during scanning
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error during scanning: %v", err)
	}
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

		var data string

		data += "1"
		data += SPACE

		// type of the value
		switch r.Value.(type) {
		case string:
			data += "1"
			break
		case int, int32, int64:
			data += "2"
			break
		case float32, float64:
			data += "3"
			break
		default:
			data += "4"
			break
		}
		data += SPACE

		data += r.Key
		data += SPACE

		data += fmt.Sprintf("%v", r.Value)

		data += "\n"

		f.WriteString(data)
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
