package main

import (
	"log"

	"github.com/gofiber/fiber/v3"
)

func main() {
	db, err := NewDB("wal")
	if err != nil {
		log.Fatal(err.Error())
	}

	app := fiber.New()

	app.Get("/:key", db.GetKey)
	app.Post("/", db.PutKey)

	log.Fatal(app.Listen(":3000"))
}
