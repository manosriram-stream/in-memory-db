package main

import (
	"log"

	"github.com/gofiber/fiber/v3"
)

func main() {
	db := NewDB("wal")

	app := fiber.New()

	app.Get("/:key", db.GetKey)
	app.Post("/", db.PutKey)

	log.Fatal(app.Listen(":3000"))
}
