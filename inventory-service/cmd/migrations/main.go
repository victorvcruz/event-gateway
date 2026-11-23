package main

import (
	"os"

	domain "github.com/victorvcruz/event-gateway/inventory-service/migrations"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func main() {
	d := os.Getenv("DATABASE_DSN")
	if e := outboxmig.Apply(d); e != nil {
		panic(e)
	}
	if e := domain.Apply(d); e != nil {
		panic(e)
	}
}
