package main

import (
	"os"

	domain "github.com/victorvcruz/event-gateway/payment-service/migrations"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func main() {
	dsn := os.Getenv("DATABASE_DSN")
	if err := outboxmig.Apply(dsn); err != nil {
		panic(err)
	}
	if err := domain.Apply(dsn); err != nil {
		panic(err)
	}
}
