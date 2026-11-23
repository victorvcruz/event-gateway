package main

import (
	"fmt"
	"os"

	domain "github.com/victorvcruz/event-gateway/order-service/migrations"
	outboxmig "github.com/victorvcruz/event-gateway/shared/outbox/migrations"
)

func main() {
	dsn := os.Getenv("DATABASE_DSN")
	if err := outboxmig.Apply(dsn); err != nil {
		panic(fmt.Errorf("outbox: %w", err))
	}
	if err := domain.Apply(dsn); err != nil {
		panic(fmt.Errorf("orders: %w", err))
	}
}
