package main

import (
	"os"

	"github.com/victorvcruz/event-gateway/notification-service/migrations"
)

func main() {
	if e := migrations.Apply(os.Getenv("DATABASE_DSN")); e != nil {
		panic(e)
	}
}
