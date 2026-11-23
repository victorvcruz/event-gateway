package migrations

import (
	"embed"

	"github.com/victorvcruz/event-gateway/shared/migratex"
)

//go:embed *.sql
var FS embed.FS

func Apply(dsn string) error { return migratex.Apply(dsn, FS, "schema_migrations_fulfillment") }
