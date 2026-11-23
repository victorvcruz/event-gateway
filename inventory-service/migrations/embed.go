package migrations

import (
	"embed"

	"github.com/victorvcruz/event-gateway/shared/migratex"
)

//go:embed *.sql
var FS embed.FS

func Apply(d string) error { return migratex.Apply(d, FS, "schema_migrations_inventory") }
