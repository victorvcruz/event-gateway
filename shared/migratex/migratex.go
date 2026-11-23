// Package migratex aplica um conjunto de migrations versionadas e embarcadas.
package migratex

import (
	"errors"
	"fmt"
	"io/fs"
	"net/url"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

func Apply(dsn string, migrations fs.FS, table string) error {
	source, err := iofs.New(migrations, ".")
	if err != nil {
		return fmt.Errorf("open migrations: %w", err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	q := u.Query()
	q.Set("x-migrations-table", table)
	u.RawQuery = q.Encode()
	m, err := migrate.NewWithSourceInstance("iofs", source, u.String())
	if err != nil {
		return fmt.Errorf("open migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("up: %w", err)
	}
	return nil
}
