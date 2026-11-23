package migrations

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// Apply roda no dsn todas as migrations embarcadas até a última versão. Pode
// ser chamada várias vezes: sem migration nova, não faz nada.
func Apply(dsn string) error {
	src, err := iofs.New(FS, ".")
	if err != nil {
		return fmt.Errorf("open embedded source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return fmt.Errorf("open migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("up: %w", err)
	}
	return nil
}
