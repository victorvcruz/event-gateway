package migrations

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"testing/fstest"
	"text/template"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const MigrationsTable = "schema_migrations_outbox"

func Apply(dsn string, opts ...Option) error {
	resolved, err := resolveOptions(opts)
	if err != nil {
		return err
	}

	rendered, err := renderTemplates(FS, resolved)
	if err != nil {
		return fmt.Errorf("render migrations: %w", err)
	}

	dsnWithTable, err := withMigrationsTable(dsn, resolved.MigrationsTable)
	if err != nil {
		return fmt.Errorf("annotate dsn: %w", err)
	}

	src, err := iofs.New(rendered, ".")
	if err != nil {
		return fmt.Errorf("open rendered source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsnWithTable)
	if err != nil {
		return fmt.Errorf("open migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("up: %w", err)
	}
	return nil
}

func Down(dsn string, opts ...Option) error {
	resolved, err := resolveOptions(opts)
	if err != nil {
		return err
	}

	rendered, err := renderTemplates(FS, resolved)
	if err != nil {
		return fmt.Errorf("render migrations: %w", err)
	}

	dsnWithTable, err := withMigrationsTable(dsn, resolved.MigrationsTable)
	if err != nil {
		return fmt.Errorf("annotate dsn: %w", err)
	}

	src, err := iofs.New(rendered, ".")
	if err != nil {
		return fmt.Errorf("open rendered source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsnWithTable)
	if err != nil {
		return fmt.Errorf("open migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("down: %w", err)
	}
	return nil
}

func renderTemplates(src fs.FS, opts Options) (fs.FS, error) {
	entries, err := fs.ReadDir(src, ".")
	if err != nil {
		return nil, err
	}
	out := fstest.MapFS{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		raw, err := fs.ReadFile(src, name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		tmpl, err := template.New(name).Option("missingkey=error").Parse(string(raw))
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, opts); err != nil {
			return nil, fmt.Errorf("execute %s: %w", name, err)
		}
		out[name] = &fstest.MapFile{Data: buf.Bytes()}
	}
	return out, nil
}

func withMigrationsTable(dsn, table string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("x-migrations-table", table)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
