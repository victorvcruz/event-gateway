package migrations

import (
	"errors"
	"fmt"
	"regexp"
)

type Options struct {
	TableName       string
	MigrationsTable string
	IndexPrefix     string
}

type Option func(*Options)

func WithTableName(name string) Option {
	return func(o *Options) { o.TableName = name }
}

func WithMigrationsTable(name string) Option {
	return func(o *Options) { o.MigrationsTable = name }
}

var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

var ErrInvalidIdentifier = errors.New("migrations: invalid identifier")

func resolveOptions(opts []Option) (Options, error) {
	o := Options{TableName: "outbox"}
	for _, apply := range opts {
		apply(&o)
	}
	if !identifierRe.MatchString(o.TableName) {
		return o, fmt.Errorf("%w: TableName %q", ErrInvalidIdentifier, o.TableName)
	}
	if o.MigrationsTable == "" {
		o.MigrationsTable = "schema_migrations_" + o.TableName
	}
	if !identifierRe.MatchString(o.MigrationsTable) {
		return o, fmt.Errorf("%w: MigrationsTable %q", ErrInvalidIdentifier, o.MigrationsTable)
	}
	if o.IndexPrefix == "" {
		o.IndexPrefix = "idx_" + o.TableName
	}
	if !identifierRe.MatchString(o.IndexPrefix) {
		return o, fmt.Errorf("%w: IndexPrefix %q", ErrInvalidIdentifier, o.IndexPrefix)
	}
	return o, nil
}
