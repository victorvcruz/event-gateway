// Package migrations embarca o DDL do outbox como embed.FS e expõe
// Apply(dsn). Cada serviço que usa shared/outbox chama Apply para criar a
// tabela no próprio banco.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
