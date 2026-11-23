// Package migrations embarca as migrations do gateway_db como embed.FS para o
// binário cmd/migrations. Os .sql ficam aqui, e não em cmd/migrations/, para
// facilitar a leitura da numeração em code review.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
