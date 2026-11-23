package schema

import "embed"

//go:embed schemas/*.json
var embeddedFS embed.FS

const embeddedRoot = "schemas"
