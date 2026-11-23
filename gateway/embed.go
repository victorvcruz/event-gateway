// Package gateway embarca o config.local.yaml no binário. Existe só porque
// //go:embed exige que o arquivo esteja no diretório do .go.
package gateway

import _ "embed"

//go:embed config.local.yaml
var ConfigLocalYAML []byte
