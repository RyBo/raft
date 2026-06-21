// Package webui embeds the built React frontend so the demo ships as a single
// self-contained Go binary. Run `make build-ui` (vite build) to populate dist/.
package webui

import "embed"

//go:embed all:dist
var DistFS embed.FS
