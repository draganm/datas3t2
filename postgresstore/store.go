package postgresstore

import "embed"

//go:generate sqlc generate

//go:embed migrations/*.sql
var MigrationsFS embed.FS
