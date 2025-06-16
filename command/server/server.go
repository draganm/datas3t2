package server

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/draganm/datas3t2/postgresstore"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5"
	"github.com/urfave/cli/v2"
)

func Server() *cli.Command {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		addr  string
		dbURL string
	}{}

	return &cli.Command{
		Name:  "server",
		Usage: "Start the server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "addr",
				Value:       ":8080",
				Usage:       "Address to bind to",
				EnvVars:     []string{"ADDR"},
				Destination: &cfg.addr,
			},
			&cli.StringFlag{
				Name:        "db-url",
				Value:       "postgres://postgres:postgres@localhost:5432/postgres",
				Usage:       "Database URL",
				EnvVars:     []string{"DB_URL"},
				Destination: &cfg.dbURL,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {

			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt, os.Kill)
			defer cancel()

			migrationFS, err := fs.Sub(postgresstore.MigrationsFS, "migrations")
			if err != nil {
				return err
			}

			d, err := iofs.New(migrationFS, ".")
			if err != nil {
				return err
			}

			// conn, err := pgx.Connect(ctx, cfg.dbURL)
			// if err != nil {
			// 	return fmt.Errorf("failed to connect to database: %w", err)
			// }

			m, err := migrate.NewWithSourceInstance("iofs", d, strings.Replace(cfg.dbURL, "postgresql:", "pgx5:", 1))
			if err != nil {
				return fmt.Errorf("failed to create migrator: %w", err)
			}

			err = m.Up()
			switch err {
			case nil:
				log.Info("Applied database migrations")
			case migrate.ErrNoChange:
				log.Info("No migrations applied")
			default:
				return fmt.Errorf("failed to run migrations: %w", err)
			}

			l, err := net.Listen("tcp", cfg.addr)
			if err != nil {
				log.Error("failed to listen", "error", err)
				return err
			}
			defer l.Close()
			log.Info("server started", "addr", l.Addr())

			mux := http.NewServeMux()

			srv := &http.Server{
				Handler: mux,
			}

			context.AfterFunc(ctx, func() {
				srv.Close()
			})

			return srv.Serve(l)
		},
	}
}
