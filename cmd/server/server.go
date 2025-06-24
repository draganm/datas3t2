package main

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/draganm/datas3t2/httpapi"
	"github.com/draganm/datas3t2/postgresstore"
	"github.com/draganm/datas3t2/server"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v2"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		addr         string
		dbURL        string
		cacheDir     string
		maxCacheSize int64
	}{}

	app := &cli.App{
		Name:  "datas3t2-server",
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
			&cli.StringFlag{
				Name:        "cache-dir",
				Usage:       "Cache directory",
				Required:    true,
				EnvVars:     []string{"CACHE_DIR"},
				Destination: &cfg.cacheDir,
			},
			&cli.Int64Flag{
				Name:        "max-cache-size",
				Value:       1024 * 1024 * 1024,
				Usage:       "Maximum cache size in bytes",
				EnvVars:     []string{"MAX_CACHE_SIZE"},
				Destination: &cfg.maxCacheSize,
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

			db, err := pgxpool.New(ctx, cfg.dbURL)
			if err != nil {
				return fmt.Errorf("failed to connect to database: %w", err)
			}
			defer db.Close()

			m, err := migrate.NewWithSourceInstance("iofs", d, strings.Replace(cfg.dbURL, "postgresql:", "pgx5:", 1))
			if err != nil {
				return fmt.Errorf("failed to create migrator: %w", err)
			}

			err = m.Up()
			switch err {
			case nil:
				logger.Info("Applied database migrations")
			case migrate.ErrNoChange:
				logger.Info("No migrations applied")
			default:
				return fmt.Errorf("failed to run migrations: %w", err)
			}

			l, err := net.Listen("tcp", cfg.addr)
			if err != nil {
				logger.Error("failed to listen", "error", err)
				return err
			}
			defer l.Close()
			logger.Info("server started", "addr", l.Addr())

			s, err := server.NewServer(db, cfg.cacheDir, cfg.maxCacheSize)
			if err != nil {
				return fmt.Errorf("failed to create server: %w", err)
			}

			mux := httpapi.NewHTTPAPI(s, logger)

			srv := &http.Server{
				Handler: mux,
			}

			context.AfterFunc(ctx, func() {
				srv.Close()
			})

			return srv.Serve(l)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
