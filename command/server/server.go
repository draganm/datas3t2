package server

import (
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/urfave/cli/v2"
)

func Server() *cli.Command {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		addr string
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
		},
		Action: func(c *cli.Context) error {

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

			return srv.Serve(l)
		},
	}
}
