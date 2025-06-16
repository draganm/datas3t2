package main

import (
	"github.com/draganm/datas3t2/command/server"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:    "Datas3t2",
		Usage:   "Datas3t2",
		Version: "0.0.1",
		Commands: []*cli.Command{
			server.Server(),
		},
	}
}
