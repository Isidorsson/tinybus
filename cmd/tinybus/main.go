// tinybus CLI entrypoint. Subcommands: migrate, enqueue, worker,
// producer, stats. Run `tinybus help` for usage.
package main

import (
	"fmt"
	"os"

	"github.com/Isidorsson/tinybus/internal/cli"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "migrate":
		cli.Migrate(args)
	case "enqueue":
		cli.Enqueue(args)
	case "worker":
		cli.Worker(args)
	case "producer":
		cli.Producer(args)
	case "stats":
		cli.Stats(args)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "tinybus: unknown subcommand %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: tinybus <command> [flags]

Commands:
  migrate <up|down>           Apply or roll back schema migrations.
  enqueue --queue=X --payload=Y [--run-in=DUR] [--max-attempts=N]
  worker  --queue=X [--concurrency=N] [--http-addr=:8080] [--lease=5m] [--poll=1s]
  producer --queue=X --interval=2s --payload=...
  stats                       Print per-queue counts.

Environment:
  DATABASE_URL                Required. e.g. postgres://user:pw@host:5432/db?sslmode=disable
  PORT                        Optional. If set and worker --http-addr is empty, the worker
                              listens on :$PORT for /healthz and /stats (Railway-friendly).`)
}
